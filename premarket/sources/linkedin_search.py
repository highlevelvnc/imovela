"""
LinkedIn career-change signal source — via DuckDuckGo HTML search.

Why DuckDuckGo (not direct LinkedIn scraping)
---------------------------------------------
Direct LinkedIn scraping violates their ToS and is technically very difficult
(login walls, rate-limits, legal risk).  DuckDuckGo indexes public LinkedIn
profiles and returns rich snippets that often contain enough information to
detect a relocation or career change.  This approach is:
  - Read-only, public data only
  - No LinkedIn account required
  - No ToS violation (searching is public)
  - Graceful — if DDG blocks or changes, returns []

What we detect
--------------
Two signal types:
  1. linkedin_city_change (score=60):
     Snippets that mention the person has moved FROM a target zone TO another
     city/country.  Keywords: "mudei para", "relocating to", "moved to",
     "saiu de Lisboa", "agora em [city]".

  2. linkedin_job_change (score=40):
     Snippets that mention a significant professional change that could imply
     relocation.  Keywords: "nova oportunidade", "new chapter", "new role",
     "novo emprego", "excited to join".

Query strategy
--------------
Three query templates per zone (no site: operator — DDG blocks it on html endpoint):
  1. "{zone}" linkedin "mudei para" OR "deixei {zone}" OR "a viver em"
  2. "{zone}" linkedin "moving to" OR "relocated" OR "now based in"
  3. "{zone}" linkedin "nova oportunidade" OR "new chapter" OR "novo emprego"

Results are parsed from DDG HTML (not their JSON API which is limited to
instant-answer boxes, not web results). Filtered client-side for linkedin.com/in/.

Rate limiting
-------------
One request per 5 seconds.  Capped at 10 results per zone query.
If DDG returns a CAPTCHA or empty results, the zone is skipped with WARNING.

CAPTCHA / blocking
------------------
DuckDuckGo detects automated requests and serves a visual CAPTCHA challenge
("Select all squares containing a duck") on server or CI IPs.
When blocked, this source returns 0 signals and logs a WARNING.
Block duration is typically 1–24 hours.  No programmatic workaround exists
without a full browser session (Playwright).  Consider this source unreliable
in automated/server environments.

Data quality note
-----------------
This is the weakest signal source. Many snippets are false positives.
The signal score (40-60) reflects this uncertainty.  Use as a supplementary
signal, not as a primary lead source.
"""
from __future__ import annotations

import re
import time
import urllib.parse
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from premarket.signals import PremktSignalData
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# DuckDuckGo endpoints — lite is simpler HTML, html is standard
_DDG_HTML_URL   = "https://html.duckduckgo.com/html/"
_DDG_LITE_URL   = "https://lite.duckduckgo.com/lite/"
_REQUEST_DELAY  = 5.0   # seconds — polite DDG crawling (increased from 4s)
_REQUEST_TIMEOUT= 15
_MAX_RESULTS    = 10    # results consumed per zone query

# Regex patterns for career/city change signals
_CITY_CHANGE_RE = re.compile(
    r"mudei[ -]para|mudei-me[ -]para|moving[ -]to|moved[ -]to|"
    r"relocated[ -]to|relocation|relocating|"
    r"agora[ -]em\b|now[ -]based[ -]in|saiu[ -]de\b|"
    r"deixei[ -](lisboa|cascais|sintra|almada|seixal|sesimbra)|"
    r"vou[ -]para\b|going[ -]to[ -]live|a[ -]viver[ -]em",
    re.IGNORECASE,
)

_JOB_CHANGE_RE = re.compile(
    r"nova[ -]oportunidade|new[ -]chapter|new[ -]role|new[ -]position|"
    r"novo[ -]emprego|excited[ -]to[ -]join|thrilled[ -]to[ -]join|"
    r"happy[ -]to[ -]announce|orgulhoso[ -]de[ -]anunciar|"
    r"comecei[ -](a[ -])?trabalhar|starting[ -]at|joined\b|"
    r"nova[ -]fase|novo[ -]cap[ií]tulo|proud[ -]to[ -]announce",
    re.IGNORECASE,
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://duckduckgo.com/",
}

# Two-query strategy per zone:
#   Query A — relocation focus (stronger signal, higher score)
#   Query B — job change / new role (weaker signal)
# NOTE: 'site:' operator dropped — DDG's html endpoint does not reliably
#       honour it and returns HTTP 202 with zero results when used.
#       Results are filtered client-side for linkedin.com/in/ URLs.
_QUERY_TEMPLATES: list[str] = [
    '"{zone}" linkedin "mudei para" OR "deixei {zone}" OR "a viver em"',
    '"{zone}" linkedin "moving to" OR "relocated" OR "now based in"',
    '"{zone}" linkedin "nova oportunidade" OR "new chapter" OR "novo emprego"',
]

# Module-level CAPTCHA/block flag — set True once detected so all further
# queries in the same run are skipped immediately (avoids 600s wasted on
# blocked requests).  Covers two failure modes:
#   1. DDG returns CAPTCHA challenge page (HTTP 202 + body keyword)
#   2. Both endpoints time out with no HTTP response (network-level block)
_DDG_CAPTCHA_BLOCKED: bool = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_signal_type(snippet: str) -> Optional[str]:
    """
    Return the signal type string based on snippet content, or None if no match.
    City-change signals take precedence over generic job-change signals.
    """
    if _CITY_CHANGE_RE.search(snippet):
        return "linkedin_city_change"
    if _JOB_CHANGE_RE.search(snippet):
        return "linkedin_job_change"
    return None


def _extract_name_from_title(title: str) -> Optional[str]:
    """
    LinkedIn page titles have the form "Name - Role @ Company | LinkedIn".
    Extract the name part (before the first " - ").
    """
    if " - " in title:
        name = title.split(" - ")[0].strip()
        if name and len(name) > 2:
            return name
    return None


def _extract_role_company(title: str) -> tuple[Optional[str], Optional[str]]:
    """
    Attempt to extract role and company from a LinkedIn page title.
    Format: "Name - Role at Company | LinkedIn"
    """
    role    = None
    company = None
    try:
        # Remove "| LinkedIn" suffix
        clean = re.sub(r"\|?\s*linkedin\s*$", "", title, flags=re.IGNORECASE).strip()
        if " - " in clean:
            after_name = clean.split(" - ", 1)[1].strip()
            # "Role at Company" or "Role @ Company"
            if " at " in after_name.lower():
                parts   = re.split(r"\s+at\s+", after_name, maxsplit=1, flags=re.IGNORECASE)
                role    = parts[0].strip()
                company = parts[1].strip()
            elif " @ " in after_name:
                parts   = after_name.split(" @ ", 1)
                role    = parts[0].strip()
                company = parts[1].strip()
            else:
                role = after_name
    except Exception:
        pass
    return role or None, company or None


def _ddg_get(client: httpx.Client, query: str) -> list[dict]:
    """
    Single DDG search via GET on html/lite duckduckgo endpoints.

    Tries html endpoint first; falls back to lite endpoint.
    Returns a list of {title, snippet, url} dicts for any result that
    contains 'linkedin.com/in/' in its URL or snippet.

    Sets _DDG_CAPTCHA_BLOCKED on two failure modes:
      1. DDG returns a CAPTCHA challenge page (HTTP 202 + body keyword).
      2. Both endpoints time out with no HTTP response (network-level block).
    """
    global _DDG_CAPTCHA_BLOCKED  # single declaration at function top

    time.sleep(_REQUEST_DELAY)

    _got_any_response = False  # tracks whether at least one endpoint responded

    for endpoint in (_DDG_HTML_URL, _DDG_LITE_URL):
        try:
            # GET request avoids some POST-blocking heuristics
            url_req = f"{endpoint}?q={urllib.parse.quote_plus(query)}&kl=pt-pt"
            resp = client.get(url_req, timeout=_REQUEST_TIMEOUT)
            _got_any_response = True

            if resp.status_code not in (200, 202):
                log.debug(
                    "[linkedin_search] DDG {ep} → {c} for query={q!r}",
                    ep=endpoint, c=resp.status_code, q=query[:60],
                )
                continue

            if len(resp.text) < 1000:
                log.debug("[linkedin_search] DDG response too short ({n} chars) — skipping",
                          n=len(resp.text))
                continue

            # Detect CAPTCHA challenge — DDG returns HTTP 202 + CAPTCHA page
            # when it identifies the client as a bot (common on server/CI IPs).
            # There is no programmatic workaround without a real browser session.
            if "bots use duckduckgo" in resp.text.lower() or "select all squares" in resp.text.lower():
                if not _DDG_CAPTCHA_BLOCKED:
                    _DDG_CAPTCHA_BLOCKED = True
                    log.warning(
                        "[linkedin_search] DDG CAPTCHA detected — "
                        "this IP is rate-limited/blocked by DuckDuckGo. "
                        "LinkedIn signals unavailable until block clears (usually 1–24h). "
                        "EXPERIMENTAL SOURCE — expect 0 signals when blocked. "
                        "Skipping all remaining LinkedIn queries this run.",
                    )
                return []

            soup    = BeautifulSoup(resp.text, "lxml")
            results = []

            # Parse both html and lite HTML structures
            for result in soup.select(
                "div.result, div.web-result, tr.result-link, .result"
            )[:_MAX_RESULTS]:
                try:
                    link_el = (
                        result.select_one("a.result__a") or
                        result.select_one("h2 a") or
                        result.select_one("a[href*='linkedin']")
                    )
                    if not link_el:
                        continue
                    title = link_el.get_text(strip=True)
                    href  = link_el.get("href", "")

                    # DDG wraps redirect URLs — unwrap to real URL
                    if "uddg=" in href:
                        href = urllib.parse.parse_qs(
                            urllib.parse.urlparse(href).query
                        ).get("uddg", [href])[0]

                    snip_el = (
                        result.select_one("a.result__snippet") or
                        result.select_one(".result__snippet") or
                        result.select_one(".result-snippet")
                    )
                    snippet = snip_el.get_text(strip=True) if snip_el else ""

                    # Accept results that reference linkedin.com/in/
                    combined = f"{href} {title} {snippet}".lower()
                    if "linkedin.com/in/" in combined:
                        results.append({"title": title, "snippet": snippet, "url": href})
                except Exception:
                    pass

            if results:
                log.debug(
                    "[linkedin_search] DDG {ep} → {n} LinkedIn results for query={q!r}",
                    ep=endpoint, n=len(results), q=query[:60],
                )
                return results

        except Exception as e:
            log.debug("[linkedin_search] DDG {ep} error: {e}", ep=endpoint, e=e)

    # Both endpoints timed out or failed without any HTTP response — network-level block.
    # Set the same flag to prevent all remaining zone queries (saves up to ~600s per run).
    if not _got_any_response and not _DDG_CAPTCHA_BLOCKED:
        _DDG_CAPTCHA_BLOCKED = True
        log.warning(
            "[linkedin_search] DDG unreachable — both endpoints timed out. "
            "IP is likely network-blocked by DuckDuckGo (no HTTP response). "
            "LinkedIn signals unavailable. Skipping all remaining queries this run.",
        )

    return []


def _safe_ddg_search(client: httpx.Client, zone: str) -> list[dict]:
    """
    Run all query templates for a zone and merge de-duplicated results.

    Stops immediately if DDG CAPTCHA has already been detected in this run
    (_DDG_CAPTCHA_BLOCKED flag), avoiding unnecessary blocked requests.
    """
    global _DDG_CAPTCHA_BLOCKED
    if _DDG_CAPTCHA_BLOCKED:
        return []

    seen_urls: set[str] = set()
    all_results: list[dict] = []

    for tmpl in _QUERY_TEMPLATES:
        if _DDG_CAPTCHA_BLOCKED:
            break
        query = tmpl.format(zone=zone)
        try:
            batch = _ddg_get(client, query)
            for r in batch:
                if r["url"] not in seen_urls:
                    seen_urls.add(r["url"])
                    all_results.append(r)
        except Exception as e:
            log.debug("[linkedin_search] Query failed zone={z}: {e}", z=zone, e=e)
        # Polite inter-query pause already included in _ddg_get via _REQUEST_DELAY

    log.debug(
        "[linkedin_search] zone={z} → {n} unique LinkedIn results across {q} queries",
        z=zone, n=len(all_results), q=len(_QUERY_TEMPLATES),
    )
    return all_results


# ── Public interface ──────────────────────────────────────────────────────────

class LinkedInSearchSource:
    """
    Detect career/relocation change signals from public LinkedIn profiles
    via DuckDuckGo search snippets.

    Signal quality note
    -------------------
    This source generates lower-confidence signals (score 40-60).
    A DDG snippet showing "mudei para Londres" next to a Lisboa-based profile
    is a genuine relocation signal, but requires human verification before
    attempting contact.

    Usage:
        source  = LinkedInSearchSource()
        signals = source.fetch(zones=["Lisboa", "Cascais"])
    """

    def fetch(self, zones: list[str] | None = None) -> list[PremktSignalData]:
        from config.settings import settings
        target_zones = zones or settings.zones

        signals: list[PremktSignalData] = []

        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            for zone in target_zones:
                try:
                    results = _safe_ddg_search(client, zone)
                    for r in results:
                        combined     = f"{r['title']} {r['snippet']}"
                        signal_type  = _detect_signal_type(combined)
                        if not signal_type:
                            continue

                        name             = _extract_name_from_title(r["title"])
                        role, company    = _extract_role_company(r["title"])

                        signals.append(PremktSignalData(
                            signal_type  = signal_type,
                            source       = "duckduckgo_linkedin",
                            signal_text  = r["snippet"] or r["title"],
                            location_raw = zone,
                            zone         = zone,
                            name         = name,
                            company      = company,
                            role         = role,
                            url          = r["url"] or None,
                            extra        = {"title": r["title"]},
                        ))

                except Exception as e:
                    log.warning(
                        "[linkedin_search] Zone {z} failed: {e}", z=zone, e=e
                    )

        log.info(
            "[linkedin_search] {n} career/relocation signals detected",
            n=len(signals),
        )
        return signals

"""
Renovation-demand signal source — OLX Portugal + CustoJusto.

What we look for
----------------
Ads posted by *homeowners* seeking renovation contractors or architects.
These are NOT contractor advertisements — they are requests FROM owners
("Procuro empreiteiro para remodelar T2 em Lisboa") and represent strong
pre-market signals: the owner is investing in the property, which often
precedes a sale within 3-12 months.

Strategy
--------
1. Search OLX "Serviços > Remodelar" category using several intent queries.
2. Search CustoJusto services section with the same queries.
3. Parse each results page for listing title, description snippet, location.
4. Filter to listings that look like owner-demand (not contractor-supply) by
   checking for "procuro / preciso / quero / precisamos" keywords in the title.
5. Zone-match the location string against the six target zones.
6. Yield PremktSignalData for each qualifying listing.

Rate-limiting
-------------
No more than 2 requests per second (sleep between requests).
Each source is capped at MAX_ADS_PER_QUERY results per query.
If a request fails, it is skipped silently — never raises to the caller.
"""
from __future__ import annotations

import re
import time
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from premarket.signals import PremktSignalData, LEAD_TYPE
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_ADS_PER_QUERY = 15        # results consumed per search query per source
REQUEST_DELAY     = 1.8       # seconds between HTTP requests (polite crawling)
REQUEST_TIMEOUT   = 12        # httpx timeout in seconds

# Search queries that signal an owner seeking renovation services
# (homeowner demand, not contractor advertising)
_INTENT_QUERIES: list[str] = [
    "procuro empreiteiro remodelacao",
    "preciso empreiteiro obras",
    "procuro arquiteto remodelacao",
    "quero remodelar apartamento",
    "obras remodelacao T2 T3",
    "procuro pedreiro pinturas electricista",
]

# OLX base URL — searches use the global ?q= parameter.
# The /servicos/remodelar/ category slug no longer exists on OLX PT (404).
_OLX_BASE  = "https://www.olx.pt"
_OLX_SEARCH = "/imoveis/"    # search within real-estate section via ?q=

_CJ_BASE   = "https://www.custojusto.pt"
_CJ_SEARCH = "/todo-o-pais"

# Titles / descriptions that contain these words are likely OWNER requests
# or renovation-related listings that signal upcoming property activity.
# Broadened from strict "procuro/preciso" to include renovation topic keywords
# since OLX homeowners often post renovation projects without demand language.
_OWNER_REQUEST_RE = re.compile(
    r"\bprocuro\b|\bpreciso\b|\bquero\b|\bnecessito\b|\bprocuramos\b|\bprecisamos\b"
    r"|\bremodelação\b|\bremodelacao\b|\bremodelar\b"
    r"|\bobras\b|\brestaurо\b|\brestauro\b|\breabilitação\b",
    re.IGNORECASE,
)

# Zone keywords for location matching (substring match, lowercase)
_ZONE_KEYWORDS: dict[str, list[str]] = {
    "Lisboa":   ["lisboa", "parque das nacoes", "lumiar", "belem", "alvalade",
                 "areeiro", "benfica", "mouraria", "intendente", "mouraria"],
    "Cascais":  ["cascais", "estoril", "birre", "alcabideche", "sao domingos de rana"],
    "Sintra":   ["sintra", "queluz", "rio de mouro", "agualva", "cacém", "cacem",
                 "mem martins", "monte abraao", "porcalhota"],
    "Almada":   ["almada", "cacilhas", "costa da caparica", "pragal", "charneca",
                 "feijo", "laranjeiro", "monte de caparica"],
    "Seixal":   ["seixal", "amora", "corroios", "fernao ferro", "aldeia de paio pires"],
    "Sesimbra": ["sesimbra", "castelo", "santana"],
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _match_zone(text: str) -> Optional[str]:
    """Return the first zone whose keywords appear in the lowercased text."""
    lower = text.lower()
    for zone, keywords in _ZONE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return zone
    return None


def _is_owner_request(title: str, description: str = "") -> bool:
    """Return True if title/description looks like a homeowner seeking a contractor."""
    return bool(_OWNER_REQUEST_RE.search(f"{title} {description}"))


def _safe_get(client: httpx.Client, url: str) -> Optional[httpx.Response]:
    try:
        time.sleep(REQUEST_DELAY)
        resp = client.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp
        log.debug("[renovation_ads] HTTP {c} for {u}", c=resp.status_code, u=url)
    except Exception as e:
        log.debug("[renovation_ads] Request failed for {u}: {e}", u=url, e=e)
    return None


# ── OLX source ────────────────────────────────────────────────────────────────

def _fetch_olx(client: httpx.Client, query: str) -> list[PremktSignalData]:
    """
    Search OLX Imóveis section for renovation demand ads.
    URL pattern: https://www.olx.pt/imoveis/?q={encoded_query}

    Note: The old /servicos/remodelar/ category URL returned 404 as of 2026-03.
    The imoveis section with keyword search returns relevant results.
    """
    import urllib.parse
    url   = f"{_OLX_BASE}{_OLX_SEARCH}?q={urllib.parse.quote_plus(query)}"
    resp  = _safe_get(client, url)
    if not resp:
        return []

    soup  = BeautifulSoup(resp.text, "lxml")
    items = soup.select("div[data-cy='l-card']") or soup.select("li.css-1sw3lwy")
    log.debug("[renovation_ads:olx] query={q!r} → {n} raw cards", q=query, n=len(items))

    signals: list[PremktSignalData] = []

    for card in items[:MAX_ADS_PER_QUERY]:
        try:
            # Title
            title_el = (
                card.select_one("h6") or
                card.select_one("h3") or
                card.select_one("[data-cy='ad-card-title']")
            )
            if not title_el:
                continue
            title = title_el.get_text(strip=True)

            # Description snippet (often absent on listing page)
            desc_el = card.select_one("p.css-1tphyv3") or card.select_one("p")
            desc    = desc_el.get_text(strip=True) if desc_el else ""

            # Apply owner-request filter
            if not _is_owner_request(title, desc):
                continue

            # URL
            link_el = card.select_one("a[href]")
            href    = link_el["href"] if link_el else ""
            full_url = href if href.startswith("http") else f"{_OLX_BASE}{href}"

            # Location
            loc_el   = card.select_one("[data-testid='location-date']") or card.select_one("p.css-1a4brun")
            loc_text = loc_el.get_text(strip=True) if loc_el else ""
            zone     = _match_zone(f"{title} {desc} {loc_text}")

            signals.append(PremktSignalData(
                signal_type  = "renovation_ad_homeowner",
                source       = "olx",
                signal_text  = title,
                location_raw = loc_text or None,
                zone         = zone,
                url          = full_url or None,
                extra        = {"query": query, "description": desc},
            ))

        except Exception as e:
            log.debug("[renovation_ads:olx] card parse error: {e}", e=e)

    log.debug(
        "[renovation_ads:olx] query={q!r} → {n} qualifying signals",
        q=query, n=len(signals),
    )
    return signals


# ── CustoJusto source ──────────────────────────────────────────────────────────

def _fetch_custojusto(client: httpx.Client, query: str) -> list[PremktSignalData]:
    """
    Search CustoJusto services section for a given query string.
    URL: https://www.custojusto.pt/todo-o-pais?q={encoded_query}
    """
    import urllib.parse
    url  = f"{_CJ_BASE}{_CJ_SEARCH}?q={urllib.parse.quote_plus(query)}"
    resp = _safe_get(client, url)
    if not resp:
        return []

    soup  = BeautifulSoup(resp.text, "lxml")
    # CustoJusto listing cards use article elements
    items = soup.select("article.item") or soup.select("li.item")
    log.debug("[renovation_ads:cj] query={q!r} → {n} raw cards", q=query, n=len(items))

    signals: list[PremktSignalData] = []

    for card in items[:MAX_ADS_PER_QUERY]:
        try:
            title_el = card.select_one("h2 a") or card.select_one("a.title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)

            desc_el = card.select_one("p.description") or card.select_one("div.description")
            desc    = desc_el.get_text(strip=True) if desc_el else ""

            if not _is_owner_request(title, desc):
                continue

            href     = title_el.get("href", "")
            full_url = href if href.startswith("http") else f"{_CJ_BASE}{href}"

            loc_el   = card.select_one(".location") or card.select_one("span.geo")
            loc_text = loc_el.get_text(strip=True) if loc_el else ""
            zone     = _match_zone(f"{title} {desc} {loc_text}")

            signals.append(PremktSignalData(
                signal_type  = "renovation_ad_homeowner",
                source       = "custojusto",
                signal_text  = title,
                location_raw = loc_text or None,
                zone         = zone,
                url          = full_url or None,
                extra        = {"query": query, "description": desc},
            ))

        except Exception as e:
            log.debug("[renovation_ads:cj] card parse error: {e}", e=e)

    log.debug(
        "[renovation_ads:cj] query={q!r} → {n} qualifying signals",
        q=query, n=len(signals),
    )
    return signals


# ── Public interface ──────────────────────────────────────────────────────────

class RenovationAdsSource:
    """
    Search OLX and CustoJusto for homeowners seeking renovation contractors.

    Usage:
        source  = RenovationAdsSource()
        signals = source.fetch(zones=["Lisboa", "Cascais"])
    """

    def fetch(self, zones: list[str] | None = None) -> list[PremktSignalData]:
        """
        Fetch renovation-demand signals across all configured intent queries.

        zones parameter is used only for post-fetch filtering when a signal
        has a detectable zone; zone-less signals are always included.
        """
        all_signals: list[PremktSignalData] = []

        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            for query in _INTENT_QUERIES:
                try:
                    all_signals.extend(_fetch_olx(client, query))
                except Exception as e:
                    log.warning("[renovation_ads] OLX query {q!r} failed: {e}", q=query, e=e)
                try:
                    all_signals.extend(_fetch_custojusto(client, query))
                except Exception as e:
                    log.warning("[renovation_ads] CustoJusto query {q!r} failed: {e}", q=query, e=e)

        # Filter by zone when zones list is provided (keep zone=None signals too)
        if zones:
            all_signals = [
                s for s in all_signals
                if s.zone is None or s.zone in zones
            ]

        log.info(
            "[renovation_ads] Total qualifying renovation-demand signals: {n}",
            n=len(all_signals),
        )
        return all_signals

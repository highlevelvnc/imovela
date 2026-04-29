"""
Website Enricher — pipeline/website_enricher.py

For agency leads that have an agency_name but no phone/email, attempts to
find and visit the agency's public homepage and extract visible contact
information (tel: links, mailto: links, phone pattern in page text).

Strategy
--------
1. Skip major franchise chains (RE/MAX, KW, Century21, ERA, Zome, etc.)
   Their homepages carry brand content, not per-agent phones.

2. For independent/smaller agencies, construct a candidate homepage URL
   from the normalised agency name slug and probe it with httpx:
     https://www.{slug}.pt  →  https://{slug}.pt  →  https://www.{slug}.com

3. Parse the returned HTML for:
     a. <a href="tel:...">  — most reliable
     b. <a href="mailto:..."> — secondary
     c. Phone regex in visible page text — fallback

4. Contacts found for a given agency are applied to ALL leads from that
   same agency in the current DB (visit once, fill many rows).

Hard limits
-----------
  MAX_AGENCIES  40     unique agency websites visited per run
  DELAY_MIN      2.0s  between successive agency requests
  DELAY_MAX      4.0s  between successive agency requests
  TIMEOUT        8s    per HTTP request
  Only .pt / .com TLDs attempted.

contact_source format: "website:{domain}"  (max ~35 chars, fits String(50))
contact_confidence:    100 (phone found) | 70 (email only) | 40 (website only)

Safe to run multiple times — only fills NULL / empty fields.
"""
from __future__ import annotations

import random
import re
import time
import unicodedata
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from utils.email_extractor import extract_first_email
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Raised from 40 → 100: with delta-crawling on the scraper side, most runs
# will see only ~20-40 NEW agencies to try anyway, so a higher cap covers
# the first-run backlog without throttling steady-state runs.
MAX_AGENCIES = 100
DELAY_MIN    = 2.0
DELAY_MAX    = 4.0
TIMEOUT      = 8

# Paths probed after the homepage when it didn't yield any contact. PT
# agency sites overwhelmingly use one of these slugs for contact info.
_CONTACT_SUBPATHS: tuple[str, ...] = (
    "/contacto", "/contactos", "/contato", "/contatos",
    "/contact", "/contactar",
    "/equipa", "/equipe", "/agentes", "/consultores",
    "/sobre", "/sobre-nos", "/about", "/about-us",
)

# Major franchise chains — homepages carry brand/franchise content only.
# Visiting them wastes a request and may trigger bot protection.
_CHAIN_SKIP: frozenset[str] = frozenset({
    "remax", "re max", "re/max",
    "century21", "century 21",
    "kw", "kw lead", "keller williams",
    "era",
    "zome",
    "dils",
    "knight frank",
    "jll",
    "savills",
    "sotheby", "sothebys",
    "engel volkers", "engel & volkers",
    "coldwell banker",
    "ihf",
    "habivida",
    "predimed",      # franchise
    "oferta privada",  # placeholder, not an agency
})

# Noise words stripped before building the slug — generic business suffixes
_STRIP_WORDS: tuple[str, ...] = (
    "imobiliaria", "imobiliário", "imobiliaria",
    "real estate", "realestate",
    "lda", "sa", "s.a", "s.a.", "unipessoal",
    "grupo", "group",
    "consultores", "consultoria",
    "mediacao", "mediação", "mediadora",
    "imoveis", "imóveis", "imóvel", "imovel",
    "property", "properties",
    "sociedade", "gestao", "gestão",
    "comercio", "comércio",
)

# Domain parking / registrar sites — if we land here after redirect,
# the agency's domain doesn't exist and we must discard the result.
_PARKING_DOMAINS: frozenset[str] = frozenset({
    "hugedomains.com", "godaddy.com", "namecheap.com", "sedo.com",
    "dan.com", "afternic.com", "undeveloped.com", "parkingcrew.net",
    "bodis.com", "domainnamesales.com", "uniregistry.com", "dynadot.com",
    "register.com", "networksolutions.com", "domainmarket.com",
})

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Portuguese phone regex — matches mobile and landline in common formats
_PT_PHONE_RE = re.compile(
    r"(?<!\d)"
    r"(?:\+351[\s\-]?|00351[\s\-]?)?"
    r"([926]\d{2}[\s\-]?\d{3}[\s\-]?\d{3})"
    r"(?!\d)"
)


# ── Text helpers ──────────────────────────────────────────────────────────────

def _norm(text: str) -> str:
    """Lowercase, strip combining accents, collapse whitespace."""
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_).strip()


def _is_chain(agency_name: str) -> bool:
    """Return True if this agency name belongs to a major franchise chain."""
    n = _norm(agency_name)
    return any(chain in n for chain in _CHAIN_SKIP)


def _build_candidate_urls(agency_name: str) -> list[str]:
    """
    Derive up to 3 candidate homepage URLs from an agency name.

    Returns an empty list if the name is too short, a known chain,
    or produces a slug shorter than 3 characters.
    """
    if not agency_name or _is_chain(agency_name):
        return []

    slug = _norm(agency_name)

    # Remove noise words
    for word in _STRIP_WORDS:
        slug = re.sub(r"\b" + re.escape(word) + r"\b", " ", slug)

    # Extract pipe/separator first part  (e.g. "RB Real Estate | Ricardo Bettencourt" → "rb")
    slug = slug.split("|")[0].strip()

    # Keep only alphanumeric + spaces; convert spaces → hyphens
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "-", slug).strip("-")

    # Collapse multiple hyphens
    slug = re.sub(r"-{2,}", "-", slug)

    if len(slug) < 3:
        return []

    return [
        f"https://www.{slug}.pt",
        f"https://{slug}.pt",
        f"https://www.{slug}.com",
    ]


def _extract_contacts(html: str) -> dict[str, str | None]:
    """
    Parse HTML for phone and email using centralised extractors.

    Search order:
      1. <a href="tel:">   — explicit tel link (most reliable)
      2. Phone regex in visible page text (fallback)
      3. Email extraction via utils.email_extractor (mailto → JSON-LD →
         plain text → obfuscation), with built-in filtering of generic
         inboxes (noreply@, info@, dpo@) and telemetry domains.

    Returns {"phone": str|None, "email": str|None}.
    """
    soup = BeautifulSoup(html, "html.parser")
    phone: str | None = None

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if href.startswith("tel:") and not phone:
            raw = href[4:].strip()
            digits = re.sub(r"\D", "", raw.lstrip("+"))
            if len(digits) >= 9:
                phone = raw
                break

    # Phone fallback: regex in visible text
    if not phone:
        text = soup.get_text(" ", strip=True)
        m = _PT_PHONE_RE.search(text)
        if m:
            phone = m.group(1).strip()

    # Email: unified extractor covers mailto, JSON-LD, plain text, obfuscation.
    # When the homepage yields only a generic inbox (info@agency.pt), we keep
    # it rather than drop — it's still more useful than nothing for agencies,
    # where "info@" often reaches a real person. So we run with a permissive
    # fallback that retries without generic-rejection if the first pass returns
    # empty AND there was a mailto href present.
    email = extract_first_email(html)

    if not email:
        # Permissive fallback: accept the raw mailto href even if generic
        # (info@, contacto@) when no other email exists on the site.
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip().lower()
            if href.startswith("mailto:"):
                addr = href[7:].split("?")[0].strip()
                if (
                    "@" in addr and "." in addr
                    and len(addr) <= 200
                    and "sentry" not in addr
                    and "ingest" not in addr
                ):
                    email = addr
                    break

    return {"phone": phone, "email": email}


# ── Main class ────────────────────────────────────────────────────────────────

class WebsiteEnricher:
    """
    Visit agency homepages and backfill missing phone/email onto Lead rows.

    Typical usage:
        from storage.database import get_db
        from pipeline.website_enricher import WebsiteEnricher

        with get_db() as db:
            stats = WebsiteEnricher().run(db)
    """

    def run(self, db) -> dict:
        """
        Load eligible agency leads, visit their homepages, write back contacts.

        Eligible leads:
          • is_demo = False
          • archived = False
          • owner_type = 'agency'
          • agency_name not null/empty
          • contact_phone null/empty AND contact_email null/empty

        Returns stats dict:
          candidates  — leads evaluated
          sites_tried — distinct agency URL probes sent
          sites_ok    — probes that returned HTTP 200
          agencies_ok — agencies for which ≥1 contact was found
          phone       — phone numbers written to leads
          email       — email addresses written to leads
          website     — contact_website fields filled
          skipped     — leads skipped (chain, no slug, already contacted, cap)
        """
        from sqlalchemy import select, or_
        from storage.models import Lead

        leads: list[Lead] = db.execute(
            select(Lead)
            .where(Lead.is_demo != True)         # noqa: E712
            .where(Lead.archived == False)        # noqa: E712
            .where(Lead.owner_type == "agency")
            .where(Lead.agency_name.isnot(None))
            .where(Lead.agency_name != "")
            .where(
                or_(Lead.contact_phone.is_(None), Lead.contact_phone == "")
            )
            .where(
                or_(Lead.contact_email.is_(None), Lead.contact_email == "")
            )
        ).scalars().all()

        log.info(
            "[website_enricher] {n} agency leads without contact — starting enrichment",
            n=len(leads),
        )

        stats: dict[str, int] = {
            "candidates":  len(leads),
            "sites_tried": 0,
            "sites_ok":    0,
            "agencies_ok": 0,
            "phone":       0,
            "email":       0,
            "website":     0,
            "skipped":     0,
        }

        # Group leads by agency name — visit each unique agency once
        agency_map: dict[str, list[Lead]] = {}
        for lead in leads:
            name = (lead.agency_name or "").strip()
            if name:
                agency_map.setdefault(name, []).append(lead)

        agencies_visited = 0

        with httpx.Client(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers=_HEADERS,
        ) as client:

            for agency_name, agency_leads in agency_map.items():

                if agencies_visited >= MAX_AGENCIES:
                    remaining = sum(
                        len(v) for k, v in agency_map.items()
                        if k not in [
                            name for name, _ in list(agency_map.items())[:agencies_visited]
                        ]
                    )
                    stats["skipped"] += remaining
                    log.debug(
                        "[website_enricher] MAX_AGENCIES={m} reached — {r} leads skipped",
                        m=MAX_AGENCIES, r=remaining,
                    )
                    break

                # Skip chains and unresolvable names
                candidate_urls = _build_candidate_urls(agency_name)
                if not candidate_urls:
                    stats["skipped"] += len(agency_leads)
                    log.debug(
                        "[website_enricher] Skip '{a}' — chain or no slug",
                        a=agency_name,
                    )
                    continue

                # Probe candidate URLs — stop at first 200
                contacts: dict[str, str | None] = {"phone": None, "email": None}
                live_url: str | None = None
                live_base: str | None = None

                for url in candidate_urls:
                    try:
                        stats["sites_tried"] += 1
                        resp = client.get(url)

                        if resp.status_code == 200 and resp.text:
                            final_domain = urlparse(str(resp.url)).netloc.lstrip("www.")

                            # Reject domain parking / registrar redirects
                            if final_domain in _PARKING_DOMAINS:
                                log.debug(
                                    "[website_enricher] '{a}' → {d} is a parking site — skip",
                                    a=agency_name, d=final_domain,
                                )
                                break

                            contacts = _extract_contacts(resp.text)
                            live_url = str(resp.url)
                            live_base = f"{urlparse(live_url).scheme}://{urlparse(live_url).netloc}"
                            stats["sites_ok"] += 1
                            log.debug(
                                "[website_enricher] '{a}' → {d} | phone={p} email={e}",
                                a=agency_name,
                                d=final_domain,
                                p=contacts["phone"],
                                e=contacts["email"],
                            )
                            break

                    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError) as e:
                        log.debug(
                            "[website_enricher] {u} failed: {e}", u=url, e=type(e).__name__
                        )

                # ── Sub-page crawl — only when homepage missed a contact ─────
                # Most PT agency sites put phone on the header/footer of the
                # homepage but email under /contacto or /equipa. Only probe
                # these sub-paths when something is still missing.
                if live_base and (not contacts["phone"] or not contacts["email"]):
                    for sub in _CONTACT_SUBPATHS:
                        if contacts["phone"] and contacts["email"]:
                            break
                        sub_url = live_base + sub
                        try:
                            stats["sites_tried"] += 1
                            sub_resp = client.get(sub_url)
                            if sub_resp.status_code != 200 or not sub_resp.text:
                                continue
                            sub_contacts = _extract_contacts(sub_resp.text)
                            if not contacts["phone"] and sub_contacts["phone"]:
                                contacts["phone"] = sub_contacts["phone"]
                                log.debug(
                                    "[website_enricher] '{a}' sub-page {s} → phone={p}",
                                    a=agency_name, s=sub, p=sub_contacts["phone"],
                                )
                            if not contacts["email"] and sub_contacts["email"]:
                                contacts["email"] = sub_contacts["email"]
                                log.debug(
                                    "[website_enricher] '{a}' sub-page {s} → email={e}",
                                    a=agency_name, s=sub, e=sub_contacts["email"],
                                )
                        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
                            continue
                        # Light inter-subpage throttle to stay polite
                        time.sleep(0.3)

                # Polite delay between agencies (not between URL candidates)
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                agencies_visited += 1

                # Nothing found for this agency
                if not live_url and not contacts["phone"] and not contacts["email"]:
                    continue

                found_something = contacts["phone"] or contacts["email"] or live_url
                if found_something:
                    stats["agencies_ok"] += 1

                domain = (
                    urlparse(live_url).netloc.lstrip("www.")
                    if live_url else agency_name[:40]
                )
                contact_source_val = f"website:{domain}"[:50]

                # Apply found contacts to every lead from this agency
                for lead in agency_leads:
                    changed = False

                    if contacts["phone"] and not (lead.contact_phone or "").strip():
                        lead.contact_phone    = contacts["phone"]
                        lead.contact_source   = contact_source_val
                        lead.contact_confidence = 100
                        stats["phone"] += 1
                        changed = True

                    if contacts["email"] and not (lead.contact_email or "").strip():
                        lead.contact_email = contacts["email"]
                        if not changed:
                            lead.contact_source   = contact_source_val
                            lead.contact_confidence = 70
                        stats["email"] += 1
                        changed = True

                    if live_url and not (lead.contact_website or "").strip():
                        lead.contact_website = domain[:200]
                        if not changed:
                            lead.contact_source   = contact_source_val
                            lead.contact_confidence = 40
                        stats["website"] += 1
                        changed = True

        db.commit()

        log.info(
            "[website_enricher] Complete — candidates={c} | "
            "tried={t} ok={ok} agencies_ok={a} | "
            "phone=+{ph} email=+{em} website=+{web} skipped={sk}",
            c=stats["candidates"],
            t=stats["sites_tried"],
            ok=stats["sites_ok"],
            a=stats["agencies_ok"],
            ph=stats["phone"],
            em=stats["email"],
            web=stats["website"],
            sk=stats["skipped"],
        )
        return stats

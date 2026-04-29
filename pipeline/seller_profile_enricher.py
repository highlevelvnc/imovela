"""
Seller-profile enricher — extracts cross-listing seller intelligence from OLX
profile pages.

Goal
----
A single OLX listing tells you about ONE property. The seller's profile page
tells you about the **person** behind it: how many ads they're running, how
long they've been on the platform, and whether their listing pattern matches
that of a private owner, a repeat investor, or a camouflaged agency.

This signal is gold for Nuno's pipeline:
  * 1 active listing  → genuine FSBO, highest conversion potential
  * 2-4 listings      → small landlord / family business — strong leads
  * 5-9 listings      → repeat investor — high lifetime value
  * 10+ listings      → camouflaged agency, lower priority

The enricher visits each unique ``seller_profile_url`` exactly once per run,
counts active ads, captures "membro desde", and writes the data back to every
lead from that seller in one batch.

Input
-----
Reads from the ``leads`` table any row with:
  * source = 'olx'
  * seller_profile_url not null
  * seller_total_listings is null (avoid re-visiting already-enriched profiles)

Output
------
Updates each Lead row with:
  * seller_total_listings : int
  * seller_member_since   : str (verbatim e.g. "Membro desde Mar 2023")
  * seller_super_flag     : True when total_listings >= SUPER_FLAG_THRESHOLD

Also reclassifies obvious camouflaged-agency profiles by setting:
  * owner_type = 'agency' when total_listings >= AGENCY_RECLASSIFY_THRESHOLD
    AND current owner_type was 'fsbo' (only downgrades suspect FSBOs).

CLI: ``python main.py enrich-sellers``
"""
from __future__ import annotations

import random
import re
import time
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Profiles with >= this many active ads are flagged as super-sellers.
SUPER_FLAG_THRESHOLD: int = 5

# Profiles with >= this many active ads are reclassified from FSBO to agency.
# Higher than the super flag because some legitimate landlords have a few.
AGENCY_RECLASSIFY_THRESHOLD: int = 10

# Cap profiles per run — bounded request volume even with 1000s of new sellers.
MAX_PROFILES_PER_RUN: int = 200

DELAY_MIN  = 2.0
DELAY_MAX  = 5.0
TIMEOUT    = 12

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# OLX profile-page selectors. Validated live 2026-04 against /perfil/<slug>.
# Markup tends to drift, so each field has multiple fallbacks.
_PROFILE_SELECTORS = {
    # Active-ad count — appears on the user's grid as a header counter or
    # within an "Anúncios ativos" badge. Multiple variants to cover redesigns.
    "ad_count": [
        "[data-testid='user-ads-count']",
        "[data-testid='listings-counter']",
        "h2[class*='ads-count']",
        "span[class*='ads-count']",
    ],
    "member_since": [
        "[data-testid='member-since']",
        "[data-testid='registration-date']",
        "div[class*='member-since']",
        "span[class*='member-since']",
    ],
    # Cards on the profile page itself — fallback ad counter when the
    # numeric badge is missing: just count cards on page 1.
    "ad_card": [
        "div[data-cy='l-card']",
        "li[data-cy='l-card']",
        "article[data-testid='l-card']",
    ],
}


def _select_first(soup: BeautifulSoup, selectors: list[str]):
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return None


def _select_all(soup: BeautifulSoup, selectors: list[str]) -> list:
    for sel in selectors:
        cards = soup.select(sel)
        if cards:
            return cards
    return []


# ── Parsing helpers ──────────────────────────────────────────────────────────

# Extract the leading number from strings like "12 anúncios" or "Anúncios (7)"
_AD_COUNT_RE = re.compile(r"\b(\d{1,4})\b")


def _parse_ad_count(text: str) -> Optional[int]:
    if not text:
        return None
    m = _AD_COUNT_RE.search(text)
    if not m:
        return None
    val = int(m.group(1))
    # Defensive cap — anything above 500 is suspect (page chrome, year, …)
    if 0 <= val <= 500:
        return val
    return None


def _fetch_profile(client: httpx.Client, profile_url: str) -> Optional[dict]:
    """Fetch one profile page and extract counters + member date + phone."""
    try:
        resp = client.get(profile_url)
        if resp.status_code != 200 or not resp.text:
            log.debug(
                "[seller_profile] HTTP {c} on {u}",
                c=resp.status_code, u=profile_url[-70:],
            )
            return None
    except (httpx.HTTPError, httpx.NetworkError) as e:
        log.debug("[seller_profile] fetch error {u}: {e}", u=profile_url[-70:], e=e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Ad count: prefer the numeric counter, fallback to counting cards on page 1.
    count_el = _select_first(soup, _PROFILE_SELECTORS["ad_count"])
    ad_count = _parse_ad_count(count_el.get_text(" ", strip=True)) if count_el else None
    if ad_count is None:
        cards = _select_all(soup, _PROFILE_SELECTORS["ad_card"])
        if cards:
            ad_count = len(cards)

    member_el = _select_first(soup, _PROFILE_SELECTORS["member_since"])
    member_since = member_el.get_text(" ", strip=True)[:80] if member_el else None

    # Phone discovery on the profile page itself — sometimes super-sellers
    # publish a real number in their profile bio that the listing pages don't
    # carry. Prefer non-relay candidates.
    real_phone = None
    try:
        from utils.phone import best_phone
        from utils.phone_discovery import discover_phones, discover_whatsapp

        non_relay = discover_phones(resp.text, soup=soup, allow_relay=False)
        if non_relay:
            best = best_phone(non_relay)
            if best and best.valid and best.phone_type in ("mobile", "landline"):
                real_phone = best.canonical
        wa_list = discover_whatsapp(resp.text, soup=soup)
    except Exception as e:
        log.debug("[seller_profile] phone discovery: {e}", e=e)
        wa_list = []

    return {
        "ad_count":     ad_count,
        "member_since": member_since,
        "real_phone":   real_phone,
        "whatsapp":     wa_list[0] if wa_list else None,
    }


# ── Public runner ────────────────────────────────────────────────────────────

class SellerProfileEnricher:
    """Visits OLX seller profiles and writes aggregated data back to leads."""

    def run(self) -> dict:
        """
        Returns stats dict:
            candidates    — distinct profiles needing enrichment
            visited       — profiles actually fetched
            super_flagged — profiles ≥ SUPER_FLAG_THRESHOLD
            reclassified  — leads moved from FSBO to agency owner_type
            updated_leads — total lead rows updated
            errors        — fetch failures
        """
        from sqlalchemy import or_, select, update

        from storage.database import get_db
        from storage.models import Lead

        stats = {
            "candidates":     0,
            "visited":        0,
            "super_flagged":  0,
            "reclassified":   0,
            "updated_leads":  0,
            "errors":         0,
        }

        with get_db() as db:
            # Group leads by profile URL — visit each profile once
            rows = db.execute(
                select(Lead.seller_profile_url)
                .where(Lead.discovery_source == "olx")
                .where(Lead.seller_profile_url.isnot(None))
                .where(Lead.seller_profile_url != "")
                .where(or_(
                    Lead.seller_total_listings.is_(None),
                    Lead.seller_total_listings == 0,
                ))
                .distinct()
                .limit(MAX_PROFILES_PER_RUN)
            ).all()
            profile_urls = [r[0] for r in rows]
            stats["candidates"] = len(profile_urls)

        log.info(
            "[seller_profile] {n} unique profiles pending enrichment",
            n=stats["candidates"],
        )
        if not profile_urls:
            return stats

        with httpx.Client(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            },
        ) as client:
            for profile_url in profile_urls:
                data = _fetch_profile(client, profile_url)
                stats["visited"] += 1

                if data is None:
                    stats["errors"] += 1
                else:
                    self._apply(profile_url, data, stats)

                # Polite throttle between profiles
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        log.info(
            "[seller_profile] complete — visited={v} super={s} reclassified={r} "
            "updated={u} errors={e}",
            v=stats["visited"], s=stats["super_flagged"],
            r=stats["reclassified"], u=stats["updated_leads"], e=stats["errors"],
        )
        return stats

    def _apply(self, profile_url: str, data: dict, stats: dict) -> None:
        from storage.database import get_db
        from storage.models import Lead
        from utils.phone import best_phone, validate_pt_phone

        ad_count    = data.get("ad_count")
        member_text = data.get("member_since")
        real_phone  = data.get("real_phone")
        wa          = data.get("whatsapp")
        super_flag  = bool(ad_count and ad_count >= SUPER_FLAG_THRESHOLD)

        with get_db() as db:
            affected = db.query(Lead).filter(
                Lead.seller_profile_url == profile_url
            ).all()

            for lead in affected:
                lead.seller_total_listings = ad_count
                lead.seller_member_since   = member_text
                lead.seller_super_flag     = super_flag

                # ── Phone upgrade from profile ────────────────────────────
                # Only overwrite a stored phone when the new candidate is
                # strictly better (mobile > landline > relay). This is the
                # cross-listing channel that turns OLX 6XX relay numbers
                # into real direct mobile when the seller has one in their
                # bio.
                if real_phone:
                    current = lead.contact_phone
                    if not current:
                        lead.contact_phone        = real_phone
                        lead.contact_source       = "olx_profile"
                        lead.phone_type           = validate_pt_phone(real_phone).phone_type
                        lead.contact_confidence   = 90
                        stats.setdefault("phone_upgraded", 0)
                        stats["phone_upgraded"] += 1
                    else:
                        picked = best_phone([current, real_phone])
                        if picked and picked.canonical == real_phone and picked.canonical != current:
                            lead.contact_phone      = real_phone
                            lead.contact_source     = "olx_profile"
                            lead.phone_type         = picked.phone_type
                            lead.contact_confidence = picked.confidence
                            stats.setdefault("phone_upgraded", 0)
                            stats["phone_upgraded"] += 1

                if wa and not lead.contact_whatsapp:
                    lead.contact_whatsapp = wa

                # ── Reclassify camouflaged-agency profiles ────────────────
                if (
                    ad_count is not None
                    and ad_count >= AGENCY_RECLASSIFY_THRESHOLD
                    and lead.owner_type == "fsbo"
                ):
                    lead.owner_type = "agency"
                    lead.is_owner   = False
                    stats["reclassified"] += 1

                stats["updated_leads"] += 1

            db.commit()

        if super_flag:
            stats["super_flagged"] += 1

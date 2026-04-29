"""
Enricher — adds computed intelligence to normalised listings:
  - price_per_m2 calculation
  - benchmark price comparison
  - days on market
  - urgency keyword detection
  - agency website/contact lookup (known PT agencies)
  - geocoding (optional, via Nominatim)
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from utils.helpers import detect_urgency, is_owner_listing
from utils.logger import get_logger

log = get_logger(__name__)


# ─── Agency Contact Lookup ────────────────────────────────────────────────────
# Static mapping of known PT real estate agency name patterns → public contact.
# Applied in enrich() when no contact_website/phone is yet known.
# Sources: official agency websites (all public information).
# NOTE: these are central/HQ contacts, not individual agent numbers.
#       They still give Nuno a way to reach the agency listing the property.
_AGENCY_LOOKUP: list[tuple[re.Pattern, dict]] = [
    # National franchise networks
    (re.compile(r"\bremax\b|\bre/max\b|\bre\.max\b", re.I),
     {"website": "remax.pt"}),
    (re.compile(r"\bera\b", re.I),
     {"website": "era.pt"}),
    (re.compile(r"\bzome\b", re.I),
     {"website": "zome.pt"}),
    (re.compile(r"\bkw\b|\bkeller\s*williams\b|\bkwportugal\b", re.I),
     {"website": "kwportugal.pt"}),
    (re.compile(r"\bcentury\s*21\b", re.I),
     {"website": "century21.pt"}),
    (re.compile(r"\bcoldwell\s*banker\b", re.I),
     {"website": "coldwellbanker.pt"}),
    (re.compile(r"\bengel\b.*\bv[oö]lkers\b|\bev\b.*\breal\s*estate\b", re.I),
     {"website": "engelvoelkers.com/pt-pt"}),
    (re.compile(r"\bsotheby\b", re.I),
     {"website": "sothebysrealty.pt"}),
    # International boutique/premium
    (re.compile(r"\bknight\s*frank\b|\bquintela.*penalva\b", re.I),
     {"website": "knightfrank.pt"}),
    (re.compile(r"\bjll\b|\bjones\s*lang\b", re.I),
     {"website": "jll.pt"}),
    (re.compile(r"\bsavills\b", re.I),
     {"website": "savills.pt"}),
    (re.compile(r"\bcushman\b|\bwakefield\b", re.I),
     {"website": "cushmanwakefield.com"}),
    # PT-specific brands (validated presence)
    (re.compile(r"\bhabisale\b", re.I),
     {"website": "habisale.pt"}),
    (re.compile(r"\bpf\s*real\s*estate\b", re.I),
     {"website": "pf-realestate.pt"}),
    (re.compile(r"\blive\s*in\s*portugal\b", re.I),
     {"website": "liveinportugal.pt"}),
    (re.compile(r"\blisboa\s*invest\b", re.I),
     {"website": "lisboainvest.pt"}),
    (re.compile(r"\bpredimed\b", re.I),
     {"website": "predimed.pt"}),
    (re.compile(r"\bvital\s*prime\b|\bvitalprime\b", re.I),
     {"website": "vitalprime.pt"}),
    (re.compile(r"\bimovision\b", re.I),
     {"website": "imovision.pt"}),
    (re.compile(r"\bcoelho\s*da\s*silva\b", re.I),
     {"website": "coelhodasilva.pt"}),
    (re.compile(r"\bbernard.*raposo\b|\braposo.*bernard\b", re.I),
     {"website": "bernardesraposo.pt"}),
    (re.compile(r"\bimokuantum\b|\bkuantum\b", re.I),
     {"website": "imokuantum.pt"}),
    (re.compile(r"\b3c\s*imobili[aá]ria\b", re.I),
     {"website": "3cimobiliaria.pt"}),
]


# ─── Benchmark Prices (€/m²) ──────────────────────────────────────────────────
# Source: Confidencial Imobiliário / SIR data 2023-2024
# Update these values periodically or replace with DB/API lookup.

BENCHMARK_PRICES: dict[tuple[str, str], float] = {
    # (Zone, Typology): €/m²
    ("Lisboa",   "T0"):        5_200,
    ("Lisboa",   "T1"):        4_800,
    ("Lisboa",   "T2"):        4_500,
    ("Lisboa",   "T3"):        4_200,
    ("Lisboa",   "T4+"):       4_000,
    ("Lisboa",   "Moradia"):   4_800,
    ("Lisboa",   "Terreno"):   1_500,

    ("Cascais",  "T0"):        5_500,
    ("Cascais",  "T1"):        5_000,
    ("Cascais",  "T2"):        4_700,
    ("Cascais",  "T3"):        4_400,
    ("Cascais",  "T4+"):       4_200,
    ("Cascais",  "Moradia"):   5_200,
    ("Cascais",  "Terreno"):   1_800,

    ("Sintra",   "T0"):        3_200,
    ("Sintra",   "T1"):        2_900,
    ("Sintra",   "T2"):        2_700,
    ("Sintra",   "T3"):        2_500,
    ("Sintra",   "T4+"):       2_300,
    ("Sintra",   "Moradia"):   3_000,
    ("Sintra",   "Terreno"):   600,

    ("Almada",   "T0"):        3_000,
    ("Almada",   "T1"):        2_700,
    ("Almada",   "T2"):        2_500,
    ("Almada",   "T3"):        2_300,
    ("Almada",   "T4+"):       2_100,
    ("Almada",   "Moradia"):   2_800,

    ("Seixal",   "T0"):        2_400,
    ("Seixal",   "T1"):        2_200,
    ("Seixal",   "T2"):        2_000,
    ("Seixal",   "T3"):        1_900,
    ("Seixal",   "T4+"):       1_800,
    ("Seixal",   "Moradia"):   2_300,

    ("Sesimbra", "T0"):        2_800,
    ("Sesimbra", "T1"):        2_600,
    ("Sesimbra", "T2"):        2_400,
    ("Sesimbra", "T3"):        2_200,
    ("Sesimbra", "T4+"):       2_000,
    ("Sesimbra", "Moradia"):   2_700,
}

# Default fallback when zone/typology combo not found
DEFAULT_BENCHMARK = 2_500.0


class Enricher:

    def enrich(self, normalised: dict, first_seen_at: Optional[datetime] = None) -> dict:
        """
        Add enriched fields to the normalised dict.
        Mutates the dict in place and returns it.
        """
        enriched = dict(normalised)

        # ── Price per m² ──────────────────────────────────────────────────────
        price = enriched.get("price")
        area = enriched.get("area_m2")
        price_per_m2 = None

        if price and area and area > 0:
            price_per_m2 = round(price / area, 2)
            enriched["price_per_m2"] = price_per_m2

        # ── Benchmark comparison ──────────────────────────────────────────────
        zone = enriched.get("zone", "")
        typology = enriched.get("typology", "")
        benchmark = self._get_benchmark(zone, typology)
        enriched["price_benchmark"] = benchmark

        if price_per_m2 and benchmark:
            delta = (benchmark - price_per_m2) / benchmark * 100
            enriched["price_delta_pct"] = round(delta, 2)
            log.debug(
                "Price delta: {delta:.1f}% ({zone}/{typ} @ {pm2:.0f}€/m² vs bench {bench:.0f})",
                delta=delta, zone=zone, typ=typology, pm2=price_per_m2, bench=benchmark,
            )
        else:
            enriched["price_delta_pct"] = None

        # ── Days on market ────────────────────────────────────────────────────
        if first_seen_at:
            delta_days = (datetime.utcnow() - first_seen_at).days
            enriched["days_on_market"] = max(0, delta_days)

        # ── Urgency keywords ──────────────────────────────────────────────────
        description = enriched.get("description", "") or ""
        title = enriched.get("title", "") or ""
        urgency_score, matched_patterns = detect_urgency(description + " " + title)
        enriched["_urgency_score"] = urgency_score
        enriched["_urgency_patterns"] = matched_patterns

        # ── Owner detection ───────────────────────────────────────────────────
        if "is_owner" not in enriched:
            enriched["is_owner"] = is_owner_listing(
                title,
                description,
                enriched.get("contact_name", ""),
            )

        # ── owner_type default ────────────────────────────────────────────────
        if "owner_type" not in enriched or not enriched.get("owner_type"):
            enriched["owner_type"] = "fsbo" if enriched.get("is_owner") else "unknown"

        # ── Agency contact lookup ─────────────────────────────────────────────
        # If no website/phone/email known yet, attempt a lookup from the agency
        # name against the known PT agencies table.  Fills contact_website only;
        # keeps existing values unchanged. Called before confidence so the new
        # website value is reflected in the computed confidence tier.
        if not enriched.get("contact_website") and not enriched.get("contact_phone"):
            agency = enriched.get("agency_name") or ""
            contact_name = enriched.get("contact_name") or ""
            lookup_result = self._lookup_agency_contact(agency, contact_name)
            if lookup_result:
                enriched["contact_website"] = lookup_result.get("website") or enriched.get("contact_website")
                # Only fill phone from lookup if we have none AND lookup has one
                if not enriched.get("contact_phone") and lookup_result.get("phone"):
                    enriched["contact_phone"] = lookup_result["phone"]
                if not enriched.get("contact_source"):
                    enriched["contact_source"] = "agency_lookup"
                log.debug(
                    "[enricher] Agency lookup hit for {ag!r} → {ws}",
                    ag=agency[:40], ws=lookup_result.get("website", ""),
                )

        # ── Contact confidence ────────────────────────────────────────────────
        # Computed here so enricher is the single source of truth.
        # Hierarchy: phone/whatsapp=100 | email=70 | website=40 | name=30 | none=0
        # IMPORTANT: use `or None` to treat empty-string '' as falsy (same as None).
        phone    = enriched.get("contact_phone") or None
        whatsapp = enriched.get("contact_whatsapp") or None
        email    = enriched.get("contact_email") or None
        website  = enriched.get("contact_website") or None
        name     = enriched.get("contact_name") or None
        if phone or whatsapp:
            enriched["contact_confidence"] = 100
        elif email:
            enriched["contact_confidence"] = 70
        elif website:
            enriched["contact_confidence"] = 40
        elif name:
            enriched["contact_confidence"] = 30
        else:
            enriched["contact_confidence"] = 0

        # ── contact_source backfill ───────────────────────────────────────────
        # If any contact signal is present but contact_source wasn't set by the
        # normalizer or agency lookup, infer it from the discovery source.
        if (phone or whatsapp or email or website) and not enriched.get("contact_source"):
            enriched["contact_source"] = (
                enriched.get("source") or
                enriched.get("_source") or
                "listagem"
            )

        # ── Lead quality tier ─────────────────────────────────────────────────
        # high   = direct contact (phone/WA) + owner listing (fsbo/frbo)
        # medium = email/website contact, OR owner listing without direct contact
        # low    = everything else (agency, no contact)
        lead_type = enriched.get("lead_type", "unknown")
        is_owner_type = lead_type in ("fsbo", "frbo")
        if (phone or whatsapp) and is_owner_type:
            enriched["lead_quality"] = "high"
        elif email or website or is_owner_type:
            enriched["lead_quality"] = "medium"
        else:
            enriched["lead_quality"] = "low"

        # ── Geocoding (lightweight — optional) ────────────────────────────────
        if not enriched.get("latitude") and enriched.get("address"):
            coords = self._geocode(enriched["address"], zone)
            if coords:
                enriched["latitude"], enriched["longitude"] = coords

        return enriched

    @staticmethod
    def _lookup_agency_contact(agency_name: str, contact_name: str = "") -> dict | None:
        """
        Match agency_name (and optionally contact_name) against the known PT
        real estate agencies lookup table.

        Returns a dict with any of {website, phone} when a match is found,
        or None when no match is found.

        Conservative: only returns a result when the pattern is unambiguous.
        The `era` pattern is a short token and could match non-agency words
        (e.g. "era uma vez"), so it requires the combined string to not contain
        common false-positive words.
        """
        if not agency_name and not contact_name:
            return None

        combined = f"{agency_name} {contact_name}".strip()

        for pattern, contact_info in _AGENCY_LOOKUP:
            # Special guard for short 3-letter tokens (era) — require the match
            # to be isolated and not part of a common sentence ("era uma vez")
            pat_str = pattern.pattern
            if r"\bera\b" in pat_str:
                m = pattern.search(combined)
                if m:
                    # Reject if surrounded by common sentence words
                    context = combined.lower()
                    if any(w in context for w in ("era uma", "era o", "era a ", "era os", "era as")):
                        continue
                    return contact_info
            elif pattern.search(combined):
                return contact_info

        return None

    def _get_benchmark(self, zone: str, typology: str) -> float:
        """Look up benchmark price, with fallbacks."""
        # Exact match
        val = BENCHMARK_PRICES.get((zone, typology))
        if val:
            return float(val)

        # Try zone with default typology
        for typ in ("T2", "T3", "T1", "T0"):
            val = BENCHMARK_PRICES.get((zone, typ))
            if val:
                return float(val)

        return DEFAULT_BENCHMARK

    def _geocode(self, address: str, zone: str) -> Optional[tuple[float, float]]:
        """
        Lazy lookup via the cached Geocoder.

        Skips the Nominatim network call inside the pipeline's hot path —
        the offline ``main.py geocode-leads`` command does the network
        work in batch mode instead. Inline calls only return cache hits
        and zone-centroid fallbacks, neither of which blocks.
        """
        try:
            from utils.geocoder import Geocoder
            result = Geocoder().geocode(address, zone, allow_network=False)
            if result:
                return result.latitude, result.longitude
        except Exception as e:
            log.debug("Geocoder lookup failed for '{a}': {e}", a=address, e=e)
        return None

    def get_zone_stats(self) -> dict:
        """Return benchmark prices indexed by zone — useful for dashboard display."""
        stats: dict[str, dict] = {}
        for (zone, typology), price in BENCHMARK_PRICES.items():
            if zone not in stats:
                stats[zone] = {}
            stats[zone][typology] = price
        return stats

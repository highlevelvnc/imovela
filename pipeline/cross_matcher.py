"""
Cross-Portal Contact Matcher — pipeline/cross_matcher.py

Detects leads that represent the same physical property on different portals
and propagates contact information from the richer lead to the one missing it.

Problem this solves
-------------------
The deduplicator merges listings that share the *same* fingerprint
(typology + zone + price±1k + area±5m² + title-prefix). When two portals
price the same flat at €245 000 vs €250 000, or one lacks the area, the
fingerprints diverge → two separate Lead records, one possibly contactless.
This module bridges that gap without touching the deduplicator.

Scoring model
-------------
Four dimensions, each gated behind data availability:

  price_sim   weight=2.0   |Δprice| / max(price)  → 1.0 if ≤2%, 0 if >20%
  typology    weight=1.5   exact match → 1.0, else 0.0
  area_sim    weight=1.5   |Δarea| / max(area)    → 1.0 if ≤5%, 0 if >20%
  title_sim   weight=1.0   SequenceMatcher ratio on normalised title

Final score = weighted average over *active* dimensions only.
MATCH_THRESHOLD = 0.72

Hard gates (score = 0.0 immediately):
  • Same lead id
  • Same discovery_source (deduplicator already covers this)
  • Different zone (after accent/case normalisation)
  • Fewer than 2 active dimensions (insufficient data)
  • No price data and fewer than 3 active dimensions (risky without price)

Merge rules
-----------
  • phone / email / whatsapp / name / website copied only when target is NULL
  • contact_source set to "cross_portal:<donor_source>"
  • contact_confidence recalculated: phone/WA=100, email=70, website=40, name=30
  • Original scraped data on donor lead is never modified

No DB schema changes required — contact_source String(50) is large enough.
"""
from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher

from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MATCH_THRESHOLD = 0.72   # minimum score to consider two leads the same property


# ── Text normalisation ────────────────────────────────────────────────────────

def _norm_text(text: str) -> str:
    """
    Lowercase, strip combining accents, collapse punctuation/whitespace.
    Used for both zone comparison and title fuzzy matching.
    """
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_str)
    return re.sub(r"\s+", " ", cleaned).strip()


def _norm_zone(zone: str) -> str:
    return _norm_text(zone or "")


# ── Contact helpers ───────────────────────────────────────────────────────────

def _has_contact(lead) -> bool:
    """True if lead carries at least one primary contact (phone / email / WA)."""
    return bool(lead.contact_phone or lead.contact_email or lead.contact_whatsapp)


def _missing_contact(lead) -> bool:
    """True if lead is missing ALL primary contact fields."""
    return not _has_contact(lead)


# ── Core matching ─────────────────────────────────────────────────────────────

def score_match(a, b) -> float:
    """
    Compute similarity between two Lead ORM objects → [0.0, 1.0].

    Returns 0.0 immediately if any hard gate fails (different source required,
    same zone required, minimum data required).
    """
    # ── Hard gates ────────────────────────────────────────────────────────────
    if a.id == b.id:
        return 0.0
    # Same portal → deduplicator already handles; cross-match only bridges portals
    if (a.discovery_source or "") == (b.discovery_source or ""):
        return 0.0
    if not a.zone or not b.zone:
        return 0.0
    if _norm_zone(a.zone) != _norm_zone(b.zone):
        return 0.0

    signals: list[tuple[float, float]] = []   # (sub_score 0–1, weight)

    # ── Price (weight 2.0) ────────────────────────────────────────────────────
    price_active = bool(a.price and b.price and a.price > 0 and b.price > 0)
    if price_active:
        diff = abs(a.price - b.price) / max(a.price, b.price)
        if diff <= 0.02:
            p = 1.00
        elif diff <= 0.05:
            p = 0.85
        elif diff <= 0.10:
            p = 0.60
        elif diff <= 0.20:
            p = 0.25
        else:
            p = 0.00
        signals.append((p, 2.0))

    # ── Typology (weight 1.5) ─────────────────────────────────────────────────
    # Hard gate: if both leads carry typology and they differ → definitely not
    # the same property (a T2 and a T3 are never the same flat).
    if a.typology and b.typology:
        if a.typology.upper() != b.typology.upper():
            return 0.0
        signals.append((1.0, 1.5))

    # ── Area (weight 1.5) ─────────────────────────────────────────────────────
    area_active = bool(a.area_m2 and b.area_m2 and a.area_m2 > 0 and b.area_m2 > 0)
    if area_active:
        diff = abs(a.area_m2 - b.area_m2) / max(a.area_m2, b.area_m2)
        if diff <= 0.05:
            ar = 1.00
        elif diff <= 0.10:
            ar = 0.75
        elif diff <= 0.20:
            ar = 0.35
        else:
            ar = 0.00
        signals.append((ar, 1.5))

    # ── Title similarity (weight 1.0) ─────────────────────────────────────────
    ta = _norm_text(a.title or "")
    tb = _norm_text(b.title or "")
    if ta and tb:
        ratio = SequenceMatcher(None, ta, tb).ratio()
        signals.append((ratio, 1.0))

    # ── Minimum data guards ───────────────────────────────────────────────────
    if len(signals) < 2:
        return 0.0   # not enough data to decide

    # Without a price signal, require at least 3 matching dimensions —
    # typology+area+title for the same zone is borderline; adding price anchors it.
    if not price_active and len(signals) < 3:
        return 0.0

    # ── Weighted average ──────────────────────────────────────────────────────
    total_weight = sum(w for _, w in signals)
    total_score  = sum(s * w for s, w in signals)
    return total_score / total_weight


def find_candidate_matches(
    target,
    candidates: list,
    threshold: float = MATCH_THRESHOLD,
) -> list[tuple[object, float]]:
    """
    Search candidates for leads likely representing the same property as target.

    Only candidates that *have* contact information are considered (they are the
    donors). Returns a list of (lead, score) sorted descending by score.
    """
    results = []
    for candidate in candidates:
        if not _has_contact(candidate):
            continue   # nothing to donate
        if getattr(candidate, "is_demo", False):
            continue   # never propagate fictitious demo contact data
        # Prevent two classes of spurious propagation:
        #   cross_portal: — cascade guard: a lead that received contact via a
        #     prior cross-match must not re-donate; only primary scraped contacts
        #     (the ones that originated from the portal itself) are valid donors.
        #   website:      — agency-level guard: contacts found on a public agency
        #     homepage are generic office numbers, not property-specific; propagating
        #     them to different properties would assign the wrong contact.
        if (candidate.contact_source or "").startswith(("cross_portal:", "website:")):
            continue
        s = score_match(target, candidate)
        if s >= threshold:
            results.append((candidate, s))
    return sorted(results, key=lambda x: x[1], reverse=True)


def merge_contact_data(target, source) -> dict[str, int]:
    """
    Copy missing contact fields from source into target (Lead ORM objects).

    Only fills NULL fields — never overwrites data already on target.
    Updates contact_source and recalculates contact_confidence.
    Does NOT commit — the caller must manage the DB session.

    Returns a dict indicating how many of each field were gained:
      {"phone": 0|1, "email": 0|1, "whatsapp": 0|1, "name": 0|1, "website": 0|1}
    """
    gained: dict[str, int] = {
        "phone": 0, "email": 0, "whatsapp": 0, "name": 0, "website": 0,
    }

    # Phone propagation — copy when target has none, OR upgrade target's
    # relay (6XX) number to a real mobile/landline from the source lead.
    # The relay number "rings through" but isn't WhatsApp-able and isn't a
    # direct seller identifier, so when we see the same property on another
    # portal carrying a real number, we promote it.
    from utils.phone import best_phone, validate_pt_phone
    if source.contact_phone:
        target_real = (
            target.contact_phone
            and validate_pt_phone(target.contact_phone).phone_type in ("mobile", "landline")
        )
        if not target.contact_phone:
            target.contact_phone = source.contact_phone
            gained["phone"] = 1
        elif not target_real:
            picked = best_phone([target.contact_phone, source.contact_phone])
            if picked and picked.canonical == source.contact_phone \
                    and picked.canonical != target.contact_phone:
                target.contact_phone = source.contact_phone
                gained["phone"] = 1

    if not target.contact_email and source.contact_email:
        target.contact_email = source.contact_email
        gained["email"] = 1

    if not target.contact_whatsapp and source.contact_whatsapp:
        target.contact_whatsapp = source.contact_whatsapp
        gained["whatsapp"] = 1

    if not target.contact_name and source.contact_name:
        target.contact_name = source.contact_name
        gained["name"] = 1

    if not target.contact_website and source.contact_website:
        target.contact_website = source.contact_website
        gained["website"] = 1

    if any(gained.values()):
        donor_src = source.discovery_source or "unknown"
        # e.g. "cross_portal:idealista" — fits in contact_source String(50)
        target.contact_source = f"cross_portal:{donor_src}"

        # Recalculate confidence based on best available contact
        if target.contact_phone or target.contact_whatsapp:
            target.contact_confidence = 100
        elif target.contact_email:
            target.contact_confidence = 70
        elif target.contact_website:
            target.contact_confidence = 40
        elif target.contact_name:
            target.contact_confidence = 30

    return gained


# ── Main runner ───────────────────────────────────────────────────────────────

class CrossMatcher:
    """
    Runs cross-portal contact discovery over all non-archived leads in a DB session.

    Typical usage (standalone):
        from storage.database import get_db
        from pipeline.cross_matcher import CrossMatcher

        with get_db() as db:
            stats = CrossMatcher().run(db)

    Typical usage (integrated in PipelineRunner.run_full):
        stats = self.run_cross_match()
    """

    def run(self, db) -> dict:
        """
        Load all active leads, group by zone, find and merge cross-portal contacts.

        Returns:
            checked  — leads evaluated (had missing primary contact)
            matched  — leads that received ≥1 contact field
            phone    — phone numbers gained across all merges
            email    — email addresses gained
            whatsapp — WhatsApp numbers gained
            name     — contact names gained
            website  — contact websites gained
            skipped  — leads already contacted or with no matching donor found
        """
        from sqlalchemy import select
        from storage.models import Lead

        all_leads: list[Lead] = db.execute(
            select(Lead).where(Lead.archived == False)   # noqa: E712
        ).scalars().all()

        log.info("[cross_matcher] Loaded {n} active leads for cross-match scan", n=len(all_leads))

        # ── Index by normalised zone — O(zone_size²) instead of O(N²) ─────────
        by_zone: dict[str, list[Lead]] = {}
        for lead in all_leads:
            key = _norm_zone(lead.zone or "unknown")
            by_zone.setdefault(key, []).append(lead)

        stats: dict[str, int] = {
            "checked": 0, "matched": 0,
            "phone": 0, "email": 0, "whatsapp": 0, "name": 0, "website": 0,
            "skipped": 0,
        }

        for lead in all_leads:
            # Demo leads carry fictitious data — skip entirely (neither target nor donor)
            if getattr(lead, "is_demo", False):
                stats["skipped"] += 1
                continue

            # Skip leads that already have all primary contact channels
            if not _missing_contact(lead):
                stats["skipped"] += 1
                continue

            stats["checked"] += 1

            zone_key   = _norm_zone(lead.zone or "unknown")
            candidates = by_zone.get(zone_key, [])
            matches    = find_candidate_matches(lead, candidates)

            if not matches:
                stats["skipped"] += 1
                continue

            best_match, best_score = matches[0]
            gained = merge_contact_data(lead, best_match)

            if any(gained.values()):
                stats["matched"] += 1
                for field, count in gained.items():
                    stats[field] += count
                log.debug(
                    "[cross_matcher] Lead {a_id} ({a_src}) ← Lead {b_id} ({b_src}) "
                    "score={s:.2f} | gained={g}",
                    a_id=lead.id,
                    a_src=lead.discovery_source,
                    b_id=best_match.id,
                    b_src=best_match.discovery_source,
                    s=best_score,
                    g={k: v for k, v in gained.items() if v},
                )

        db.commit()

        log.info(
            "[cross_matcher] Complete — checked={c} matched={m} | "
            "phone=+{ph} email=+{em} wa=+{wa} website=+{web} | skipped={sk}",
            c=stats["checked"],
            m=stats["matched"],
            ph=stats["phone"],
            em=stats["email"],
            wa=stats["whatsapp"],
            web=stats["website"],
            sk=stats["skipped"],
        )
        return stats

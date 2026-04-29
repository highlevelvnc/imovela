"""
Lead Scorer — 12-dimension weighted scoring model (0–100).

Dimensions:
  1. Price Opportunity      (30 pts) — how far below market benchmark
  2. Urgency Signals        (25 pts) — keywords in title/description
  3. Owner Direct           (25 pts) — proprietário vs. agência; mobile bonus:
       fsbo + mobile        → +25  confirmed owner + best channel
       fsbo / frbo          → +20  direct owner
       active_owner+mobile  → +18  marketplace private seller + mobile
       active_owner         → +15  marketplace private seller
       unknown              → +10  unclassified — benefit of the doubt
       developer            → +5   direct but professional
       agency               → 0    agency intermediary
  4. Days on Market         (15 pts) — longer = more motivated seller
  5. Data Quality           ( 5 pts) — completeness of property data
  6. Zone Priority          ( 5 pts) — Lisboa/Cascais score higher
  7. Contact Quality   (-15..+20)   — differentiated by phone type + owner:
       mobile (FSBO/unknown) → +20  direct owner line, highest conversion
       mobile (agency)       → +15  agent's phone, not owner — reduced
       relay                 → +10  OLX masking — valid but indirect
       landline              → +5   often agency office line
       unknown               → +12  phone confirmed but type unclear
       email                 → +5   async outreach
       none                  → -15  cannot be actioned (penalty)
  8. Phone Type Bonus  (-8..+8)    — stacks on dim 7:
       mobile (FSBO/unknown) → +8   confirmed direct owner line
       mobile (agency)       → +3   agent's mobile (reduced)
       relay                 → 0    neutral
       landline 21x/22x      → -8   Lisboa/Porto agency lines
       landline other 2xx    → -3   mild penalty
       unknown 9xx (agency)  → 0    no unwarranted mobile assumption
  9. Recency Bonus          ( 5 pts) — listed within last 7 days
 10. Agency Penalty         (-10 pts) — confirmed agency (owner_type="agency")
 11. Contact Confidence     ( 3 pts) — based on contact_confidence field:
       ≥80 (mobile verified) → +3
       ≥60 (landline)        → +1
 12. Repeated Phone Penalty (0 / -5 / -10) — same phone in many listings:
       4–7 listings          → -5   likely agent managing multiple properties
       8+ listings           → -10  portfolio agent / agency pattern

Contact dimension totals by type (dims 7+8+11 combined):
  mobile FSBO direct  : +20 +8 +3 = +31  ← max actionable
  mobile agency       : +15 +3 +3 = +21  (agency downgrade applied)
  relay FSBO          : +10 +0 +0 = +10
  landline 21x agency :  +5 -8 +1 =  -2  ← agency lines penalised
  landline other      :  +5 -3 +1 =  +3
  email only          :  +5 +0 +0 =  +5
  no contact          : -15 +0 +0 = -15  ← cannot action

Quality gate:
  Leads with zero contact (no phone AND no email) are capped at
  HOT_THRESHOLD - 1, preventing un-actionable leads from ranking HOT
  regardless of property signals.

Output:
  score: int (0–100, clamped)
  label: "HOT" | "WARM" | "COLD"
  breakdown: dict with per-dimension scores for transparency

Notes:
  • Thresholds: HOT ≥ 60, WARM ≥ 40  (calibrated for PT market data)
  • Dim 3 mobile bonus stacks only for owner_type="fsbo"/"frbo" — not active_owner.
  • Dim 7 reduces agency mobile from +20 to +15 (agent's phone ≠ owner's phone).
  • Dim 10 raised from -5 to -10 to create clear gap between agency and FSBO.
  • Dim 12 requires _load_phone_freq() pre-call; safe to skip (returns 0 if unavail).
  • All dimensions are summed then clamped to 0–100.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from config.settings import settings
from storage.database import get_db
from storage.repository import LeadRepo
from utils.helpers import detect_urgency
from utils.logger import get_logger

log = get_logger(__name__)


# ── Zone priority weights ─────────────────────────────────────────────────────
ZONE_POINTS: dict[str, int] = {
    "Lisboa":   5,
    "Cascais":  5,
    "Sintra":   3,
    "Almada":   3,
    "Seixal":   2,
    "Sesimbra": 2,
}


@dataclass
class ScoreResult:
    total: int = 0
    label: str = "COLD"
    breakdown: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"total": self.total, "label": self.label, **self.breakdown}


class Scorer:

    def __init__(self) -> None:
        # Pre-loaded phone frequency map for repeated-phone detection (dim 12).
        # Populated by _load_phone_freq() before batch scoring.
        # Safe to skip — _score_repeated_phone() returns 0 when empty.
        self._phone_freq: dict[str, int] = {}
        # Pre-loaded premarket zone bonus map (dim 13).
        self._premarket_zones: dict[str, int] = {}

    def _load_phone_freq(self) -> None:
        """
        Pre-compute phone → listing-count map from the live DB.

        Call once before batch scoring to enable dim 12 (repeated phone penalty).
        Safe to call multiple times — replaces the previous map each time.
        """
        from sqlalchemy import text
        from storage.database import engine
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT contact_phone, COUNT(*) FROM leads "
                    "WHERE contact_phone IS NOT NULL AND is_demo=0 "
                    "GROUP BY contact_phone"
                )).fetchall()
                self._phone_freq = {str(r[0]): int(r[1]) for r in rows}
                log.debug(
                    "[Scorer] phone_freq loaded — {n} unique phones",
                    n=len(self._phone_freq),
                )
        except Exception as e:
            log.debug("[Scorer] _load_phone_freq failed (non-fatal): {e}", e=e)
            self._phone_freq = {}

    def score(self, lead) -> ScoreResult:
        """
        Score a Lead ORM object.
        Returns a ScoreResult with total, label and per-dimension breakdown.
        """
        result = ScoreResult()
        breakdown: dict[str, int] = {}

        # ── 1. Price Opportunity (max 30 pts) ─────────────────────────────────
        breakdown["price_opportunity"] = self._score_price(lead)

        # ── 2. Urgency Signals (max 25 pts) ───────────────────────────────────
        breakdown["urgency_signals"] = self._score_urgency(lead)

        # ── 3. Owner Direct (max 20 pts) ──────────────────────────────────────
        breakdown["owner_direct"] = self._score_owner(lead)

        # ── 4. Days on Market (max 15 pts) ────────────────────────────────────
        breakdown["days_on_market"] = self._score_days(lead)

        # ── 5. Data Quality (max 5 pts) ───────────────────────────────────────
        breakdown["data_quality"] = self._score_quality(lead)

        # ── 6. Zone Priority (max 5 pts) ──────────────────────────────────────
        breakdown["zone_priority"] = self._score_zone(lead)

        # ── 7. Contact Quality (-15 to +20) ───────────────────────────────────
        breakdown["contact_quality"] = self._score_contact(lead)

        # ── 8. Phone Type Bonus (-8 to +8) ────────────────────────────────────
        breakdown["phone_type_bonus"] = self._score_phone_type_bonus(lead)

        # ── 9. Recency Bonus (0 or +5) ────────────────────────────────────────
        breakdown["recency_bonus"] = self._score_recency(lead)

        # ── 10. Agency Penalty (0 or -10) ─────────────────────────────────────
        breakdown["agency_penalty"] = self._score_agency_penalty(lead)

        # ── 11. Contact Confidence Bonus (0 to +3) ────────────────────────────
        breakdown["contact_confidence_bonus"] = self._score_contact_confidence(lead)

        # ── 12. Repeated Phone Penalty (0 / -5 / -10) ─────────────────────────
        breakdown["repeated_phone_penalty"] = self._score_repeated_phone(lead)

        # ── 13. Pre-Market Signal Bonus (0 to +10) ────────────────────────────
        breakdown["premarket_signal_bonus"] = self._score_premarket(lead)

        result.total = sum(breakdown.values())
        result.total = max(0, min(100, result.total))  # clamp 0-100

        # ── Quality gate: zero-contact leads cannot be HOT ────────────────────
        # A lead with no phone AND no email is un-actionable regardless of
        # property signals. Cap it at HOT_THRESHOLD - 1 to keep HOT reserved
        # for leads that can actually be contacted.
        has_contact = (
            bool(getattr(lead, "contact_phone", None)) or
            bool(getattr(lead, "contact_email", None))
        )
        if not has_contact:
            result.total = min(result.total, settings.hot_score_threshold - 1)

        # Label
        if result.total >= settings.hot_score_threshold:
            result.label = "HOT"
        elif result.total >= settings.warm_score_threshold:
            result.label = "WARM"
        else:
            result.label = "COLD"

        result.breakdown = breakdown

        log.debug(
            "Scored lead {id}: {total} ({label}) — {bd}",
            id=getattr(lead, "id", "?"),
            total=result.total,
            label=result.label,
            bd=breakdown,
        )

        return result

    # ── Dimension scorers ────────────────────────────────────────────────────

    def _score_price(self, lead) -> int:
        """Score based on % below market benchmark."""
        delta = getattr(lead, "price_delta_pct", None)
        if delta is None:
            return 0
        if delta >= 20:
            return 30
        if delta >= 10:
            return 20
        if delta >= 5:
            return 10
        if delta >= 0:
            return 5
        return 0  # above market

    def _score_urgency(self, lead) -> int:
        """Detect urgency keywords in description + title."""
        text = " ".join(filter(None, [
            getattr(lead, "title", "") or "",
            getattr(lead, "description", "") or "",
        ]))
        pts, _ = detect_urgency(text)
        return min(pts, 25)

    def _score_owner(self, lead) -> int:
        """
        Score based on owner_type and lead_type, with mobile combination bonus.

        owner_type priority:
          fsbo + mobile     → +25  confirmed owner + best contact channel
          fsbo / frbo       → +20  direct owner, highest conversion, no agency fee
          active_owner+mob  → +18  marketplace private seller + mobile confirmed
          active_owner      → +15  marketplace seller, property owner signal
          unknown           → +10  unclassified — give benefit of the doubt
          developer         → +5   direct but professional, less flexible
          agency            → 0    agency intermediary, lowest priority

        Mobile bonus (+5 for fsbo/frbo, +3 for active_owner) stacks only when
        phone_type == "mobile", rewarding the ideal owner + direct channel combo.
        """
        owner_type = getattr(lead, "owner_type", None)
        lead_type  = getattr(lead, "lead_type", None)
        is_mobile  = getattr(lead, "phone_type", None) == "mobile"

        # FRBO landlords — direct owner renting; mobile adds certainty
        if lead_type == "frbo":
            return 25 if is_mobile else 20

        # Active owners (marketplace sellers) — mobile confirms private person
        if lead_type == "active_owner":
            return 18 if is_mobile else 15

        # Confirmed FSBO — mobile is the ideal combination
        if owner_type == "fsbo":
            return 25 if is_mobile else 20
        if owner_type == "unknown":
            return 10
        if owner_type == "developer":
            return 5
        if owner_type == "agency":
            return 0
        # Fallback for pre-migration leads without owner_type
        if getattr(lead, "is_owner", False):
            return 25 if is_mobile else 20
        return 0

    def _score_contact(self, lead) -> int:
        """
        Contact quality — differentiated by phone type (dim 7).

          mobile   → +20  direct owner line, highest conversion rate
          relay    → +10  OLX masking / VoIP forwarding — valid but indirect
          landline → +5   typically agency office; lower conversion
          unknown  → +12  phone present but type not classified
          email    → +5   async outreach only
          none     → -15  cannot be actioned (penalty)

        Range: -15 to +20.  Previously flat +15 for any phone.
        """
        phone = getattr(lead, "contact_phone", None)
        if phone:
            pt = getattr(lead, "phone_type", None) or ""
            if pt == "mobile":
                # Agency mobile = agent's phone, not owner's — lower conversion
                if getattr(lead, "owner_type", None) == "agency":
                    return 15
                return 20
            if pt == "relay":
                return 10
            if pt == "landline":
                return 5
            return 12  # phone present but phone_type not yet classified
        if getattr(lead, "contact_email", None):
            return 5
        return -15

    def _score_days(self, lead) -> int:
        """Longer on market → more motivated seller."""
        days = getattr(lead, "days_on_market", 0) or 0
        if days >= 90:
            return 15
        if days >= 60:
            return 10
        if days >= 30:
            return 5
        return 0

    def _score_quality(self, lead) -> int:
        """Reward completeness of key property data fields (contact scored separately)."""
        has_area  = bool(getattr(lead, "area_m2", None))
        has_zone  = bool(getattr(lead, "zone", None))
        has_price = bool(getattr(lead, "price", None))

        if has_area and has_zone and has_price:
            return 5
        if has_area and has_zone:
            return 3
        if has_price and has_zone:
            return 1
        return 0

    def _score_zone(self, lead) -> int:
        """Priority zones get higher scores."""
        zone = getattr(lead, "zone", "") or ""
        return ZONE_POINTS.get(zone, 1)

    def _score_phone_type_bonus(self, lead) -> int:
        """
        Phone type bonus/penalty — stacks with contact_quality (dim 8).

          mobile              → +8   confirmed direct owner, no agency intermediary
          relay               →  0   neutral (OLX relay valid but indirect)
          landline 21x/22x    → -8   Lisboa/Porto agency office lines — clear penalty
          landline other 2xx  → -3   regional landline — mild penalty
          no phone / unknown  →  0   no bonus, no penalty

        Combined contact total (dims 7+8):
          mobile   = +20 + 8 = +28  (best)
          relay    = +10 + 0 = +10
          landline 21x = +5 - 8 = -3  (agencies clearly ranked lower)
        """
        phone_type = getattr(lead, "phone_type", None)

        if phone_type == "mobile":
            # Reduce bonus for confirmed agencies — their mobile is an agent's phone,
            # not a direct owner number.  Still small positive (direct contact useful).
            if getattr(lead, "owner_type", None) == "agency":
                return 3
            return 8

        if phone_type == "relay":
            return 0

        if phone_type == "landline":
            phone = str(getattr(lead, "contact_phone", "") or "")
            national = phone.replace("+351", "").replace("00351", "").strip()
            if national[:2] in ("21", "22"):
                return -8   # Lisboa / Porto agency office lines
            return -3       # other regional landlines — mild penalty

        # "unknown" or unclassified — apply conservative fallback only for
        # leads that pre-date the phone_type migration (no phone_type field).
        # Do NOT award the full +8 for unclassified phones.
        phone = str(getattr(lead, "contact_phone", "") or "")
        national = phone.replace("+351", "").replace("00351", "").strip()
        if national[:2] in ("21", "22"):
            return -8
        if national and national[0] == "9":
            # Agency unknown 9xx → agent's mobile, not owner — no mobile assumption
            if getattr(lead, "owner_type", None) == "agency":
                return 0
            return 4    # conservative mobile-like bonus (lower than confirmed mobile)
        return 0

    def _score_contact_confidence(self, lead) -> int:
        """
        Contact confidence bonus — rewards validated, high-quality phone data (dim 11).

          confidence ≥ 80 → +3   (mobile number, pipeline-validated)
          confidence ≥ 60 → +1   (landline confirmed)
          else            →  0

        contact_confidence is set by pipeline/normalizer:
          mobile   = 90 | landline = 70 | relay = 50 | email/none = 0-30

        This dimension is intentionally small (+3 max) — it acts as a
        tiebreaker between leads with identical property signals, favouring
        those where the phone provenance is explicitly confirmed.
        """
        conf = getattr(lead, "contact_confidence", 0) or 0
        if conf >= 80:
            return 3
        if conf >= 60:
            return 1
        return 0

    def _score_recency(self, lead) -> int:
        """
        +5 bonus for listings first seen within the last 7 days.

        Fresh listings indicate recently-motivated sellers who have not yet
        negotiated, price-reduced, or been contacted by other buyers.
        Actionable window is typically 1–2 weeks after first appearance.
        """
        first_seen = getattr(lead, "first_seen_at", None)
        if not first_seen:
            return 0
        try:
            if isinstance(first_seen, str):
                first_seen = datetime.fromisoformat(first_seen)
            delta = (datetime.utcnow() - first_seen).days
            return 5 if delta <= 7 else 0
        except Exception:
            return 0

    def _score_agency_penalty(self, lead) -> int:
        """
        -10 penalty for confirmed agency leads.

        Stacks with _score_owner (which returns 0 for agencies) to create
        clear separation between agency leads and unknown/unclassified leads.
        An agency lead gets 0 + (-10) = -10 from owner dimensions, while an
        unknown lead gets 10 + 0 = +10 — a 20-pt gap.

        The -10 value (raised from -5) ensures that even agency leads with
        below-market pricing and mobile phones do not inflate into HOT,
        keeping HOT reserved for directly-contactable owner leads.
        """
        if getattr(lead, "owner_type", None) == "agency":
            return -10
        return 0

    def _score_repeated_phone(self, lead) -> int:
        """
        Penalty for phone numbers shared across many listings (dim 12).

        A phone appearing in 4+ leads signals an agent or agency rep managing
        a portfolio — not a direct owner. The penalty stacks with agency_penalty
        to amplify the signal when owner_type is already "agency", and provides
        independent detection when owner_type is still "unknown".

          4–7 listings  → -5   likely agent managing multiple properties
          8+ listings   → -10  clear agency portfolio pattern

        Requires _load_phone_freq() to be called before batch scoring.
        Returns 0 safely when phone_freq is empty (e.g. per-lead rescore).
        """
        if not self._phone_freq:
            return 0
        phone = str(getattr(lead, "contact_phone", "") or "")
        if not phone:
            return 0
        count = self._phone_freq.get(phone, 1)
        if count >= 8:
            return -10
        if count >= 4:
            return -5
        return 0

    def _score_premarket(self, lead) -> int:
        """
        Bonus for leads whose zone has pre-market signals (dim 13).

        Pre-market signals (building permits, renovation ads) indicate that
        a zone has property owners actively preparing to sell — even before
        listings appear. A lead in such a zone gets a bonus:

          building_permit in zone  → +10  strong sell intent signal
          renovation_ad in zone    → +5   softer signal (may be refurb)

        Requires _load_premarket_zones() called before batch scoring.
        Returns 0 safely when cache is empty.
        """
        if not self._premarket_zones:
            return 0
        zone = str(getattr(lead, "zone", "") or "").strip()
        if not zone:
            return 0
        return self._premarket_zones.get(zone, 0)

    def _load_premarket_zones(self) -> None:
        """Build a zone → bonus map from premarket_signals table."""
        self._premarket_zones: dict[str, int] = {}
        try:
            with get_db() as db:
                from sqlalchemy import text
                rows = db.execute(text('''
                    SELECT zone, signal_type, COUNT(*) FROM premarket_signals
                    WHERE zone IS NOT NULL AND zone != ''
                    GROUP BY zone, signal_type
                ''')).fetchall()
                for zone, sig_type, count in rows:
                    bonus = 10 if "permit" in (sig_type or "") else 5
                    current = self._premarket_zones.get(zone, 0)
                    self._premarket_zones[zone] = max(current, bonus)
            log.debug("Premarket zones loaded: {n}", n=len(self._premarket_zones))
        except Exception as e:
            log.debug("_load_premarket_zones error: {e}", e=e)

    # ── Bulk scoring ─────────────────────────────────────────────────────────

    def score_all_pending(self) -> int:
        """Score all leads that haven't been scored yet or need re-scoring. Returns count."""
        # Pre-load phone frequency map once for the full batch (dim 12 repeated phone)
        self._load_phone_freq()
        self._load_premarket_zones()

        scored = 0
        with get_db() as db:
            repo = LeadRepo(db)
            leads = repo.get_needs_rescore(hours=24)
            log.info("Scoring {n} leads...", n=len(leads))
            for lead in leads:
                result = self.score(lead)
                lead.score = result.total
                lead.score_label = result.label
                lead.set_score_breakdown(result.breakdown)
                lead.scored_at = datetime.utcnow()
                scored += 1

        log.info("Scoring complete — {n} leads scored", n=scored)
        return scored

    def rescore_lead(self, lead_id: int) -> Optional[ScoreResult]:
        """Re-score a specific lead by ID."""
        with get_db() as db:
            repo = LeadRepo(db)
            lead = repo.get_by_id(lead_id)
            if not lead:
                log.warning("Lead {id} not found for rescoring", id=lead_id)
                return None
            result = self.score(lead)
            lead.score = result.total
            lead.score_label = result.label
            lead.set_score_breakdown(result.breakdown)
            lead.scored_at = datetime.utcnow()
        return result

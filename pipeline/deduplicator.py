"""
Deduplicator — detects and merges duplicate listings across sources.

Fingerprint strategy:
  hash(typology + zone + round(price, -3) + round(area_m2, 0))

If two listings from different sources have the same fingerprint,
they represent the same property — we merge sources instead of duplicating.
"""
from __future__ import annotations

from utils.helpers import fingerprint, slugify_text
from utils.logger import get_logger

log = get_logger(__name__)


class Deduplicator:

    def compute_fingerprint(self, normalised: dict) -> str:
        """
        Build a stable fingerprint from the most stable fields.
        Rounds price to nearest 1000 and area to nearest 5 to absorb minor differences.
        """
        typology = (normalised.get("typology") or "").lower()
        zone = (normalised.get("zone") or "").lower()

        price = normalised.get("price") or 0
        price_rounded = round(price / 1000) * 1000  # round to nearest €1k

        area = normalised.get("area_m2") or 0
        area_rounded = round(area / 5) * 5  # round to nearest 5m²

        # Title slug as secondary disambiguation (first 6 words)
        title = normalised.get("title") or ""
        title_slug = slugify_text(" ".join(title.split()[:6]))

        fp = fingerprint(typology, zone, str(price_rounded), str(area_rounded), title_slug)
        log.debug(
            "Fingerprint {fp} ← type={t} zone={z} price≈{p} area≈{a}",
            fp=fp, t=typology, z=zone, p=price_rounded, a=area_rounded,
        )
        return fp

    def should_update(self, existing_lead, normalised: dict) -> bool:
        """
        Decide whether a re-seen listing warrants an update.
        Always update last_seen_at; only update price/area if changed.
        """
        return True  # always touch last_seen_at via the update path

    def merge_sources(self, existing_sources: list[dict], new_source: str, new_url: str) -> list[dict]:
        """Add a new source reference if it doesn't already exist."""
        existing_source_names = {s["source"] for s in existing_sources}
        if new_source in existing_source_names:
            return existing_sources
        from datetime import datetime
        return existing_sources + [{"source": new_source, "url": new_url, "seen_at": datetime.utcnow().isoformat()}]

    def detect_price_change(self, current_price: float | None, new_price: float | None) -> bool:
        """Return True if the price has changed by more than 0.5%."""
        if current_price is None or new_price is None:
            return False
        if current_price == 0:
            return new_price != 0
        pct_change = abs((new_price - current_price) / current_price)
        return pct_change > 0.005  # 0.5% threshold

    def build_update_payload(self, existing_lead, normalised: dict) -> dict:
        """
        Build the dict of fields to update on an existing lead.
        Preserves manually set CRM fields; updates market data.
        """
        payload: dict = {
            "last_seen_at": __import__("datetime").datetime.utcnow(),
        }

        # Update price if changed
        new_price = normalised.get("price")
        if new_price and self.detect_price_change(existing_lead.price, new_price):
            payload["price"] = new_price
            payload["price_changes"] = (existing_lead.price_changes or 0) + 1
            log.info(
                "Price change detected for lead {id}: {old} → {new}",
                id=existing_lead.id, old=existing_lead.price, new=new_price,
            )

        # Update area if we now have it and didn't before
        if normalised.get("area_m2") and not existing_lead.area_m2:
            payload["area_m2"] = normalised["area_m2"]

        # Update contact info if we now have it
        if normalised.get("contact_phone") and not existing_lead.contact_phone:
            payload["contact_phone"] = normalised["contact_phone"]
        if normalised.get("contact_email") and not existing_lead.contact_email:
            payload["contact_email"] = normalised["contact_email"]
        if normalised.get("contact_name") and not existing_lead.contact_name:
            payload["contact_name"] = normalised["contact_name"]

        # Update description if richer
        new_desc = normalised.get("description", "")
        if new_desc and len(new_desc) > len(existing_lead.description or ""):
            payload["description"] = new_desc

        # Update days on market
        from datetime import datetime
        if existing_lead.first_seen_at:
            delta = datetime.utcnow() - existing_lead.first_seen_at
            payload["days_on_market"] = delta.days

        return payload

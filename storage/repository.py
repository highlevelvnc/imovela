"""
Repository pattern — all DB access goes through these classes.
Keeps business logic clean and makes testing easier (swap repo for mocks).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy.orm import Session

from storage.models import Alert, CRMNote, Lead, PriceHistory, RawListing
from utils.logger import get_logger

log = get_logger(__name__)


# ─── RawListing ───────────────────────────────────────────────────────────────

class RawListingRepo:
    def __init__(self, db: Session):
        self.db = db

    def create(self, source: str, url: str, raw_data: dict,
               external_id: str = None, batch_id: str = None) -> RawListing:
        import json
        listing = RawListing(
            source=source,
            url=url,
            raw_data=json.dumps(raw_data, ensure_ascii=False, default=str),
            external_id=external_id,
            batch_id=batch_id,
        )
        self.db.add(listing)
        self.db.flush()
        return listing

    def get_unprocessed(self, source: str = None, limit: int = 500) -> List[RawListing]:
        q = self.db.query(RawListing).filter(RawListing.processed == False)
        if source:
            q = q.filter(RawListing.source == source)
        return q.order_by(RawListing.scraped_at.asc()).limit(limit).all()

    def mark_processed(self, listing_id: int) -> None:
        self.db.query(RawListing).filter(RawListing.id == listing_id).update(
            {"processed": True}
        )

    def exists(self, source: str, external_id: str) -> bool:
        return self.db.query(RawListing.id).filter(
            RawListing.source == source,
            RawListing.external_id == external_id,
        ).first() is not None

    def get_external_ids(self, source: str) -> set[str]:
        """
        Return every external_id previously captured for this source.
        Used by the delta-crawl pass to stop pagination early once a page
        is mostly-seen.
        """
        rows = self.db.query(RawListing.external_id).filter(
            RawListing.source == source,
            RawListing.external_id.isnot(None),
        ).all()
        return {r[0] for r in rows if r[0]}

    def count_today(self, source: str = None) -> int:
        today = datetime.utcnow().date()
        q = self.db.query(RawListing).filter(
            RawListing.scraped_at >= today
        )
        if source:
            q = q.filter(RawListing.source == source)
        return q.count()


# ─── Lead ─────────────────────────────────────────────────────────────────────

class LeadRepo:
    def __init__(self, db: Session):
        self.db = db

    def get_by_fingerprint(self, fingerprint: str) -> Optional[Lead]:
        return self.db.query(Lead).filter(Lead.fingerprint == fingerprint).first()

    def get_by_id(self, lead_id: int) -> Optional[Lead]:
        return self.db.query(Lead).filter(Lead.id == lead_id).first()

    def create(self, data: dict) -> Lead:
        lead = Lead(**data)
        self.db.add(lead)
        self.db.flush()
        log.debug("Created lead id={id} zone={zone} price={price}", **{
            "id": lead.id, "zone": lead.zone, "price": lead.price
        })
        return lead

    def update(self, lead: Lead, data: dict) -> Lead:
        for key, value in data.items():
            if hasattr(lead, key):
                setattr(lead, key, value)
        lead.updated_at = datetime.utcnow()
        self.db.flush()
        return lead

    def upsert(self, fingerprint: str, data: dict) -> tuple[Lead, bool]:
        """Returns (lead, created). If lead exists, updates it; otherwise creates."""
        existing = self.get_by_fingerprint(fingerprint)
        if existing:
            self.update(existing, data)
            return existing, False
        data["fingerprint"] = fingerprint
        return self.create(data), True

    # ── Queries ───────────────────────────────────────────────────────────────

    def list_active(
        self,
        zone: str = None,
        typology: str = None,
        score_min: int = None,
        crm_stage: str = None,
        label: str = None,
        is_demo: bool = None,   # None = all | False = real only | True = demo only
        contact: str = None,    # None=all | "any"=has contact | "phone"=has phone | "email"=has email | "none"=no contact
        owner_type: str = None, # None=all | "fsbo" | "agency" | "developer" | "unknown"
        limit: int = 200,
        offset: int = 0,
    ) -> List[Lead]:
        from sqlalchemy import or_
        q = self.db.query(Lead).filter(Lead.archived == False)
        if is_demo is not None:
            q = q.filter(Lead.is_demo == is_demo)
        if contact == "any":
            q = q.filter(or_(Lead.contact_phone.isnot(None), Lead.contact_email.isnot(None)))
        elif contact == "phone":
            q = q.filter(Lead.contact_phone.isnot(None))
        elif contact == "email":
            q = q.filter(Lead.contact_email.isnot(None))
        elif contact == "none":
            q = q.filter(Lead.contact_phone.is_(None), Lead.contact_email.is_(None))
        if zone:
            q = q.filter(Lead.zone == zone)
        if typology:
            q = q.filter(Lead.typology == typology)
        if score_min is not None:
            q = q.filter(Lead.score >= score_min)
        if crm_stage:
            q = q.filter(Lead.crm_stage == crm_stage)
        if label:
            q = q.filter(Lead.score_label == label)
        if owner_type:
            q = q.filter(Lead.owner_type == owner_type)
        # Real data first (is_demo=False sorts before True), then by score desc
        return q.order_by(Lead.is_demo.asc(), Lead.score.desc()).offset(offset).limit(limit).all()

    def delete_demo(self) -> int:
        """
        Delete all demo leads (is_demo=True) and their related records.
        Returns number of leads deleted.
        """
        demo_ids = [
            row[0] for row in
            self.db.query(Lead.id).filter(Lead.is_demo == True).all()
        ]
        if not demo_ids:
            return 0
        from storage.models import Alert, CRMNote, PriceHistory
        self.db.query(Alert      ).filter(Alert.lead_id      .in_(demo_ids)).delete(synchronize_session=False)
        self.db.query(CRMNote    ).filter(CRMNote.lead_id    .in_(demo_ids)).delete(synchronize_session=False)
        self.db.query(PriceHistory).filter(PriceHistory.lead_id.in_(demo_ids)).delete(synchronize_session=False)
        n = self.db.query(Lead).filter(Lead.is_demo == True).delete(synchronize_session=False)
        return n

    def get_hot_leads(self, threshold: int = 75) -> List[Lead]:
        return (
            self.db.query(Lead)
            .filter(Lead.score >= threshold, Lead.archived == False)
            .order_by(Lead.score.desc())
            .all()
        )

    def get_unscored(self) -> List[Lead]:
        return self.db.query(Lead).filter(Lead.scored_at == None).all()

    def get_needs_rescore(self, hours: int = 24) -> List[Lead]:
        """Leads that haven't been scored in the last N hours."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return (
            self.db.query(Lead)
            .filter(
                (Lead.scored_at == None) | (Lead.scored_at < cutoff),
                Lead.archived == False,
            )
            .all()
        )

    def count_by_label(self) -> dict[str, int]:
        from sqlalchemy import func
        rows = (
            self.db.query(Lead.score_label, func.count(Lead.id))
            .filter(Lead.archived == False)
            .group_by(Lead.score_label)
            .all()
        )
        return {label: count for label, count in rows}

    def count_by_zone(self) -> dict[str, int]:
        from sqlalchemy import func
        rows = (
            self.db.query(Lead.zone, func.count(Lead.id))
            .filter(Lead.archived == False)
            .group_by(Lead.zone)
            .all()
        )
        return {zone: count for zone, count in rows}

    def count_by_stage(self) -> dict[str, int]:
        from sqlalchemy import func
        rows = (
            self.db.query(Lead.crm_stage, func.count(Lead.id))
            .filter(Lead.archived == False)
            .group_by(Lead.crm_stage)
            .all()
        )
        return {stage: count for stage, count in rows}

    def total_active(self) -> int:
        return self.db.query(Lead).filter(Lead.archived == False).count()

    def added_today(self) -> int:
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return self.db.query(Lead).filter(Lead.created_at >= today_start).count()

    def record_price(self, lead_id: int, price: float, source: str) -> None:
        ph = PriceHistory(lead_id=lead_id, price=price, source=source)
        self.db.add(ph)

    def update_crm_stage(self, lead_id: int, stage: str) -> None:
        self.db.query(Lead).filter(Lead.id == lead_id).update({
            "crm_stage": stage,
            "updated_at": datetime.utcnow(),
        })


# ─── CRM Notes ────────────────────────────────────────────────────────────────

class CRMNoteRepo:
    def __init__(self, db: Session):
        self.db = db

    def add(self, lead_id: int, note: str,
            note_type: str = "internal", created_by: str = "Nuno Reis") -> CRMNote:
        crm = CRMNote(lead_id=lead_id, note=note,
                      note_type=note_type, created_by=created_by)
        self.db.add(crm)
        self.db.flush()
        return crm

    def list_for_lead(self, lead_id: int) -> List[CRMNote]:
        return (
            self.db.query(CRMNote)
            .filter(CRMNote.lead_id == lead_id)
            .order_by(CRMNote.created_at.desc())
            .all()
        )


# ─── Alert log ────────────────────────────────────────────────────────────────

class AlertRepo:
    def __init__(self, db: Session):
        self.db = db

    def log(self, alert_type: str, channel: str,
            lead_id: int = None, payload: dict = None,
            success: bool = True, error_msg: str = None) -> Alert:
        import json
        alert = Alert(
            lead_id=lead_id,
            alert_type=alert_type,
            channel=channel,
            payload=json.dumps(payload, default=str) if payload else None,
            success=success,
            error_msg=error_msg,
        )
        self.db.add(alert)
        self.db.flush()
        return alert

    def already_alerted_today(self, lead_id: int, alert_type: str) -> bool:
        today = datetime.utcnow().date()
        return (
            self.db.query(Alert.id)
            .filter(
                Alert.lead_id == lead_id,
                Alert.alert_type == alert_type,
                Alert.sent_at >= today,
                Alert.success == True,
            )
            .first()
        ) is not None

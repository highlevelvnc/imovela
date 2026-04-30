"""
SQLAlchemy ORM models.
Uses String columns for JSON data to stay compatible with both SQLite and PostgreSQL.
For PostgreSQL migration: replace String JSON columns with JSONB where noted.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ─── Raw Listings ─────────────────────────────────────────────────────────────

class RawListing(Base):
    """
    Immutable store of scraped data exactly as received from the source.
    Never modified after creation — provides audit trail.
    """
    __tablename__ = "raw_listings"

    id:          Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    source:      Mapped[str]           = mapped_column(String(50), nullable=False, index=True)
    external_id: Mapped[Optional[str]] = mapped_column(String(200), index=True)
    url:         Mapped[str]           = mapped_column(Text, nullable=False)
    raw_data:    Mapped[str]           = mapped_column(Text, nullable=False)  # JSON string — use JSONB in PG
    scraped_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow, index=True)
    processed:   Mapped[bool]          = mapped_column(Boolean, default=False, index=True)
    batch_id:    Mapped[Optional[str]] = mapped_column(String(36), index=True)  # UUID of the scraping run

    def get_data(self) -> dict[str, Any]:
        return json.loads(self.raw_data) if self.raw_data else {}

    def set_data(self, data: dict[str, Any]) -> None:
        self.raw_data = json.dumps(data, ensure_ascii=False)

    def __repr__(self) -> str:
        return f"<RawListing id={self.id} source={self.source} ext={self.external_id}>"


# ─── Leads ────────────────────────────────────────────────────────────────────

class Lead(Base):
    """
    Normalised, deduplicated, enriched and scored lead.
    One record per unique property regardless of how many sources reported it.
    """
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Deduplication
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    # ── Property ─────────────────────────────────────────────────────────────
    title:           Mapped[Optional[str]]   = mapped_column(Text)
    typology:        Mapped[Optional[str]]   = mapped_column(String(20), index=True)
    # Semantic property category: Apartamento / Moradia / Terreno / Quinta / Loja / Garagem
    property_type:   Mapped[Optional[str]]   = mapped_column(String(50))
    area_m2:         Mapped[Optional[float]] = mapped_column(Float)
    price:           Mapped[Optional[float]] = mapped_column(Float, index=True)
    price_per_m2:    Mapped[Optional[float]] = mapped_column(Float)
    price_benchmark: Mapped[Optional[float]] = mapped_column(Float)   # avg €/m² for zone+typology
    price_delta_pct: Mapped[Optional[float]] = mapped_column(Float)   # % below benchmark (positive = cheap)
    condition:       Mapped[Optional[str]]   = mapped_column(String(50))
    description:     Mapped[Optional[str]]   = mapped_column(Text)

    # Marketplace / Active Owner fields (olx_marketplace leads)
    # product_title: what the seller is selling (e.g. "Sofá IKEA 3 lugares")
    # product_value: asking price for the item (€)
    product_title:   Mapped[Optional[str]]   = mapped_column(Text)
    product_value:   Mapped[Optional[float]] = mapped_column(Float)

    # ── Location ─────────────────────────────────────────────────────────────
    zone:         Mapped[Optional[str]]   = mapped_column(String(100), index=True)
    municipality: Mapped[Optional[str]]   = mapped_column(String(100), index=True)  # e.g. "Cascais"
    parish:       Mapped[Optional[str]]   = mapped_column(String(100))              # e.g. "Carcavelos e Parede"
    address:      Mapped[Optional[str]]   = mapped_column(Text)
    latitude:     Mapped[Optional[float]] = mapped_column(Float)
    longitude:    Mapped[Optional[float]] = mapped_column(Float)

    # ── Contact ──────────────────────────────────────────────────────────────
    contact_name:     Mapped[Optional[str]] = mapped_column(String(200))
    first_name:       Mapped[Optional[str]] = mapped_column(String(100))
    last_name:        Mapped[Optional[str]] = mapped_column(String(100))
    birthday:         Mapped[Optional[str]] = mapped_column(String(50))
    contact_phone:    Mapped[Optional[str]] = mapped_column(String(50))
    # Type of phone number: "mobile" | "landline" | "relay" | "unknown"
    # mobile   = 9[1236]x (MEO/Vodafone/NOS)  — highest conversion, direct owner
    # landline = 2xx (Lisboa 21x, Porto 22x…)  — often agency office line
    # relay    = 6xx (OLX masking service)     — valid but not direct owner number
    # unknown  = valid digits, unclassified prefix
    phone_type:       Mapped[Optional[str]] = mapped_column(String(10), default="unknown")
    contact_email:    Mapped[Optional[str]] = mapped_column(String(200))
    contact_whatsapp: Mapped[Optional[str]] = mapped_column(String(50))   # canonical +351XXXXXXXXX
    contact_website:  Mapped[Optional[str]] = mapped_column(String(200))  # clean domain (empresa.pt)
    is_owner:         Mapped[bool]          = mapped_column(Boolean, default=False)
    agency_name:   Mapped[Optional[str]] = mapped_column(String(200))

    # ── Seller-profile sweep fields (OLX) ────────────────────────────────────
    # Populated by ``pipeline.seller_profile_enricher`` from the OLX seller
    # profile page. Used to detect "super sellers" — accounts with many
    # active listings that are likely camouflaged agencies or repeat investors.
    seller_profile_url:    Mapped[Optional[str]] = mapped_column(String(300))
    seller_total_listings: Mapped[Optional[int]] = mapped_column(Integer)
    seller_member_since:   Mapped[Optional[str]] = mapped_column(String(80))
    seller_super_flag:     Mapped[bool]          = mapped_column(Boolean, default=False)

    # ── Photo perceptual hash ────────────────────────────────────────────────
    # Populated by ``utils.image_hasher.backfill_image_hashes`` from the
    # first listing image. Consumed by ``photo_dedup_sweep`` to merge cross-
    # portal duplicates that the fingerprint deduper can't catch.
    image_phash:           Mapped[Optional[str]] = mapped_column(String(20), index=True)

    # ── Amenity tags ────────────────────────────────────────────────────────
    # Comma-separated canonical tags ("piscina,garagem,vista_mar") populated
    # by ``utils.amenity_tags.extract_amenities`` from title + description.
    # Populated at upsert time by the normalizer + during initial backfill.
    amenity_tags:          Mapped[Optional[str]] = mapped_column(String(400))

    # ── Sources (JSON list) ───────────────────────────────────────────────────
    # Format: [{"source": "olx", "url": "...", "seen_at": "ISO datetime"}]
    # PostgreSQL migration: replace with JSONB column
    sources_json: Mapped[str] = mapped_column(Text, default="[]")

    @property
    def sources(self) -> list[dict]:
        return json.loads(self.sources_json or "[]")

    @sources.setter
    def sources(self, value: list[dict]) -> None:
        self.sources_json = json.dumps(value, ensure_ascii=False, default=str)

    def add_source(self, source: str, url: str) -> None:
        existing = self.sources
        seen_sources = {s["source"] for s in existing}
        if source not in seen_sources:
            existing.append({"source": source, "url": url, "seen_at": datetime.utcnow().isoformat()})
            self.sources = existing

    # ── Market timing ────────────────────────────────────────────────────────
    first_seen_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    last_seen_at:   Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    days_on_market: Mapped[int]           = mapped_column(Integer, default=0)
    price_changes:  Mapped[int]           = mapped_column(Integer, default=0)

    # ── Scoring ──────────────────────────────────────────────────────────────
    score:           Mapped[int]          = mapped_column(Integer, default=0, index=True)
    score_label:     Mapped[str]          = mapped_column(String(10), default="COLD")
    score_breakdown: Mapped[str]          = mapped_column(Text, default="{}")  # JSON — JSONB in PG
    scored_at:       Mapped[Optional[datetime]] = mapped_column(DateTime)

    def get_score_breakdown(self) -> dict:
        return json.loads(self.score_breakdown or "{}")

    def set_score_breakdown(self, data: dict) -> None:
        self.score_breakdown = json.dumps(data)

    # ── CRM ──────────────────────────────────────────────────────────────────
    crm_stage:     Mapped[str]           = mapped_column(String(50), default="novo", index=True)
    assigned_to:   Mapped[Optional[str]] = mapped_column(String(100))
    priority_flag: Mapped[bool]          = mapped_column(Boolean, default=False)
    archived:      Mapped[bool]          = mapped_column(Boolean, default=False, index=True)

    # ── Data origin & provenance ──────────────────────────────────────────────
    # True  → created by seed-demo (demo/testing purposes only)
    # False → scraped from a real source (OLX, Imovirtual, Idealista, …)
    is_demo:          Mapped[bool]          = mapped_column(Boolean, default=False, index=True)

    # Which source first discovered this lead (olx / imovirtual / sapo / custojusto / …)
    discovery_source: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    # Which source provided the contact info (may differ from discovery_source)
    contact_source:   Mapped[Optional[str]] = mapped_column(String(50))

    # Confidence in the contact info: 100=phone verified, 70=email, 30=name-only, 0=none
    contact_confidence: Mapped[int]         = mapped_column(Integer, default=0)

    # Owner classification: fsbo / agency / developer / unknown
    # fsbo     = For Sale By Owner (proprietário direto)
    # agency   = mediadora / imobiliária
    # developer= promotor / construtora
    # unknown  = cannot determine from available data
    owner_type:       Mapped[Optional[str]] = mapped_column(String(20), index=True)

    # Lead type (more granular than owner_type)
    # fsbo             = For Sale By Owner
    # frbo             = For Rent By Owner
    # agency_listing   = listed by an agency
    # developer_listing= new development / promotor
    # unknown          = cannot determine
    lead_type:        Mapped[Optional[str]] = mapped_column(String(20), index=True)

    # Quality tier derived from contact completeness + owner type
    # high   = phone/WA present + fsbo/frbo
    # medium = email/website present, OR fsbo without direct contact
    # low    = agency listing with no direct contact
    lead_quality:     Mapped[Optional[str]] = mapped_column(String(10))

    # ── Timestamps ───────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # ── Relationships ────────────────────────────────────────────────────────
    price_history: Mapped[list["PriceHistory"]] = relationship("PriceHistory", back_populates="lead", cascade="all, delete-orphan")
    crm_notes:     Mapped[list["CRMNote"]]      = relationship("CRMNote",      back_populates="lead", cascade="all, delete-orphan")
    alerts:        Mapped[list["Alert"]]        = relationship("Alert",        back_populates="lead", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Lead id={self.id} score={self.score} zone={self.zone} price={self.price}>"


# ─── Price History ────────────────────────────────────────────────────────────

class PriceHistory(Base):
    """One record per price observation — tracks price changes over time."""
    __tablename__ = "price_history"

    id:          Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id:     Mapped[int]           = mapped_column(Integer, ForeignKey("leads.id"), nullable=False, index=True)
    price:       Mapped[float]         = mapped_column(Float, nullable=False)
    source:      Mapped[Optional[str]] = mapped_column(String(50))
    recorded_at: Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)

    lead: Mapped["Lead"] = relationship("Lead", back_populates="price_history")


# ─── CRM Notes ────────────────────────────────────────────────────────────────

class CRMNote(Base):
    """Free-form interaction log for each lead."""
    __tablename__ = "crm_notes"

    id:         Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id:    Mapped[int]           = mapped_column(Integer, ForeignKey("leads.id"), nullable=False, index=True)
    note:       Mapped[str]           = mapped_column(Text, nullable=False)
    note_type:  Mapped[str]           = mapped_column(String(30), default="internal")
    # note_type: 'call' | 'email' | 'visit' | 'whatsapp' | 'internal'
    created_by: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)

    lead: Mapped["Lead"] = relationship("Lead", back_populates="crm_notes")


# ─── Alerts ───────────────────────────────────────────────────────────────────

class Alert(Base):
    """Audit log of all notifications sent."""
    __tablename__ = "alerts"

    id:         Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id:    Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("leads.id"), index=True)
    alert_type: Mapped[str]           = mapped_column(String(50))
    # alert_type: 'hot_lead' | 'price_drop' | 'new_opportunity' | 'daily_report'
    channel:    Mapped[str]           = mapped_column(String(30))
    # channel: 'email' | 'telegram' | 'log'
    sent_at:    Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    payload:    Mapped[Optional[str]] = mapped_column(Text)  # JSON — JSONB in PG
    success:    Mapped[bool]          = mapped_column(Boolean, default=True)
    error_msg:  Mapped[Optional[str]] = mapped_column(Text)

    lead: Mapped[Optional["Lead"]] = relationship("Lead", back_populates="alerts")


# ─── Pre-market Signals ────────────────────────────────────────────────────────

class PremktSignal(Base):
    """
    Pre-market property signal — evidence that an owner may sell before listing.

    Created by premarket.enricher.PremktEnricher.  Never modified by the main
    pipeline.  Promoted to a proper Lead via enricher.promote_to_lead().

    Signal types (descending score):
        building_permit         (85) — CM Lisboa obras permit
        renovation_ad_homeowner (70) — OLX/CustoJusto owner seeking contractor
        contractor_search_post  (65) — Forum/social post seeking works services
        linkedin_city_change    (60) — Confirmed relocation from target zone
        renovation_ad_generic   (55) — Renovation ad, origin ambiguous
        linkedin_job_change     (40) — Career change implying relocation
    """
    __tablename__ = "premarket_signals"

    id:           Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Deduplication — stable 16-char hash of (signal_type, source, signal_text[:120])
    fingerprint:  Mapped[str]           = mapped_column(String(16), unique=True, nullable=False, index=True)

    # Signal classification
    # "building_permit" | "renovation_ad_homeowner" | "renovation_ad_generic"
    # | "linkedin_city_change" | "linkedin_job_change" | "contractor_search_post"
    signal_type:  Mapped[str]           = mapped_column(String(50), nullable=False, index=True)

    # "olx" | "custojusto" | "cm_lisboa" | "duckduckgo_linkedin"
    source:       Mapped[str]           = mapped_column(String(50), nullable=False)

    # Raw title / snippet / permit description that triggered the signal
    signal_text:  Mapped[str]           = mapped_column(Text, nullable=False)

    # Person / company info (best-effort, often null for non-LinkedIn sources)
    name:         Mapped[Optional[str]] = mapped_column(String(200))
    company:      Mapped[Optional[str]] = mapped_column(String(200))
    role:         Mapped[Optional[str]] = mapped_column(String(200))

    # Location
    location_raw: Mapped[Optional[str]] = mapped_column(String(300))
    zone:         Mapped[Optional[str]] = mapped_column(String(100), index=True)

    # Source URL (listing page, LinkedIn profile, permit record URL)
    url:          Mapped[Optional[str]] = mapped_column(Text)

    # Signal strength 0-100 (from premarket.signals.SIGNAL_SCORES)
    signal_score: Mapped[int]           = mapped_column(Integer, default=0, index=True)

    # Always "premarket_owner" — distinguishes these from active listing leads
    lead_type:    Mapped[str]           = mapped_column(String(30), default="premarket_owner")

    # True when manually promoted to a Lead record via PremktEnricher.promote_to_lead()
    promoted:     Mapped[bool]          = mapped_column(Boolean, default=False, index=True)

    # Source-specific metadata (query used, description snippet, permit type, etc.)
    # Use JSONB in PostgreSQL migration
    extra_json:   Mapped[str]           = mapped_column(Text, default="{}")

    # Provenance
    batch_id:     Mapped[Optional[str]] = mapped_column(String(36), index=True)
    created_at:   Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow, index=True)

    def get_extra(self) -> dict:
        import json
        return json.loads(self.extra_json or "{}")

    def __repr__(self) -> str:
        return (
            f"<PremktSignal id={self.id} type={self.signal_type} "
            f"zone={self.zone} score={self.signal_score}>"
        )

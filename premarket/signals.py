"""
Pre-market signal type definitions, scoring weights, and data container.

A "pre-market signal" is any piece of evidence that a property owner is
likely to sell (or has recently decided to sell) before they post a listing
on any of the main portals.

Signal types (in descending score order):
  building_permit         — official obras permit filed with CM Lisboa / other CM
  renovation_ad_homeowner — OLX/CustoJusto ad: homeowner seeking contractor
  renovation_ad_generic   — renovation-related ad, requester ambiguous
  linkedin_city_change    — LinkedIn snippet: person confirmed relocated away from zone
  linkedin_job_change     — LinkedIn snippet: career change, likely relocation
  contractor_search_post  — forum/FB post: homeowner actively seeking works services

All signals are given lead_type = "premarket_owner" so they appear as a
distinct category in the dashboard, separate from active listings.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Optional

# ── Score table ───────────────────────────────────────────────────────────────
# 0-100, higher = stronger pre-sell intent signal

SIGNAL_SCORES: dict[str, int] = {
    "building_permit":           85,
    "renovation_ad_homeowner":   70,
    "renovation_ad_generic":     55,
    "linkedin_city_change":      60,
    "linkedin_job_change":       40,
    "contractor_search_post":    65,
}

SIGNAL_LABELS_PT: dict[str, str] = {
    "building_permit":           "Licenca de Obras",
    "renovation_ad_homeowner":   "Anuncio Remodelacao",
    "renovation_ad_generic":     "Remodelacao Generica",
    "linkedin_city_change":      "Mudanca de Cidade",
    "linkedin_job_change":       "Mudanca Profissional",
    "contractor_search_post":    "Procura Empreiteiro",
}

SIGNAL_ICONS: dict[str, str] = {
    "building_permit":           "🏗️",
    "renovation_ad_homeowner":   "🔨",
    "renovation_ad_generic":     "🔧",
    "linkedin_city_change":      "✈️",
    "linkedin_job_change":       "💼",
    "contractor_search_post":    "📋",
}

LEAD_TYPE = "premarket_owner"


# ── Data container ────────────────────────────────────────────────────────────

@dataclass
class PremktSignalData:
    """
    Intermediate representation of a pre-market signal detected by any source.

    Each source produces a list of these objects.  The enricher deduplicates
    them by fingerprint, then converts surviving records to PremktSignal ORM
    objects before persisting to the database.
    """
    signal_type:  str                          # key from SIGNAL_SCORES
    source:       str                          # "olx" | "custojusto" | "cm_lisboa" | "duckduckgo"
    signal_text:  str                          # raw title or snippet that triggered the signal
    location_raw: Optional[str] = None         # location string as found in the source
    zone:         Optional[str] = None         # normalised zone (Lisboa / Cascais / …)
    name:         Optional[str] = None         # person or company name (if extractable)
    company:      Optional[str] = None         # company / agency name
    role:         Optional[str] = None         # job title / function (LinkedIn signals)
    url:          Optional[str] = None         # source URL
    signal_score: int            = 0           # overridden by __post_init__ from table
    extra:        dict           = field(default_factory=dict)  # source-specific metadata

    def __post_init__(self) -> None:
        if not self.signal_score:
            self.signal_score = SIGNAL_SCORES.get(self.signal_type, 50)

    @property
    def fingerprint(self) -> str:
        """
        Stable 16-char dedup hash.

        Based on signal_type + source + first 120 chars of signal_text.
        Identical ads re-fetched on successive runs produce the same fingerprint
        so they are silently skipped by the enricher.
        """
        raw = f"{self.signal_type}|{self.source}|{self.signal_text[:120]}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @property
    def label_pt(self) -> str:
        return SIGNAL_LABELS_PT.get(self.signal_type, self.signal_type)

    @property
    def icon(self) -> str:
        return SIGNAL_ICONS.get(self.signal_type, "?")

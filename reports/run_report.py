"""
Scrape Run Report — captures before/after DB snapshots and produces a
structured analytics report for each pipeline run.

Usage (in main.py or runner.py):

    from reports.run_report import RunReportCollector

    collector = RunReportCollector()
    collector.snapshot_before()
    # ... run scrape + process + score ...
    collector.snapshot_after(zones=["Lisboa", "Almada"], sources=["imovirtual"])
    report = collector.build()
    print(report.as_text())

The collector makes two cheap SQL COUNT queries (before and after) and
compares them — no heavy joins, no performance impact on the pipeline.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from storage.database import engine
from utils.logger import get_logger

log = get_logger(__name__)


# ── Snapshot ─────────────────────────────────────────────────────────────────

@dataclass
class DBSnapshot:
    """Point-in-time counts from the leads table."""
    captured_at: datetime = field(default_factory=datetime.utcnow)

    total_leads:    int = 0
    phone:          int = 0
    email:          int = 0
    website:        int = 0
    zero_contact:   int = 0
    conf_ge70:      int = 0   # contact_confidence >= 70
    high_quality:   int = 0   # lead_quality = 'high'
    fsbo_leads:     int = 0   # lead_type = 'fsbo'
    frbo_leads:     int = 0   # lead_type = 'frbo'
    active_owners:  int = 0   # lead_type = 'active_owner'
    raw_total:      int = 0

    # per source
    imov_total:     int = 0
    imov_phone:     int = 0
    olx_total:      int = 0
    olx_phone:      int = 0
    idealista_total: int = 0
    idealista_phone: int = 0

    # phones by contact_source tag
    phones_imov_html:       int = 0
    phones_imov_playwright: int = 0
    phones_agency_lookup:   int = 0
    phones_agency_website:  int = 0
    phones_cross_match:     int = 0
    phones_other:           int = 0


def _take_snapshot() -> DBSnapshot:
    """Execute a batch of COUNT queries against the live DB and return a snapshot."""
    from sqlalchemy import text

    snap = DBSnapshot()
    with engine.connect() as conn:
        def q(sql: str) -> int:
            try:
                result = conn.execute(text(sql)).scalar()
                return int(result or 0)
            except Exception:
                return 0

        snap.total_leads  = q("SELECT COUNT(*) FROM leads")
        snap.phone        = q("SELECT COUNT(*) FROM leads WHERE contact_phone IS NOT NULL AND contact_phone != ''")
        snap.email        = q("SELECT COUNT(*) FROM leads WHERE contact_email IS NOT NULL AND contact_email != ''")
        snap.website      = q("SELECT COUNT(*) FROM leads WHERE contact_website IS NOT NULL AND contact_website != ''")
        snap.zero_contact = q("SELECT COUNT(*) FROM leads WHERE contact_confidence = 0")
        snap.conf_ge70    = q("SELECT COUNT(*) FROM leads WHERE contact_confidence >= 70")
        snap.high_quality = q("SELECT COUNT(*) FROM leads WHERE lead_quality = 'high'")
        snap.fsbo_leads    = q("SELECT COUNT(*) FROM leads WHERE lead_type = 'fsbo'")
        snap.frbo_leads    = q("SELECT COUNT(*) FROM leads WHERE lead_type = 'frbo'")
        snap.active_owners = q("SELECT COUNT(*) FROM leads WHERE lead_type = 'active_owner'")
        snap.raw_total     = q("SELECT COUNT(*) FROM raw_listings")

        # per source
        snap.imov_total      = q("SELECT COUNT(*) FROM leads WHERE discovery_source='imovirtual'")
        snap.imov_phone      = q("SELECT COUNT(*) FROM leads WHERE discovery_source='imovirtual' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.olx_total       = q("SELECT COUNT(*) FROM leads WHERE discovery_source='olx'")
        snap.olx_phone       = q("SELECT COUNT(*) FROM leads WHERE discovery_source='olx' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.idealista_total = q("SELECT COUNT(*) FROM leads WHERE discovery_source='idealista'")
        snap.idealista_phone = q("SELECT COUNT(*) FROM leads WHERE discovery_source='idealista' AND contact_phone IS NOT NULL AND contact_phone != ''")

        # phones by contact_source tag
        snap.phones_imov_html       = q("SELECT COUNT(*) FROM leads WHERE contact_source='imov_html' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.phones_imov_playwright = q("SELECT COUNT(*) FROM leads WHERE contact_source='imov_playwright' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.phones_agency_lookup   = q("SELECT COUNT(*) FROM leads WHERE contact_source='agency_lookup' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.phones_agency_website  = q("SELECT COUNT(*) FROM leads WHERE contact_source LIKE 'website:%' AND contact_phone IS NOT NULL AND contact_phone != ''")
        snap.phones_cross_match     = q("SELECT COUNT(*) FROM leads WHERE contact_source='cross_match' AND contact_phone IS NOT NULL AND contact_phone != ''")

    return snap


# ── Report ────────────────────────────────────────────────────────────────────

@dataclass
class RunReport:
    """
    Computed delta between a before and after DB snapshot, enriched with
    per-zone and per-source breakdowns collected during the run.
    """
    # Run metadata
    run_started_at:  datetime = field(default_factory=datetime.utcnow)
    run_finished_at: Optional[datetime] = None
    elapsed_seconds: float = 0.0
    zones:   list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    # Raw listing counts
    listings_scraped: int = 0    # new raw_listings created this run
    listings_total:   int = 0    # total raw_listings after run

    # Lead deltas
    leads_new:     int = 0
    leads_total:   int = 0

    # Phone capture
    phones_before:      int = 0
    phones_after:       int = 0
    phones_gained:      int = 0
    phone_rate_before:  float = 0.0   # %
    phone_rate_after:   float = 0.0   # %
    phone_rate_delta:   float = 0.0   # pp

    # Phones by source
    phones_imov_html:       int = 0
    phones_imov_playwright: int = 0
    phones_agency_lookup:   int = 0
    phones_agency_website:  int = 0
    phones_cross_match:     int = 0

    # FSBO
    fsbo_before:    int = 0
    fsbo_after:     int = 0
    fsbo_gained:    int = 0
    fsbo_rate:      float = 0.0   # % of total leads

    # FRBO
    frbo_before:    int = 0
    frbo_after:     int = 0
    frbo_gained:    int = 0

    # Active Owners
    active_owners_before: int = 0
    active_owners_after:  int = 0
    active_owners_gained: int = 0

    # Contact quality
    conf70_before:  int = 0
    conf70_after:   int = 0
    conf70_gained:  int = 0
    high_q_before:  int = 0
    high_q_after:   int = 0
    high_q_gained:  int = 0
    zero_contact_after: int = 0

    # Per-source phone coverage (after)
    imov_phone_rate:      float = 0.0
    olx_phone_rate:       float = 0.0
    idealista_phone_rate: float = 0.0

    # Per-zone phone breakdown (populated by collector)
    zone_breakdown: dict[str, dict] = field(default_factory=dict)

    # Score distribution (populated after scoring)
    hot_count:  int = 0
    warm_count: int = 0
    cold_count: int = 0

    def as_text(self) -> str:
        """Human-readable report suitable for console / log output."""
        sep  = "─" * 62
        sep2 = "═" * 62
        lines: list[str] = [
            "",
            sep2,
            f"  LEAD ENGINE — SCRAPE RUN REPORT",
            f"  {self.run_started_at.strftime('%Y-%m-%d %H:%M')}  |  "
            f"zones: {', '.join(self.zones)}  |  sources: {', '.join(self.sources)}",
            sep2,
            "",
            f"  SCRAPE",
            sep,
            f"  Listings scraped (new)    : {self.listings_scraped:>6}",
            f"  Raw listings (total)      : {self.listings_total:>6}",
            f"  Leads created (new)       : {self.leads_new:>6}",
            f"  Leads total               : {self.leads_total:>6}",
            f"  Elapsed                   : {self.elapsed_seconds:>6.0f}s  "
            f"({self.elapsed_seconds/60:.1f} min)",
            "",
            f"  PHONE CAPTURE",
            sep,
            f"  Before          : {self.phones_before:>5} / {self.leads_total - self.leads_new:>5}"
            f"  ({self.phone_rate_before:.1f}%)",
            f"  After           : {self.phones_after:>5} / {self.leads_total:>5}"
            f"  ({self.phone_rate_after:.1f}%)",
            f"  Gained this run : {self.phones_gained:>+5}          "
            f"({self.phone_rate_delta:>+.1f}pp)",
            "",
            f"  Phones by source:",
            f"    imov_html       : {self.phones_imov_html:>5}",
            f"    imov_playwright : {self.phones_imov_playwright:>5}",
            f"    agency_lookup   : {self.phones_agency_lookup:>5}",
            f"    agency_website  : {self.phones_agency_website:>5}",
            f"    cross_match     : {self.phones_cross_match:>5}",
        ]

        if self.zone_breakdown:
            lines += [
                "",
                f"  PHONE COVERAGE BY ZONE",
                sep,
            ]
            for zone, stats in sorted(self.zone_breakdown.items(),
                                       key=lambda x: x[1].get("phone_rate", 0), reverse=True):
                total  = stats.get("total", 0)
                phones = stats.get("phones", 0)
                rate   = stats.get("phone_rate", 0.0)
                new    = stats.get("new_leads", 0)
                bar    = "█" * int(rate / 5)
                lines.append(
                    f"  {zone:<12} {total:>4} leads  {phones:>3} phones  "
                    f"{rate:>5.1f}%  {bar}"
                )
                if new:
                    lines[-1] += f"  (+{new} new)"

        lines += [
            "",
            f"  LEAD TYPES & QUALITY",
            sep,
            f"  FSBO leads      : {self.fsbo_after:>5} / {self.leads_total:>5}"
            f"  ({self.fsbo_rate:.1f}%)   [{self.fsbo_gained:>+d} this run]",
            f"  FRBO leads      : {self.frbo_after:>5}            [{self.frbo_gained:>+d} this run]",
            f"  Active owners   : {self.active_owners_after:>5}            [{self.active_owners_gained:>+d} this run]",
            f"  Confidence ≥70  : {self.conf70_after:>5}   [{self.conf70_gained:>+d} this run]",
            f"  Quality = high  : {self.high_q_after:>5}   [{self.high_q_gained:>+d} this run]",
            f"  Zero contact    : {self.zero_contact_after:>5}",
        ]

        if self.hot_count or self.warm_count or self.cold_count:
            from config.settings import settings
            hot_t  = settings.hot_score_threshold
            warm_t = settings.warm_score_threshold
            lines += [
                "",
                f"  SCORE DISTRIBUTION",
                sep,
                f"  HOT  (≥{hot_t}) : {self.hot_count:>5}",
                f"  WARM (≥{warm_t}) : {self.warm_count:>5}",
                f"  COLD (<{warm_t}) : {self.cold_count:>5}",
            ]

        lines += [
            "",
            f"  SOURCE PHONE RATES (after)",
            sep,
            f"  Imovirtual : {self.imov_phone_rate:>5.1f}%",
            f"  OLX        : {self.olx_phone_rate:>5.1f}%",
            f"  Idealista  : {self.idealista_phone_rate:>5.1f}%",
            "",
            sep2,
        ]
        return "\n".join(lines)

    def as_dict(self) -> dict:
        """Return all metrics as a plain dict (for JSON export or dashboard ingestion)."""
        return {
            "run_started_at":         self.run_started_at.isoformat(),
            "run_finished_at":        self.run_finished_at.isoformat() if self.run_finished_at else None,
            "elapsed_seconds":        round(self.elapsed_seconds, 1),
            "zones":                  self.zones,
            "sources":                self.sources,
            "listings_scraped":       self.listings_scraped,
            "listings_total":         self.listings_total,
            "leads_new":              self.leads_new,
            "leads_total":            self.leads_total,
            "phones_before":          self.phones_before,
            "phones_after":           self.phones_after,
            "phones_gained":          self.phones_gained,
            "phone_rate_before":      round(self.phone_rate_before, 2),
            "phone_rate_after":       round(self.phone_rate_after, 2),
            "phone_rate_delta":       round(self.phone_rate_delta, 2),
            "phones_imov_html":       self.phones_imov_html,
            "phones_imov_playwright": self.phones_imov_playwright,
            "phones_agency_lookup":   self.phones_agency_lookup,
            "phones_agency_website":  self.phones_agency_website,
            "phones_cross_match":     self.phones_cross_match,
            "fsbo_leads":             self.fsbo_after,
            "fsbo_rate":              round(self.fsbo_rate, 2),
            "fsbo_gained":            self.fsbo_gained,
            "conf70_after":           self.conf70_after,
            "conf70_gained":          self.conf70_gained,
            "high_quality_after":     self.high_q_after,
            "high_quality_gained":    self.high_q_gained,
            "zero_contact":           self.zero_contact_after,
            "score_hot":              self.hot_count,
            "score_warm":             self.warm_count,
            "score_cold":             self.cold_count,
            "imov_phone_rate":        round(self.imov_phone_rate, 2),
            "olx_phone_rate":         round(self.olx_phone_rate, 2),
            "idealista_phone_rate":   round(self.idealista_phone_rate, 2),
            "zone_breakdown":         self.zone_breakdown,
        }


# ── Collector ─────────────────────────────────────────────────────────────────

class RunReportCollector:
    """
    Collect before/after snapshots around a pipeline run and produce a
    RunReport.

    Typical usage:
        collector = RunReportCollector()
        collector.snapshot_before()
        t0 = time.time()
        # ... run scrape + process + score ...
        collector.snapshot_after(zones=zones, sources=sources)
        report = collector.build(elapsed=time.time() - t0)
        log.info(report.as_text())
    """

    def __init__(self) -> None:
        self._before: Optional[DBSnapshot] = None
        self._after:  Optional[DBSnapshot] = None

    def snapshot_before(self) -> None:
        """Call this immediately before the pipeline run starts."""
        self._before = _take_snapshot()
        log.debug("[RunReport] before snapshot taken — leads={n}", n=self._before.total_leads)

    def snapshot_after(
        self,
        zones:   list[str] | None = None,
        sources: list[str] | None = None,
    ) -> None:
        """Call this immediately after scrape + process + score completes."""
        self._after = _take_snapshot()
        self._zones   = zones   or []
        self._sources = sources or []
        log.debug("[RunReport] after snapshot taken — leads={n}", n=self._after.total_leads)

    def build(self, elapsed: float = 0.0) -> RunReport:
        """
        Compare before and after snapshots and return a populated RunReport.
        Safe to call even if snapshot_before() was never called (uses zeros).
        """
        b = self._before or DBSnapshot()
        a = self._after  or _take_snapshot()

        r = RunReport(
            zones   = getattr(self, "_zones",   []),
            sources = getattr(self, "_sources", []),
            elapsed_seconds  = elapsed,
            run_finished_at  = datetime.utcnow(),
        )

        # Listings
        r.listings_scraped = a.raw_total    - b.raw_total
        r.listings_total   = a.raw_total
        r.leads_new        = a.total_leads  - b.total_leads
        r.leads_total      = a.total_leads

        # Phone capture
        r.phones_before = b.phone
        r.phones_after  = a.phone
        r.phones_gained = a.phone - b.phone
        r.phone_rate_before = (b.phone / b.total_leads * 100) if b.total_leads else 0.0
        r.phone_rate_after  = (a.phone / a.total_leads * 100) if a.total_leads else 0.0
        r.phone_rate_delta  = r.phone_rate_after - r.phone_rate_before

        # Phones by source
        r.phones_imov_html       = a.phones_imov_html
        r.phones_imov_playwright = a.phones_imov_playwright
        r.phones_agency_lookup   = a.phones_agency_lookup
        r.phones_agency_website  = a.phones_agency_website
        r.phones_cross_match     = a.phones_cross_match

        # FSBO
        r.fsbo_before = b.fsbo_leads
        r.fsbo_after  = a.fsbo_leads
        r.fsbo_gained = a.fsbo_leads - b.fsbo_leads
        r.fsbo_rate   = (a.fsbo_leads / a.total_leads * 100) if a.total_leads else 0.0

        # FRBO
        r.frbo_before = b.frbo_leads
        r.frbo_after  = a.frbo_leads
        r.frbo_gained = a.frbo_leads - b.frbo_leads

        # Active Owners
        r.active_owners_before = b.active_owners
        r.active_owners_after  = a.active_owners
        r.active_owners_gained = a.active_owners - b.active_owners

        # Quality
        r.conf70_before    = b.conf_ge70
        r.conf70_after     = a.conf_ge70
        r.conf70_gained    = a.conf_ge70  - b.conf_ge70
        r.high_q_before    = b.high_quality
        r.high_q_after     = a.high_quality
        r.high_q_gained    = a.high_quality - b.high_quality
        r.zero_contact_after = a.zero_contact

        # Per-source phone rates
        r.imov_phone_rate      = (a.imov_phone      / a.imov_total      * 100) if a.imov_total      else 0.0
        r.olx_phone_rate       = (a.olx_phone        / a.olx_total        * 100) if a.olx_total        else 0.0
        r.idealista_phone_rate = (a.idealista_phone  / a.idealista_total  * 100) if a.idealista_total  else 0.0

        # Score distribution (live query — cheap)
        r.hot_count, r.warm_count, r.cold_count = _get_score_distribution()

        # Per-zone breakdown
        r.zone_breakdown = _get_zone_breakdown()

        log.info(
            "[RunReport] run complete — "
            "+{new} leads | +{ph} phones ({rate:.1f}%) | +{fsbo} fsbo | {hot} HOT",
            new=r.leads_new,
            ph=r.phones_gained,
            rate=r.phone_rate_after,
            fsbo=r.fsbo_gained,
            hot=r.hot_count,
        )
        return r


# ── Helper queries ────────────────────────────────────────────────────────────

def _get_score_distribution() -> tuple[int, int, int]:
    """Return (HOT, WARM, COLD) counts from the current leads table."""
    from sqlalchemy import text
    from config.settings import settings

    hot_t  = settings.hot_score_threshold
    warm_t = settings.warm_score_threshold
    try:
        with engine.connect() as conn:
            hot  = conn.execute(text(f"SELECT COUNT(*) FROM leads WHERE score >= {hot_t}")).scalar() or 0
            warm = conn.execute(text(f"SELECT COUNT(*) FROM leads WHERE score >= {warm_t} AND score < {hot_t}")).scalar() or 0
            cold = conn.execute(text(f"SELECT COUNT(*) FROM leads WHERE score IS NULL OR score < {warm_t}")).scalar() or 0
        return int(hot), int(warm), int(cold)
    except Exception:
        return 0, 0, 0


def _get_zone_breakdown() -> dict[str, dict]:
    """Per-zone lead and phone counts from the current leads table."""
    from sqlalchemy import text
    result: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT zone, "
                "COUNT(*) AS total, "
                "SUM(CASE WHEN contact_phone IS NOT NULL AND contact_phone != '' THEN 1 ELSE 0 END) AS phones, "
                "SUM(CASE WHEN lead_type = 'fsbo' THEN 1 ELSE 0 END) AS fsbo "
                "FROM leads WHERE zone IS NOT NULL "
                "GROUP BY zone ORDER BY total DESC"
            )).fetchall()
        for row in rows:
            zone   = row[0]
            total  = int(row[1] or 0)
            phones = int(row[2] or 0)
            fsbo   = int(row[3] or 0)
            result[zone] = {
                "total":      total,
                "phones":     phones,
                "phone_rate": round(phones / total * 100, 1) if total else 0.0,
                "fsbo":       fsbo,
                "fsbo_rate":  round(fsbo  / total * 100, 1) if total else 0.0,
            }
    except Exception as e:
        log.debug("[RunReport] _get_zone_breakdown error: {e}", e=e)
    return result

"""
Pre-market enricher — orchestrates all signal sources and persists results.

Flow
----
1. Run each source (renovation_ads, building_permits, linkedin_search).
2. Collect all PremktSignalData objects into a single list.
3. Deduplicate by fingerprint (same signal re-fetched on successive runs is
   silently skipped — idempotent).
4. Persist new signals to the `premarket_signals` DB table.
5. Return a PremktResult summary.

Design constraints
------------------
- Completely independent of the main pipeline.  No shared state, no imports
  from pipeline/, runner.py or scorer.py.
- Safe to run in parallel with or after the main pipeline — uses its own DB
  session, writes only to `premarket_signals`.
- Each source failure is caught, logged, and skipped — the run never crashes.
- Can be triggered from CLI (`python main.py premarket`), from the scheduler
  (weekly), or from the dashboard "Run" button.

Usage
-----
    enricher = PremktEnricher()
    result   = enricher.run(zones=["Lisboa", "Cascais"])
    print(result)
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from config.settings import settings
from premarket.signals import PremktSignalData, LEAD_TYPE
from utils.logger import get_logger

log = get_logger(__name__)


# ── Result container ──────────────────────────────────────────────────────────

@dataclass
class PremktResult:
    """Summary returned by PremktEnricher.run()."""
    batch_id:       str
    zones:          list[str]
    started_at:     datetime
    finished_at:    Optional[datetime]   = None
    total_found:    int                  = 0    # signals returned by all sources
    new_signals:    int                  = 0    # actually persisted (not duplicates)
    skipped:        int                  = 0    # duplicates (already in DB)
    source_counts:  dict[str, int]       = field(default_factory=dict)
    errors:         list[str]            = field(default_factory=list)

    def __str__(self) -> str:
        elapsed = (
            (self.finished_at - self.started_at).total_seconds()
            if self.finished_at else "—"
        )
        return (
            f"PremktResult batch={self.batch_id[:8]} "
            f"found={self.total_found} new={self.new_signals} "
            f"skip={self.skipped} errors={len(self.errors)} "
            f"elapsed={elapsed:.1f}s"
        )


# ── Enricher ──────────────────────────────────────────────────────────────────

class PremktEnricher:
    """
    Orchestrator for pre-market signal detection.

    Sources registered:
        RenovationAdsSource   — OLX + CustoJusto renovation demand ads
        BuildingPermitsSource — CM Lisboa open data (building permits)
        LinkedInSearchSource  — DuckDuckGo-mediated LinkedIn profile signals

    Each source is optional: if not installed (missing dependency) or if it
    raises during fetch(), it is logged and skipped without failing the run.
    """

    def __init__(self):
        self.batch_id = str(uuid.uuid4())
        self._sources = self._load_sources()

    def _load_sources(self) -> list:
        sources = []
        try:
            from premarket.sources.renovation_ads import RenovationAdsSource
            sources.append(RenovationAdsSource())
        except Exception as e:
            log.warning("[premarket] Could not load RenovationAdsSource: {e}", e=e)

        try:
            from premarket.sources.building_permits import BuildingPermitsSource
            sources.append(BuildingPermitsSource())
        except Exception as e:
            log.warning("[premarket] Could not load BuildingPermitsSource: {e}", e=e)

        try:
            from premarket.sources.linkedin_search import LinkedInSearchSource
            sources.append(LinkedInSearchSource())
        except Exception as e:
            log.warning("[premarket] Could not load LinkedInSearchSource: {e}", e=e)

        return sources

    # ── Public run ────────────────────────────────────────────────────────────

    def run(self, zones: list[str] | None = None) -> PremktResult:
        """
        Execute all sources, deduplicate, persist, and return summary.

        zones: list of zone names (Lisboa, Cascais, …).  Defaults to all
               zones defined in settings.zones.
        """
        zones = zones or settings.zones
        result = PremktResult(
            batch_id   = self.batch_id,
            zones      = zones,
            started_at = datetime.utcnow(),
        )

        log.info(
            "[premarket] Starting enrichment — zones={z} batch={b}",
            z=zones, b=self.batch_id[:8],
        )

        # ── 1. Collect signals from all sources ───────────────────────────────
        all_signals: list[PremktSignalData] = []

        for source in self._sources:
            source_name = type(source).__name__
            try:
                log.info("[premarket] Running source: {s}", s=source_name)
                fetched = source.fetch(zones=zones)
                all_signals.extend(fetched)
                result.source_counts[source_name] = len(fetched)
                log.info(
                    "[premarket] {s} → {n} signals",
                    s=source_name, n=len(fetched),
                )
            except Exception as e:
                msg = f"{source_name} failed: {e}"
                result.errors.append(msg)
                log.error("[premarket] {msg}", msg=msg)
                result.source_counts[source_name] = 0

        result.total_found = len(all_signals)

        # ── 2. Deduplicate by fingerprint ─────────────────────────────────────
        seen: set[str] = set()
        unique: list[PremktSignalData] = []
        for sig in all_signals:
            fp = sig.fingerprint
            if fp not in seen:
                seen.add(fp)
                unique.append(sig)

        log.info(
            "[premarket] After in-memory dedup: {n} unique signals",
            n=len(unique),
        )

        # ── 3. Persist (DB-level dedup via fingerprint unique constraint) ──────
        new, skipped = self._persist(unique, result.batch_id)
        result.new_signals = new
        result.skipped     = skipped

        result.finished_at = datetime.utcnow()
        elapsed = (result.finished_at - result.started_at).total_seconds()

        log.info(
            "[premarket] Done — +{new} new signals, {skip} duplicates skipped, "
            "{err} errors, {t:.1f}s",
            new=new, skip=skipped, err=len(result.errors), t=elapsed,
        )
        return result

    # ── Persistence ───────────────────────────────────────────────────────────

    def _persist(
        self,
        signals: list[PremktSignalData],
        batch_id: str,
    ) -> tuple[int, int]:
        """
        Upsert signals into the `premarket_signals` table.

        Returns (created, skipped) counts.
        Uses fingerprint as the unique key — idempotent on repeated runs.
        """
        from storage.database import get_db
        from storage.models import PremktSignal

        created = 0
        skipped = 0

        with get_db() as db:
            # Load existing fingerprints in a single query (fast path)
            existing_fps: set[str] = {
                row[0]
                for row in db.execute(
                    __import__("sqlalchemy").text(
                        "SELECT fingerprint FROM premarket_signals"
                    )
                )
            }

            for sig in signals:
                if sig.fingerprint in existing_fps:
                    skipped += 1
                    continue

                import json
                orm = PremktSignal(
                    fingerprint  = sig.fingerprint,
                    signal_type  = sig.signal_type,
                    source       = sig.source,
                    signal_text  = sig.signal_text,
                    location_raw = sig.location_raw,
                    zone         = sig.zone,
                    name         = sig.name,
                    company      = sig.company,
                    role         = sig.role,
                    url          = sig.url,
                    signal_score = sig.signal_score,
                    lead_type    = LEAD_TYPE,
                    batch_id     = batch_id,
                    extra_json   = json.dumps(sig.extra, ensure_ascii=False)
                    if sig.extra else "{}",
                )
                db.add(orm)
                existing_fps.add(sig.fingerprint)
                created += 1

        return created, skipped

    # ── Promote to Lead ───────────────────────────────────────────────────────

    def promote_to_lead(self, signal_id: int) -> bool:
        """
        Manually promote a pre-market signal to a proper Lead in the leads table.

        Creates a minimal Lead record with:
          - lead_type = "premarket_owner"
          - zone, contact_name from the signal
          - score = signal_score
          - source = signal.source
          - discovery_source = signal.source

        Returns True if promoted successfully, False if signal not found or
        already promoted.
        """
        from storage.database import get_db
        from storage.models import PremktSignal, Lead
        from storage.repository import LeadRepo
        import hashlib

        with get_db() as db:
            signal = db.get(PremktSignal, signal_id)
            if not signal:
                log.warning("[premarket] promote_to_lead: signal {id} not found", id=signal_id)
                return False
            if signal.promoted:
                log.debug("[premarket] signal {id} already promoted", id=signal_id)
                return False

            # Build a minimal lead fingerprint
            fp_raw = (
                f"premarket|{signal.source}|{signal.signal_text[:80]}|{signal.zone or ''}"
            )
            fingerprint = hashlib.sha256(fp_raw.encode()).hexdigest()[:64]

            existing = db.query(Lead).filter_by(fingerprint=fingerprint).first()
            if not existing:
                lead = Lead(
                    fingerprint       = fingerprint,
                    title             = signal.signal_text[:200],
                    zone              = signal.zone,
                    contact_name      = signal.name,
                    lead_type         = LEAD_TYPE,
                    owner_type        = "unknown",
                    score             = signal.signal_score,
                    score_label       = "WARM" if signal.signal_score >= 50 else "COLD",
                    discovery_source  = signal.source,
                    description       = f"Pre-market signal: {signal.signal_type}. {signal.signal_text}",
                    is_demo           = False,
                )
                db.add(lead)
                log.info(
                    "[premarket] Promoted signal {id} to Lead (zone={z})",
                    id=signal_id, z=signal.zone,
                )

            signal.promoted = True

        return True

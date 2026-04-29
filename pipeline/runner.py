"""
Pipeline Runner — orchestrates the full ETL flow:
  1. Load unprocessed RawListings from DB
  2. Normalize each one
  3. Deduplicate (fingerprint check)
  4. Enrich (price benchmark, urgency, geocoding)
  5. Upsert Lead to DB
  6. Record price history on changes
  7. Mark RawListing as processed

Can be run in two modes:
  - Full run: scrape all sources → save raw → process
  - Process-only: process existing unprocessed raw listings
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipeline.deduplicator import Deduplicator
from pipeline.enricher import Enricher
from pipeline.normalizer import Normalizer
from storage.database import get_db
from storage.repository import LeadRepo, RawListingRepo
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class PipelineStats:
    started_at: datetime = field(default_factory=datetime.utcnow)
    raw_processed: int = 0
    leads_created: int = 0
    leads_updated: int = 0
    leads_skipped: int = 0
    errors: int = 0
    finished_at: Optional[datetime] = None

    def finish(self) -> None:
        self.finished_at = datetime.utcnow()
        elapsed = (self.finished_at - self.started_at).total_seconds()
        log.info(
            "Pipeline complete — {total} raw | +{created} new | ↑{updated} updated | "
            "⚠{skip} skipped | ✗{err} errors | {t:.1f}s",
            total=self.raw_processed,
            created=self.leads_created,
            updated=self.leads_updated,
            skip=self.leads_skipped,
            err=self.errors,
            t=elapsed,
        )


class PipelineRunner:

    def __init__(self):
        self.normalizer = Normalizer()
        self.deduplicator = Deduplicator()
        self.enricher = Enricher()

    # ── Full pipeline: scrape + process ───────────────────────────────────────

    def run_full(self, sources: list[str] = None, zones: list[str] = None) -> PipelineStats:
        """Scrape all configured sources then process everything in the pipeline."""
        import time
        from config.settings import settings
        from reports.run_report import RunReportCollector
        # custojusto excluded — CSR grid requires URL-first strategy (Phase 2)
        # linkedin excluded from auto-run — requires manual login first (run with sources=["linkedin"])
        sources = sources or ["olx", "olx_marketplace", "standvirtual", "imovirtual", "idealista", "sapo", "custojusto"]
        zones = zones or settings.zones

        log.info("=== Full pipeline run — sources: {s}, zones: {z} ===", s=sources, z=zones)

        # Snapshot DB state before run (for run report)
        report_collector = RunReportCollector()
        report_collector.snapshot_before()
        t0 = time.time()

        # Step 1: Scrape
        self._run_scrapers(sources, zones)

        # Step 2: Process raw → leads
        stats = self.process_raw()

        # Step 3: Cross-portal contact enrichment
        # Finds leads missing contact that match a lead on another portal
        # which has a phone/email. Propagates contact fields across the match.
        try:
            cross_stats = self.run_cross_match()
            log.info(
                "Cross-match enrichment — matched={m} phone=+{ph} email=+{em} wa=+{wa}",
                m=cross_stats.get("matched", 0),
                ph=cross_stats.get("phone", 0),
                em=cross_stats.get("email", 0),
                wa=cross_stats.get("whatsapp", 0),
            )
        except Exception as e:
            log.error("Cross-match step failed (non-fatal): {e}", e=e)

        # Step 4: Website enrichment for agency leads
        try:
            web_stats = self.run_website_enrichment()
            log.info(
                "Website enrichment — agencies_ok={a} phone=+{ph} email=+{em} website=+{web}",
                a=web_stats.get("agencies_ok", 0),
                ph=web_stats.get("phone", 0),
                em=web_stats.get("email", 0),
                web=web_stats.get("website", 0),
            )
        except Exception as e:
            log.error("Website enrichment step failed (non-fatal): {e}", e=e)

        # Step 5: Build and emit run report
        try:
            report_collector.snapshot_after(zones=zones, sources=sources)
            run_report = report_collector.build(elapsed=time.time() - t0)
            log.info(run_report.as_text())
            stats.run_report = run_report   # attach to stats for callers
        except Exception as e:
            log.error("Run report generation failed (non-fatal): {e}", e=e)

        return stats

    def _run_scrapers(self, sources: list[str], zones: list[str]) -> None:
        """Launch each configured scraper and persist results as RawListings."""
        from scrapers import (
            OLXScraper,
            OLXMarketplaceScraper,
            StandvirtualScraper,
            LinkedInScraper,
            ImovirtualScraper,
            IdealistaScraper,
            SapoScraper,
            CustojustoScraper,
        )

        scraper_map = {
            "olx":             OLXScraper,
            "olx_marketplace": OLXMarketplaceScraper,
            "standvirtual":    StandvirtualScraper,
            "linkedin":        LinkedInScraper,
            "imovirtual":      ImovirtualScraper,
            "idealista":       IdealistaScraper,
            "sapo":            SapoScraper,
            "custojusto":      CustojustoScraper,
        }

        for source in sources:
            cls = scraper_map.get(source)
            if not cls:
                log.warning("No scraper registered for source: {s}", s=source)
                continue

            log.info("--- Scraping: {s} ---", s=source)
            try:
                scraper = cls()

                # Prime the delta-crawl cache. Scrapers use this set to stop
                # paginating once a page is mostly-seen (BaseScraper.DELTA_STOP_RATIO).
                # A cold DB (empty set) is a full first-run; subsequent runs
                # converge to only fetching first-1-to-2 pages of new listings.
                try:
                    with get_db() as db:
                        scraper.known_external_ids = RawListingRepo(db).get_external_ids(source)
                    log.info(
                        "[{s}] delta cache primed — {n} known external_ids",
                        s=source, n=len(scraper.known_external_ids),
                    )
                except Exception as e:
                    log.warning("[{s}] delta prime failed (non-fatal): {e}", s=source, e=e)

                result = scraper.run(zones=zones)
                self._persist_raw(result.items, source, result.batch_id)
            except Exception as e:
                log.error("Scraper failed for {s}: {e}", s=source, e=e)

    def _persist_raw(self, items: list[dict], source: str, batch_id: str) -> int:
        """Save scraped items as RawListings. Returns count of new records saved."""
        saved = 0
        with get_db() as db:
            repo = RawListingRepo(db)
            for item in items:
                url = item.get("url", "")
                external_id = item.get("external_id")

                # Skip if already scraped (by external_id)
                if external_id and repo.exists(source, external_id):
                    continue

                repo.create(
                    source=source,
                    url=url,
                    raw_data=item,
                    external_id=external_id,
                    batch_id=batch_id,
                )
                saved += 1

        log.info("Persisted {n} new raw listings from {s}", n=saved, s=source)
        return saved

    # ── Process-only pipeline ─────────────────────────────────────────────────

    def process_raw(self, source: str = None, limit: int = 1000) -> PipelineStats:
        """Process all unprocessed RawListings through normalize → dedupe → enrich → upsert."""
        stats = PipelineStats()
        log.info("Processing raw listings (source={s}, limit={l})", s=source or "all", l=limit)

        with get_db() as db:
            raw_repo = RawListingRepo(db)
            lead_repo = LeadRepo(db)

            raw_listings = raw_repo.get_unprocessed(source=source, limit=limit)
            log.info("Found {n} unprocessed raw listings", n=len(raw_listings))

            for raw in raw_listings:
                try:
                    self._process_one(raw, lead_repo, raw_repo, stats)
                except Exception as e:
                    log.error("Error processing raw_listing id={id}: {e}", id=raw.id, e=e)
                    stats.errors += 1

        stats.finish()
        return stats

    def _process_one(self, raw, lead_repo: LeadRepo, raw_repo: RawListingRepo, stats: PipelineStats) -> None:
        """Process a single RawListing through the full pipeline."""
        raw_data = raw.get_data()

        # 1. Normalize
        normalised = self.normalizer.normalize(raw.source, raw_data)
        if not normalised or not normalised.get("url"):
            log.debug("Skipping raw id={id} — normalisation failed", id=raw.id)
            raw_repo.mark_processed(raw.id)
            stats.leads_skipped += 1
            return

        # 2. Compute fingerprint
        fp = self.deduplicator.compute_fingerprint(normalised)

        # 3. Enrich
        existing = lead_repo.get_by_fingerprint(fp)
        first_seen = existing.first_seen_at if existing else None
        enriched = self.enricher.enrich(normalised, first_seen_at=first_seen)

        # 4. Upsert
        if existing:
            update_payload = self.deduplicator.build_update_payload(existing, enriched)
            lead_repo.update(existing, update_payload)

            # Track price change in history
            new_price = enriched.get("price")
            if new_price and self.deduplicator.detect_price_change(existing.price, new_price):
                lead_repo.record_price(existing.id, new_price, raw.source)

            # Merge sources
            new_sources = self.deduplicator.merge_sources(
                existing.sources,
                enriched.get("source", raw.source),
                enriched.get("url", ""),
            )
            existing.sources = new_sources

            stats.leads_updated += 1

        else:
            # Build Lead creation dict
            lead_data = self._build_lead_data(enriched, fp, raw.source)
            new_lead = lead_repo.create(lead_data)

            # Record initial price
            if new_lead.price:
                lead_repo.record_price(new_lead.id, new_lead.price, raw.source)

            stats.leads_created += 1

        # 5. Mark raw as processed
        raw_repo.mark_processed(raw.id)
        stats.raw_processed += 1

    # ── Cross-portal contact discovery ───────────────────────────────────────

    def run_cross_match(self) -> dict:
        """
        Run cross-portal contact discovery over all active leads.

        Finds leads missing phone/email/whatsapp that match a lead from a
        different portal (by zone + price + typology + area + title similarity)
        and copies the contact fields across.

        Safe to run multiple times — only fills NULL fields, never overwrites.

        Returns stats dict:
            checked, matched, phone, email, whatsapp, name, website, skipped
        """
        from pipeline.cross_matcher import CrossMatcher
        with get_db() as db:
            return CrossMatcher().run(db)

    def run_website_enrichment(self) -> dict:
        """
        Visit agency homepages to backfill missing phone/email on agency leads.

        Targets leads where:
          • owner_type = 'agency'
          • agency_name is set
          • contact_phone AND contact_email are both missing

        Skips major franchise chains (RE/MAX, KW, Century21, etc.) whose
        homepages carry brand content rather than per-agent contacts.

        Safe to run multiple times — only fills NULL/empty fields.

        Returns stats dict:
            candidates, sites_tried, sites_ok, agencies_ok,
            phone, email, website, skipped
        """
        from pipeline.website_enricher import WebsiteEnricher
        with get_db() as db:
            return WebsiteEnricher().run(db)

    # ── Contact backfill ──────────────────────────────────────────────────────

    def backfill_contact_source(self) -> dict:
        """
        Re-examine all leads that are missing any contact channel field.

        Scope: leads where contact_source IS NULL OR contact_whatsapp IS NULL
               OR contact_website IS NULL — so each re-run only touches leads
               that still have something missing (idempotent).

        For each lead, attempt extraction from the stored description:
          Phone/WA found  → contact_source = portal_source, confidence = 100
          Email found     → contact_source = portal_source, confidence = 70
          Website found   → contact_source = portal_source, confidence = 40
          Name only       → contact_source = portal_source, confidence = 30
          Nothing         → leave unchanged

        Returns counts: checked / updated_source / updated_whatsapp /
                        updated_website / skipped.
        """
        import json as _json
        from pipeline.normalizer import (
            extract_phone_from_text,
            extract_email_from_text,
            extract_whatsapp_from_text,
            extract_website_from_text,
        )
        from sqlalchemy import select, or_
        from storage.models import Lead

        counts = {
            "checked": 0,
            "updated_source": 0,
            "updated_whatsapp": 0,
            "updated_website": 0,
            "skipped": 0,
        }

        def _infer_source(lead) -> str:
            """Get best available source name for a lead."""
            if lead.discovery_source:
                return lead.discovery_source
            try:
                srcs = _json.loads(lead.sources_json or "[]")
                if srcs and srcs[0].get("source"):
                    return srcs[0]["source"]
            except Exception:
                pass
            return "listagem"

        with get_db() as db:
            # Cover leads missing any of the three new/updated fields
            leads = db.execute(
                select(Lead).where(
                    or_(
                        Lead.contact_source.is_(None),
                        Lead.contact_whatsapp.is_(None),
                        Lead.contact_website.is_(None),
                    )
                )
            ).scalars().all()

            counts["checked"] = len(leads)

            for lead in leads:
                description = lead.description or ""
                title       = lead.title or ""
                source      = _infer_source(lead)
                changed     = False

                # ── WhatsApp (new channel — always try if still NULL) ───────
                if lead.contact_whatsapp is None:
                    found_wa = (
                        extract_whatsapp_from_text(description) or
                        extract_whatsapp_from_text(title)
                    )
                    if found_wa:
                        lead.contact_whatsapp = found_wa
                        counts["updated_whatsapp"] += 1
                        changed = True

                # ── Website (new channel — always try if still NULL) ────────
                if lead.contact_website is None:
                    found_site = extract_website_from_text(description)
                    if found_site:
                        lead.contact_website = found_site
                        counts["updated_website"] += 1
                        changed = True

                # ── contact_source / confidence (only if still NULL) ────────
                if lead.contact_source is None:
                    src_changed = False

                    if lead.contact_phone or lead.contact_whatsapp:
                        lead.contact_source = source
                        lead.contact_confidence = 100
                        src_changed = True
                    else:
                        # Try extracting phone
                        found_phone = (
                            extract_phone_from_text(description) or
                            extract_phone_from_text(title)
                        )
                        if found_phone:
                            lead.contact_phone = found_phone
                            lead.contact_source = source
                            lead.contact_confidence = 100
                            src_changed = True

                    if not src_changed:
                        if lead.contact_email:
                            lead.contact_source = source
                            lead.contact_confidence = 70
                            src_changed = True
                        else:
                            found_email = extract_email_from_text(description)
                            if found_email:
                                lead.contact_email = found_email
                                lead.contact_source = source
                                lead.contact_confidence = 70
                                src_changed = True

                    if not src_changed and lead.contact_website:
                        lead.contact_source = source
                        lead.contact_confidence = 40
                        src_changed = True

                    if not src_changed and lead.contact_name:
                        lead.contact_source = source
                        lead.contact_confidence = 30
                        src_changed = True

                    if src_changed:
                        counts["updated_source"] += 1
                        changed = True

                if not changed:
                    counts["skipped"] += 1

            db.commit()

        log.info(
            "backfill_contact_source: checked={c} | source={s} | wa={w} | web={wb} | skipped={sk}",
            c=counts["checked"],
            s=counts["updated_source"],
            w=counts["updated_whatsapp"],
            wb=counts["updated_website"],
            sk=counts["skipped"],
        )
        return counts

    def backfill_agency_contacts(self) -> dict:
        """
        Apply the agency website lookup table to ALL existing leads that have
        an agency_name but still lack a contact_website.

        This is a fast in-DB backfill — no HTTP requests.  Runs entirely in
        Python because the lookup is regex-based.

        Safe to run multiple times — only updates leads with no website yet.

        Returns:
            checked / matched / updated_website / updated_quality counts
        """
        from pipeline.enricher import Enricher
        from sqlalchemy import select
        from storage.models import Lead

        counts = {"checked": 0, "matched": 0, "updated_website": 0, "updated_quality": 0}

        with get_db() as db:
            # Targets: have agency_name but no website and no phone
            leads = db.execute(
                select(Lead).where(
                    Lead.agency_name.isnot(None),
                    Lead.agency_name != "",
                    (Lead.contact_website.is_(None)) | (Lead.contact_website == ""),
                    (Lead.contact_phone.is_(None))   | (Lead.contact_phone   == ""),
                )
            ).scalars().all()

            log.info("[backfill_agency] {n} leads to check", n=len(leads))

            for lead in leads:
                counts["checked"] += 1
                result = Enricher._lookup_agency_contact(
                    lead.agency_name or "",
                    lead.contact_name or "",
                )
                if not result:
                    continue

                counts["matched"] += 1
                ws = result.get("website") or ""
                if ws:
                    lead.contact_website = ws
                    counts["updated_website"] += 1
                    if not lead.contact_source:
                        lead.contact_source = "agency_lookup"

                    # Recompute confidence and quality in-place
                    # Hierarchy (same as enricher): phone=100, email=70, website=40, name=30
                    has_phone = bool((lead.contact_phone or "").strip())
                    has_wa    = bool((lead.contact_whatsapp or "").strip())
                    has_email = bool((lead.contact_email or "").strip())
                    if has_phone or has_wa:
                        lead.contact_confidence = 100
                    elif has_email:
                        lead.contact_confidence = 70
                    elif ws:
                        lead.contact_confidence = 40
                    elif lead.contact_name:
                        lead.contact_confidence = 30
                    else:
                        lead.contact_confidence = 0

                    # Upgrade lead_quality from 'low' to 'medium' when website gained
                    if lead.lead_quality == "low" and ws:
                        lead.lead_quality = "medium"
                        counts["updated_quality"] += 1

            db.commit()

        log.info(
            "[backfill_agency] Done — checked={c} matched={m} website=+{w} quality=+{q}",
            c=counts["checked"], m=counts["matched"],
            w=counts["updated_website"], q=counts["updated_quality"],
        )
        return counts

    def _build_lead_data(self, enriched: dict, fp: str, source: str) -> dict:
        """Build the dict for Lead.create() from enriched normalised data."""
        import json

        discovery_source = enriched.get("source", source)
        sources_list = [{
            "source": discovery_source,
            "url": enriched.get("url", ""),
            "seen_at": datetime.utcnow().isoformat(),
        }]

        return {
            "fingerprint": fp,
            "is_demo": False,           # all scraped leads are real data
            "discovery_source": discovery_source,
            "contact_source": enriched.get("contact_source"),
            "contact_confidence": enriched.get("contact_confidence", 0),
            "owner_type": enriched.get("owner_type", "unknown"),
            "lead_type": enriched.get("lead_type", "unknown"),
            "lead_quality": enriched.get("lead_quality"),
            "title": enriched.get("title"),
            "typology":      enriched.get("typology"),
            "property_type": enriched.get("property_type"),
            "area_m2":       enriched.get("area_m2"),
            "price":         enriched.get("price"),
            "product_title": enriched.get("product_title"),
            "product_value": enriched.get("product_value"),
            "price_per_m2": enriched.get("price_per_m2"),
            "price_benchmark": enriched.get("price_benchmark"),
            "price_delta_pct": enriched.get("price_delta_pct"),
            "condition": enriched.get("condition"),
            "description": enriched.get("description"),
            "zone":         enriched.get("zone"),
            "municipality": enriched.get("municipality"),
            "parish":       enriched.get("parish"),
            "address": enriched.get("address"),
            "latitude": enriched.get("latitude"),
            "longitude": enriched.get("longitude"),
            "contact_name":     enriched.get("contact_name"),
            "first_name":       enriched.get("first_name"),
            "last_name":        enriched.get("last_name"),
            "birthday":         enriched.get("birthday"),
            "contact_phone":    enriched.get("contact_phone"),
            "contact_email":    enriched.get("contact_email"),
            "contact_whatsapp": enriched.get("contact_whatsapp"),
            "contact_website":  enriched.get("contact_website"),
            "is_owner": enriched.get("is_owner", False),
            "agency_name": enriched.get("agency_name"),
            "sources_json": json.dumps(sources_list, default=str),
            "days_on_market": enriched.get("days_on_market", 0),
            "first_seen_at": datetime.utcnow(),
            "last_seen_at": datetime.utcnow(),
            "crm_stage": "novo",
        }

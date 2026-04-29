"""
Scheduler — automated pipeline execution.

Phase 1: Uses APScheduler (no Redis needed, runs in-process).
Phase 2: Replace with Celery + Redis Beat for distributed execution.

Daily schedule (configurable via SCHEDULE_TIME in .env):
  08:00 — Scraping (all sources in parallel threads)
  08:30 — Pipeline processing (normalize → dedupe → enrich)
  09:00 — Scoring all leads
  09:05 — Hot lead alerts
  09:10 — Daily report

Weekly schedule (every Sunday):
  07:00 — Pre-market signal scan (renovation ads, building permits, LinkedIn)
"""
from __future__ import annotations

import threading
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import settings
from utils.logger import get_logger

log = get_logger(__name__)


class Scheduler:

    def __init__(self):
        self._scheduler = BackgroundScheduler(
            job_defaults={
                "coalesce": True,      # if job missed, run once not multiple times
                "max_instances": 1,    # only one instance of each job at a time
                "misfire_grace_time": 3600,  # allow up to 1h late start
            },
            timezone="Europe/Lisbon",
        )
        self._running = False

    def start(self, run_now: bool = False) -> None:
        """Start the scheduler. If run_now=True, trigger a full run immediately."""
        if not settings.schedule_enabled:
            log.warning("Scheduler disabled via SCHEDULE_ENABLED=false")
            return

        hour, minute = settings.schedule_time.split(":")

        # Daily full pipeline
        self._scheduler.add_job(
            func=self._job_full_pipeline,
            trigger=CronTrigger(hour=int(hour), minute=int(minute)),
            id="full_pipeline",
            name="Full Pipeline (scrape + process + score)",
            replace_existing=True,
        )

        # Hot lead check every 2 hours
        self._scheduler.add_job(
            func=self._job_hot_lead_check,
            trigger=IntervalTrigger(hours=2),
            id="hot_lead_check",
            name="Hot Lead Alert Check",
            replace_existing=True,
        )

        # Weekly pre-market signal scan — Sundays at 07:00
        self._scheduler.add_job(
            func=self._job_premarket_scan,
            trigger=CronTrigger(day_of_week="sun", hour=7, minute=0),
            id="premarket_scan",
            name="Pre-Market Signal Scan (weekly)",
            replace_existing=True,
        )

        self._scheduler.start()
        self._running = True
        log.info(
            "Scheduler started — daily run at {time} (Europe/Lisbon)",
            time=settings.schedule_time,
        )

        if run_now:
            log.info("run_now=True — triggering immediate pipeline run")
            t = threading.Thread(target=self._job_full_pipeline, daemon=True)
            t.start()

    def stop(self) -> None:
        if self._running:
            self._scheduler.shutdown(wait=False)
            self._running = False
            log.info("Scheduler stopped")

    def run_now(self, step: str = "full") -> None:
        """Manually trigger a specific pipeline step."""
        steps = {
            "full":      self._job_full_pipeline,
            "scrape":    self._job_scrape_only,
            "process":   self._job_process_only,
            "score":     self._job_score_only,
            "alerts":    self._job_hot_lead_check,
            "report":    self._job_daily_report,
            "premarket": self._job_premarket_scan,
        }
        fn = steps.get(step)
        if fn:
            log.info("Manual trigger: {step}", step=step)
            t = threading.Thread(target=fn, daemon=True)
            t.start()
        else:
            log.error("Unknown step: {step}. Options: {opts}", step=step, opts=list(steps.keys()))

    # ── Job implementations ───────────────────────────────────────────────────

    def _job_full_pipeline(self) -> None:
        log.info("=== JOB: Full Pipeline ===")
        try:
            self._job_scrape_only()
            self._job_process_only()
            self._job_score_only()
            self._job_hot_lead_check()
            self._job_daily_report()
        except Exception as e:
            log.error("Full pipeline job failed: {e}", e=e)

    def _job_scrape_only(self) -> None:
        log.info("--- JOB: Scraping ---")
        try:
            from pipeline.runner import PipelineRunner
            runner = PipelineRunner()
            runner._run_scrapers(
                sources=["olx", "imovirtual", "idealista"],
                zones=settings.zones,
            )
        except Exception as e:
            log.error("Scrape job failed: {e}", e=e)

    def _job_process_only(self) -> None:
        log.info("--- JOB: Processing raw listings ---")
        try:
            from pipeline.runner import PipelineRunner
            runner = PipelineRunner()
            stats = runner.process_raw()
            log.info(
                "Pipeline stats: +{c} created, ↑{u} updated",
                c=stats.leads_created,
                u=stats.leads_updated,
            )
        except Exception as e:
            log.error("Process job failed: {e}", e=e)

    def _job_score_only(self) -> None:
        log.info("--- JOB: Scoring leads ---")
        try:
            from scoring.scorer import Scorer
            scorer = Scorer()
            n = scorer.score_all_pending()
            log.info("Scored {n} leads", n=n)
        except Exception as e:
            log.error("Score job failed: {e}", e=e)

    def _job_hot_lead_check(self) -> None:
        log.info("--- JOB: Hot lead alerts ---")
        try:
            from alerts.notifier import Notifier
            notifier = Notifier()
            n = notifier.check_and_alert_hot_leads()
            log.info("Hot lead alerts sent: {n}", n=n)
        except Exception as e:
            log.error("Alert job failed: {e}", e=e)

    def _job_daily_report(self) -> None:
        log.info("--- JOB: Daily report ---")
        try:
            from reports.generator import ReportGenerator
            from alerts.notifier import Notifier
            gen = ReportGenerator()
            notifier = Notifier()
            leads = gen.daily_report_leads(top_n=20)
            notifier.send_daily_report(leads)
        except Exception as e:
            log.error("Daily report job failed: {e}", e=e)

    def _job_premarket_scan(self) -> None:
        log.info("--- JOB: Pre-Market Signal Scan ---")
        try:
            from premarket.enricher import PremktEnricher
            result = PremktEnricher().run()
            log.info(
                "Pre-market scan complete: +{new} new signals, {skip} duplicates",
                new=result.new_signals,
                skip=result.skipped,
            )
        except Exception as e:
            log.error("Pre-market scan job failed: {e}", e=e)

    @property
    def is_running(self) -> bool:
        return self._running

    def get_jobs(self) -> list[dict]:
        """Return info about scheduled jobs — for dashboard display."""
        if not self._running:
            return []
        jobs = []
        for job in self._scheduler.get_jobs():
            next_run = job.next_run_time
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": next_run.isoformat() if next_run else "—",
            })
        return jobs

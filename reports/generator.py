"""
Report Generator — exports data in CSV/JSON formats and generates summary stats.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from config.settings import settings
from storage.database import get_db
from storage.models import Lead
from storage.repository import LeadRepo
from utils.logger import get_logger

log = get_logger(__name__)


class ReportGenerator:

    def export_csv(
        self,
        output_path: str = None,
        score_min: int = 0,
        days: int = None,
    ) -> str:
        """
        Export leads to CSV.
        Returns the path of the generated file.
        """
        if not output_path:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(settings.data_dir / f"leads_export_{ts}.csv")

        with get_db() as db:
            repo = LeadRepo(db)
            leads = repo.list_active(score_min=score_min, limit=5000)

            if days:
                cutoff = datetime.utcnow() - timedelta(days=days)
                leads = [l for l in leads if l.created_at and l.created_at >= cutoff]

        fields = [
            "id", "score", "score_label", "title", "typology", "zone",
            "price", "area_m2", "price_per_m2", "price_delta_pct",
            "is_owner", "contact_name", "contact_phone", "contact_email",
            "agency_name", "condition", "days_on_market", "price_changes",
            "crm_stage", "address", "description", "first_seen_at", "last_seen_at",
        ]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for lead in leads:
                row = {field: getattr(lead, field, None) for field in fields}
                # Clean up datetimes
                for dt_field in ["first_seen_at", "last_seen_at"]:
                    if row[dt_field]:
                        row[dt_field] = row[dt_field].isoformat()
                writer.writerow(row)

        log.info("CSV exported: {path} ({n} leads)", path=output_path, n=len(leads))
        return output_path

    def export_json(self, score_min: int = 50) -> str:
        """Export top leads to JSON. Returns file path."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(settings.data_dir / f"leads_{ts}.json")

        with get_db() as db:
            repo = LeadRepo(db)
            leads = repo.list_active(score_min=score_min, limit=500)

        data = []
        for lead in leads:
            data.append({
                "id": lead.id,
                "score": lead.score,
                "label": lead.score_label,
                "title": lead.title,
                "typology": lead.typology,
                "zone": lead.zone,
                "price": lead.price,
                "area_m2": lead.area_m2,
                "price_delta_pct": lead.price_delta_pct,
                "is_owner": lead.is_owner,
                "contact_phone": lead.contact_phone,
                "crm_stage": lead.crm_stage,
                "sources": lead.sources,
                "days_on_market": lead.days_on_market,
            })

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

        log.info("JSON exported: {path}", path=output_path)
        return output_path

    def get_summary_stats(self) -> dict:
        """Return a summary dict for dashboard KPIs."""
        with get_db() as db:
            repo = LeadRepo(db)
            total      = repo.total_active()
            today      = repo.added_today()
            by_label   = repo.count_by_label()
            by_zone    = repo.count_by_zone()
            by_stage   = repo.count_by_stage()

            from sqlalchemy import func
            avg_score = db.query(func.avg(Lead.score)).filter(Lead.archived == False).scalar()

            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            hot_today = (
                db.query(Lead)
                .filter(
                    Lead.score_label == "HOT",
                    Lead.archived == False,
                    Lead.scored_at >= today_start,
                )
                .count()
            )

            # ── Enriched intelligence stats ───────────────────────────────────
            owner_count       = db.query(Lead).filter(Lead.archived == False, Lead.is_owner == True).count()
            price_drop_count  = db.query(Lead).filter(Lead.archived == False, Lead.price_changes > 0).count()
            high_delta_count  = db.query(Lead).filter(Lead.archived == False, Lead.price_delta_pct >= 15).count()

            # Leads por fonte (sources_json is text — simple LIKE match)
            olx_count         = db.query(Lead).filter(Lead.archived == False, Lead.sources_json.like('%"olx"%')).count()
            imov_count        = db.query(Lead).filter(Lead.archived == False, Lead.sources_json.like('%"imovirtual"%')).count()
            ideal_count       = db.query(Lead).filter(Lead.archived == False, Lead.sources_json.like('%"idealista"%')).count()

            # Urgency keywords in descriptions (hot signals)
            urgency_kw = ['urgente', 'herança', 'herdeiro', 'divórcio', 'emigr', 'partilha']
            urgency_count = 0
            for kw in urgency_kw:
                urgency_count += db.query(Lead).filter(
                    Lead.archived == False,
                    Lead.description.ilike(f'%{kw}%')
                ).count()
            # Deduplicate rough estimate — cap at total
            urgency_count = min(urgency_count, total)

            # Days on market distribution
            dom_gt30  = db.query(Lead).filter(Lead.archived == False, Lead.days_on_market >= 30).count()
            dom_gt60  = db.query(Lead).filter(Lead.archived == False, Lead.days_on_market >= 60).count()

            # ── Contact availability counts (DB-level, unfiltered) ─────────────
            with_phone_count = db.query(Lead).filter(
                Lead.archived == False,
                Lead.contact_phone.isnot(None),
            ).count()
            with_email_count = db.query(Lead).filter(
                Lead.archived == False,
                Lead.contact_email.isnot(None),
            ).count()
            no_contact_count = db.query(Lead).filter(
                Lead.archived == False,
                Lead.contact_phone.is_(None),
                Lead.contact_email.is_(None),
            ).count()

        return {
            "total_active":    total,
            "added_today":     today,
            "hot_count":       by_label.get("HOT",  0),
            "warm_count":      by_label.get("WARM", 0),
            "cold_count":      by_label.get("COLD", 0),
            "hot_today":       hot_today,
            "avg_score":       round(avg_score or 0, 1),
            "by_zone":         by_zone,
            "by_stage":        by_stage,
            # Intelligence
            "owner_count":     owner_count,
            "price_drop_count":price_drop_count,
            "high_delta_count":high_delta_count,
            "urgency_count":   urgency_count,
            "dom_gt30":        dom_gt30,
            "dom_gt60":        dom_gt60,
            # Contact availability
            "with_phone_count": with_phone_count,
            "with_email_count": with_email_count,
            "no_contact_count": no_contact_count,
            # By source
            "by_source": {"olx": olx_count, "imovirtual": imov_count, "idealista": ideal_count},
            "generated_at":    datetime.utcnow().isoformat(),
        }

    def daily_report_leads(self, top_n: int = 20) -> List[Lead]:
        """Return the top N leads for the daily report."""
        with get_db() as db:
            repo = LeadRepo(db)
            return repo.list_active(score_min=0, limit=top_n)

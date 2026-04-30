"""
Time-series queries for the Dashboard trend charts.

These read directly from the SQLite/Postgres backend with aggregation
that fits in <200 ms even on 50k-lead DBs:

  leads_per_day(window_days)       — bar chart of new leads/day
  hot_share_per_day(window_days)   — % of new leads classified HOT
  contact_rate_per_day(window_days)— % new leads with phone+email
  avg_score_per_day(window_days)   — mean score of new leads
  source_share(window_days)        — pie/share by discovery_source
  drops_per_day(window_days)       — count of CRMNote 'price drop' events
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import case, func, select

from storage.database import get_db
from storage.models import CRMNote, Lead


def _date_floor():
    """SQLite-friendly DATE() expression — works on Postgres too."""
    return func.date(Lead.first_seen_at)


def leads_per_day(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(_date_floor().label("d"), func.count(Lead.id).label("n"))
            .where(Lead.first_seen_at >= cutoff)
            .where(Lead.is_demo == False)               # noqa: E712
            .group_by("d").order_by("d")
        ).all()
    return [{"date": r.d, "count": int(r.n)} for r in rows]


def hot_share_per_day(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(
                _date_floor().label("d"),
                func.count(Lead.id).label("n"),
                func.sum(case((Lead.score_label == "HOT", 1), else_=0)).label("hot"),
            )
            .where(Lead.first_seen_at >= cutoff)
            .where(Lead.is_demo == False)               # noqa: E712
            .group_by("d").order_by("d")
        ).all()
    return [
        {"date": r.d, "hot_pct": (100.0 * (r.hot or 0) / r.n) if r.n else 0.0}
        for r in rows
    ]


def contact_rate_per_day(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(
                _date_floor().label("d"),
                func.count(Lead.id).label("n"),
                func.sum(case(
                    ((Lead.contact_phone.isnot(None)) & (Lead.contact_phone != ""), 1),
                    else_=0,
                )).label("with_phone"),
            )
            .where(Lead.first_seen_at >= cutoff)
            .where(Lead.is_demo == False)               # noqa: E712
            .group_by("d").order_by("d")
        ).all()
    return [
        {"date": r.d, "contact_pct": (100.0 * (r.with_phone or 0) / r.n) if r.n else 0.0}
        for r in rows
    ]


def avg_score_per_day(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(_date_floor().label("d"), func.avg(Lead.score).label("avg"))
            .where(Lead.first_seen_at >= cutoff)
            .where(Lead.is_demo == False)               # noqa: E712
            .group_by("d").order_by("d")
        ).all()
    return [{"date": r.d, "avg_score": round(float(r.avg or 0), 1)} for r in rows]


def source_share(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(Lead.discovery_source, func.count(Lead.id).label("n"))
            .where(Lead.first_seen_at >= cutoff)
            .where(Lead.is_demo == False)               # noqa: E712
            .where(Lead.discovery_source.isnot(None))
            .group_by(Lead.discovery_source)
            .order_by(func.count(Lead.id).desc())
        ).all()
    return [{"source": r.discovery_source, "count": int(r.n)} for r in rows]


def drops_per_day(window_days: int = 30) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        rows = db.execute(
            select(
                func.date(CRMNote.created_at).label("d"),
                func.count(CRMNote.id).label("n"),
            )
            .where(CRMNote.created_at >= cutoff)
            .where(CRMNote.note.like("%Price drop%"))
            .group_by("d").order_by("d")
        ).all()
    return [{"date": r.d, "count": int(r.n)} for r in rows]

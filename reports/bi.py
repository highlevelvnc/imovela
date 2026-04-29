"""
Business intelligence queries — used by the dashboard "BI" page.

Exposes pure-Python helpers that return plain dicts/lists. No Streamlit
imports here so the same functions can be reused by the PDF report
generator and any future API endpoint.

Functions
---------
conversion_funnel()        Pipeline counts per stage with deltas
agency_leaderboard(...)    Agency activity stats (volume + price moves)
zone_heatmap_data(...)     Per-zone aggregates ready for Folium markers
score_distribution()       Histogram bin counts for a chart
recent_signal_summary()    Headline numbers for the page hero
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import case, func, select

from storage.database import get_db
from storage.models import CRMNote, Lead


# ── Conversion funnel ────────────────────────────────────────────────────────

# Pipeline stages we surface in the funnel chart, in order. Stage names map
# to ``Lead.crm_stage`` as it is populated by the CRM tab.
FUNNEL_STAGES: tuple[tuple[str, str], ...] = (
    ("captured",   "Capturado"),
    ("qualified",  "Qualificado"),
    ("contacted",  "Contactado"),
    ("interested", "Interesse"),
    ("negotiating","Negociação"),
    ("closed",     "Fechado"),
)


def conversion_funnel(window_days: Optional[int] = None) -> list[dict]:
    """
    Return one row per pipeline stage with:
        key, label, count, pct_of_top
    """
    with get_db() as db:
        q = db.query(Lead.crm_stage, func.count(Lead.id))
        q = q.filter(Lead.archived == False)            # noqa: E712
        q = q.filter(Lead.is_demo == False)             # noqa: E712
        if window_days:
            cutoff = datetime.utcnow() - timedelta(days=window_days)
            q = q.filter(Lead.last_seen_at >= cutoff)
        counts: dict[str, int] = {
            (k or "captured"): n for k, n in q.group_by(Lead.crm_stage).all()
        }
        # captured = leads that exist at all
        total = sum(counts.values()) or 1

    rows: list[dict] = []
    cumulative = total
    top = total
    for key, label in FUNNEL_STAGES:
        n = counts.get(key, 0)
        if key == "captured":
            n = total
        rows.append({
            "key":         key,
            "label":       label,
            "count":       n,
            "pct_of_top":  round(100.0 * n / top, 1) if top else 0.0,
        })
    return rows


# ── Agency leaderboard ───────────────────────────────────────────────────────

def agency_leaderboard(limit: int = 25, min_listings: int = 3) -> list[dict]:
    """
    Aggregate per-agency stats ranked by listing volume.
    Returns rows:
        agency, total_listings, hot_count, avg_score, avg_price,
        avg_price_per_m2, has_contact_pct
    """
    with get_db() as db:
        rows = db.execute(
            select(
                Lead.agency_name,
                func.count(Lead.id).label("total"),
                func.sum(case((Lead.score_label == "HOT", 1), else_=0)).label("hot"),
                func.avg(Lead.score).label("avg_score"),
                func.avg(Lead.price).label("avg_price"),
                func.avg(Lead.price_per_m2).label("avg_ppm2"),
                func.sum(
                    case(
                        (
                            (Lead.contact_phone.isnot(None)) & (Lead.contact_phone != ""),
                            1,
                        ),
                        else_=0,
                    )
                ).label("with_phone"),
            )
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo == False)               # noqa: E712
            .where(Lead.owner_type == "agency")
            .where(Lead.agency_name.isnot(None))
            .where(Lead.agency_name != "")
            .group_by(Lead.agency_name)
            .order_by(func.count(Lead.id).desc())
            .limit(limit)
        ).all()

    results: list[dict] = []
    for r in rows:
        if r.total < min_listings:
            continue
        results.append({
            "agency":           r.agency_name,
            "total_listings":   int(r.total),
            "hot_count":        int(r.hot or 0),
            "avg_score":        round(float(r.avg_score or 0), 1),
            "avg_price":        round(float(r.avg_price or 0), 0),
            "avg_price_per_m2": round(float(r.avg_ppm2 or 0), 0),
            "has_contact_pct":  round(100.0 * (r.with_phone or 0) / r.total, 1),
        })
    return results


# ── Zone heatmap ─────────────────────────────────────────────────────────────

# Static centroid coordinates for fallback when leads in a zone don't have
# their own lat/lng. Mirrors utils/geocoder.py — kept here as a copy so this
# module has zero non-stdlib runtime dependencies.
_ZONE_CENTROIDS: dict[str, tuple[float, float]] = {
    "Lisboa":   (38.7223, -9.1393),
    "Cascais":  (38.6979, -9.4215),
    "Sintra":   (38.7980, -9.3900),
    "Almada":   (38.6790, -9.1569),
    "Seixal":   (38.6420, -9.1037),
    "Sesimbra": (38.4448, -9.1014),
    "Oeiras":   (38.6929, -9.3097),
    "Amadora":  (38.7536, -9.2302),
    "Loures":   (38.8307, -9.1681),
    "Odivelas": (38.7950, -9.1832),
    "Barreiro": (38.6633, -9.0697),
    "Montijo":  (38.7060, -8.9744),
    "Palmela":  (38.5703, -8.9000),
    "Setubal":  (38.5244, -8.8882),
}


def zone_heatmap_data(min_count: int = 1) -> list[dict]:
    """
    Aggregated stats per zone, ready to feed Folium markers.

    Returns:
        zone, count, avg_score, hot_count, avg_price_per_m2, lat, lon
    """
    with get_db() as db:
        rows = db.execute(
            select(
                Lead.zone,
                func.count(Lead.id).label("total"),
                func.avg(Lead.score).label("avg_score"),
                func.sum(case((Lead.score_label == "HOT", 1), else_=0)).label("hot"),
                func.avg(Lead.price_per_m2).label("avg_ppm2"),
                func.avg(Lead.latitude).label("avg_lat"),
                func.avg(Lead.longitude).label("avg_lng"),
            )
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo == False)               # noqa: E712
            .where(Lead.zone.isnot(None))
            .where(Lead.zone != "")
            .group_by(Lead.zone)
        ).all()

    results: list[dict] = []
    for r in rows:
        if r.total < min_count:
            continue
        # Prefer real geocoded centroid; fall back to static table
        lat, lon = r.avg_lat, r.avg_lng
        if lat is None or lon is None:
            fallback = _ZONE_CENTROIDS.get(r.zone)
            if not fallback:
                continue
            lat, lon = fallback
        results.append({
            "zone":             r.zone,
            "count":            int(r.total),
            "avg_score":        round(float(r.avg_score or 0), 1),
            "hot_count":        int(r.hot or 0),
            "avg_price_per_m2": round(float(r.avg_ppm2 or 0), 0),
            "lat":              float(lat),
            "lon":              float(lon),
        })
    return results


# ── Score distribution + headline summary ──────────────────────────────────

def score_distribution(bins: int = 20) -> list[dict]:
    """Histogram-friendly bins for the score column."""
    with get_db() as db:
        scores = [
            s[0] for s in db.execute(
                select(Lead.score)
                .where(Lead.archived == False)          # noqa: E712
                .where(Lead.is_demo == False)           # noqa: E712
            ).all()
        ]
    if not scores:
        return []
    bin_size = max(1, 100 // bins)
    counter = Counter()
    for s in scores:
        bucket = (int(s or 0) // bin_size) * bin_size
        counter[bucket] += 1
    return [{"bin": k, "count": counter[k]} for k in sorted(counter)]


def recent_signal_summary(window_days: int = 7) -> dict:
    """Headline numbers for the BI hero."""
    cutoff = datetime.utcnow() - timedelta(days=window_days)
    with get_db() as db:
        new_leads = db.query(Lead).filter(
            Lead.is_demo == False,                      # noqa: E712
            Lead.archived == False,                     # noqa: E712
            Lead.first_seen_at >= cutoff,
        ).count()
        new_hot = db.query(Lead).filter(
            Lead.is_demo == False,                      # noqa: E712
            Lead.archived == False,                     # noqa: E712
            Lead.score_label == "HOT",
            Lead.first_seen_at >= cutoff,
        ).count()
        price_drops = db.query(CRMNote).filter(
            CRMNote.created_at >= cutoff,
            CRMNote.body.like("%Price drop%"),
        ).count()
        super_sellers = db.query(Lead).filter(
            Lead.is_demo == False,                      # noqa: E712
            Lead.seller_super_flag == True,             # noqa: E712
        ).count()
        contacted = db.query(Lead).filter(
            Lead.is_demo == False,                      # noqa: E712
            Lead.crm_stage == "contacted",
        ).count()
    return {
        "new_leads_7d":   new_leads,
        "new_hot_7d":     new_hot,
        "price_drops_7d": price_drops,
        "super_sellers":  super_sellers,
        "contacted":      contacted,
    }

"""
Dashboard Metrics — pure DB query functions.

All functions are side-effect-free, return plain Python dicts/lists,
and are safe to call from any context (Streamlit, CLI, API).

No ORM models are imported here — raw SQL only — so this module has
no circular-import risk and can be called from dashboard/app.py
without pulling in the full pipeline stack.
"""
from __future__ import annotations

from typing import Any

from storage.database import engine
from utils.logger import get_logger

log = get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _q(sql: str, params: dict | None = None) -> list[Any]:
    """Execute a SELECT and return all rows."""
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql), params or {}).fetchall()
    except Exception as e:
        log.debug("[metrics] query error: {e}", e=e)
        return []


def _scalar(sql: str, params: dict | None = None) -> int:
    """Execute a scalar COUNT/SUM and return as int."""
    rows = _q(sql, params)
    if rows:
        return int(rows[0][0] or 0)
    return 0


# ── Core metrics ──────────────────────────────────────────────────────────────

def phone_coverage_rate() -> dict:
    """
    Overall phone coverage across all leads.

    Returns:
        {
            "total": int,
            "with_phone": int,
            "rate_pct": float,
            "mobile_phone": int,       -- 9xx numbers
            "landline_phone": int,     -- 2xx numbers
        }
    """
    total       = _scalar("SELECT COUNT(*) FROM leads")
    with_phone  = _scalar("SELECT COUNT(*) FROM leads WHERE contact_phone IS NOT NULL AND contact_phone != ''")
    mobile      = _scalar("SELECT COUNT(*) FROM leads WHERE contact_phone LIKE '+3519%'")
    landline    = _scalar("SELECT COUNT(*) FROM leads WHERE contact_phone LIKE '+3512%'")
    return {
        "total":         total,
        "with_phone":    with_phone,
        "rate_pct":      round(with_phone / total * 100, 2) if total else 0.0,
        "mobile_phone":  mobile,
        "landline_phone": landline,
    }


def fsbo_rate() -> dict:
    """
    FSBO (For Sale By Owner) lead statistics.

    Returns:
        {
            "total_leads": int,
            "fsbo_count": int,
            "fsbo_rate_pct": float,
            "fsbo_with_phone": int,
            "fsbo_with_phone_rate_pct": float,
            "fsbo_high_quality": int,
        }
    """
    total       = _scalar("SELECT COUNT(*) FROM leads")
    fsbo        = _scalar("SELECT COUNT(*) FROM leads WHERE lead_type = 'fsbo'")
    fsbo_phone  = _scalar("SELECT COUNT(*) FROM leads WHERE lead_type='fsbo' AND contact_phone IS NOT NULL AND contact_phone != ''")
    fsbo_high   = _scalar("SELECT COUNT(*) FROM leads WHERE lead_type='fsbo' AND lead_quality='high'")
    return {
        "total_leads":              total,
        "fsbo_count":               fsbo,
        "fsbo_rate_pct":            round(fsbo / total * 100, 2) if total else 0.0,
        "fsbo_with_phone":          fsbo_phone,
        "fsbo_with_phone_rate_pct": round(fsbo_phone / fsbo * 100, 2) if fsbo else 0.0,
        "fsbo_high_quality":        fsbo_high,
    }


def lead_quality_distribution() -> dict:
    """
    Distribution of leads across quality tiers + score labels.

    Returns:
        {
            "by_quality": {"high": int, "medium": int, "low": int},
            "by_score_label": {"HOT": int, "WARM": int, "COLD": int},
            "no_contact_pct": float,
            "conf_ge70_pct": float,
        }
    """
    from config.settings import settings

    total = _scalar("SELECT COUNT(*) FROM leads")
    rows  = _q("SELECT lead_quality, COUNT(*) FROM leads GROUP BY lead_quality")
    by_q  = {str(r[0] or "none"): int(r[1] or 0) for r in rows}

    rows2    = _q("SELECT score_label, COUNT(*) FROM leads WHERE score_label IS NOT NULL GROUP BY score_label")
    by_score = {str(r[0]): int(r[1] or 0) for r in rows2}

    zero    = _scalar("SELECT COUNT(*) FROM leads WHERE contact_confidence = 0")
    conf70  = _scalar("SELECT COUNT(*) FROM leads WHERE contact_confidence >= 70")

    return {
        "by_quality":     {"high": by_q.get("high",   0), "medium": by_q.get("medium", 0), "low": by_q.get("low", 0)},
        "by_score_label": {"HOT":  by_score.get("HOT", 0), "WARM": by_score.get("WARM", 0), "COLD": by_score.get("COLD", 0)},
        "no_contact_pct": round(zero   / total * 100, 2) if total else 0.0,
        "conf_ge70_pct":  round(conf70 / total * 100, 2) if total else 0.0,
        "total":          total,
    }


def phones_by_source() -> list[dict]:
    """
    Phone counts broken down by contact_source tag.

    Returns list of dicts sorted by phone count descending:
        [{"source": str, "phones": int, "rate_pct": float}, ...]
    """
    total = _scalar("SELECT COUNT(*) FROM leads WHERE contact_phone IS NOT NULL AND contact_phone != ''")
    rows  = _q(
        "SELECT contact_source, COUNT(*) AS phones "
        "FROM leads "
        "WHERE contact_phone IS NOT NULL AND contact_phone != '' "
        "GROUP BY contact_source "
        "ORDER BY phones DESC"
    )
    result = []
    for r in rows:
        source = str(r[0] or "unknown")
        phones = int(r[1] or 0)
        result.append({
            "source":    source,
            "phones":    phones,
            "rate_pct":  round(phones / total * 100, 1) if total else 0.0,
        })
    return result


def leads_by_city() -> list[dict]:
    """
    Lead counts and phone coverage per zone/city.

    Returns list of dicts sorted by total leads descending:
        [{
            "zone": str,
            "total": int,
            "with_phone": int,
            "phone_rate_pct": float,
            "fsbo_count": int,
            "fsbo_rate_pct": float,
            "high_quality": int,
            "avg_score": float | None,
        }, ...]
    """
    rows = _q(
        "SELECT "
        "  zone, "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN contact_phone IS NOT NULL AND contact_phone != '' THEN 1 ELSE 0 END) AS with_phone, "
        "  SUM(CASE WHEN lead_type = 'fsbo' THEN 1 ELSE 0 END) AS fsbo, "
        "  SUM(CASE WHEN lead_quality = 'high' THEN 1 ELSE 0 END) AS high_q, "
        "  ROUND(AVG(CAST(score AS FLOAT)), 1) AS avg_score "
        "FROM leads "
        "WHERE zone IS NOT NULL "
        "GROUP BY zone "
        "ORDER BY total DESC"
    )
    result = []
    for r in rows:
        zone   = str(r[0])
        total  = int(r[1] or 0)
        phones = int(r[2] or 0)
        fsbo   = int(r[3] or 0)
        high_q = int(r[4] or 0)
        avg_sc = float(r[5]) if r[5] is not None else None
        result.append({
            "zone":            zone,
            "total":           total,
            "with_phone":      phones,
            "phone_rate_pct":  round(phones / total * 100, 1) if total else 0.0,
            "fsbo_count":      fsbo,
            "fsbo_rate_pct":   round(fsbo   / total * 100, 1) if total else 0.0,
            "high_quality":    high_q,
            "avg_score":       avg_sc,
        })
    return result


def contact_channel_breakdown() -> dict:
    """
    Coverage across all contact channels.

    Returns:
        {
            "phone": int,
            "whatsapp": int,
            "email": int,
            "website": int,
            "any_contact": int,
            "no_contact": int,
            "total": int,
            "any_contact_rate_pct": float,
        }
    """
    total    = _scalar("SELECT COUNT(*) FROM leads")
    phone    = _scalar("SELECT COUNT(*) FROM leads WHERE contact_phone    IS NOT NULL AND contact_phone    != ''")
    wa       = _scalar("SELECT COUNT(*) FROM leads WHERE contact_whatsapp IS NOT NULL AND contact_whatsapp != ''")
    email    = _scalar("SELECT COUNT(*) FROM leads WHERE contact_email    IS NOT NULL AND contact_email    != ''")
    website  = _scalar("SELECT COUNT(*) FROM leads WHERE contact_website  IS NOT NULL AND contact_website  != ''")
    any_c    = _scalar(
        "SELECT COUNT(*) FROM leads WHERE "
        "(contact_phone    IS NOT NULL AND contact_phone    != '') OR "
        "(contact_whatsapp IS NOT NULL AND contact_whatsapp != '') OR "
        "(contact_email    IS NOT NULL AND contact_email    != '') OR "
        "(contact_website  IS NOT NULL AND contact_website  != '')"
    )
    return {
        "phone":               phone,
        "whatsapp":            wa,
        "email":               email,
        "website":             website,
        "any_contact":         any_c,
        "no_contact":          total - any_c,
        "total":               total,
        "any_contact_rate_pct": round(any_c / total * 100, 2) if total else 0.0,
    }


def scrape_source_summary() -> list[dict]:
    """
    Per-source stats (total leads, phones, FSBO) across all scrapers.

    Returns list sorted by total leads descending.
    """
    rows = _q(
        "SELECT "
        "  discovery_source, "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN contact_phone IS NOT NULL AND contact_phone != '' THEN 1 ELSE 0 END) AS phones, "
        "  SUM(CASE WHEN lead_type = 'fsbo' THEN 1 ELSE 0 END) AS fsbo, "
        "  ROUND(AVG(CAST(score AS FLOAT)), 1) AS avg_score "
        "FROM leads "
        "WHERE discovery_source IS NOT NULL "
        "GROUP BY discovery_source "
        "ORDER BY total DESC"
    )
    result = []
    for r in rows:
        total  = int(r[1] or 0)
        phones = int(r[2] or 0)
        result.append({
            "source":          str(r[0]),
            "total":           total,
            "phones":          phones,
            "phone_rate_pct":  round(phones / total * 100, 1) if total else 0.0,
            "fsbo_count":      int(r[3] or 0),
            "avg_score":       float(r[4]) if r[4] is not None else None,
        })
    return result


def top_actionable_leads(limit: int = 20) -> list[dict]:
    """
    Top leads with phone + highest score — ready to act on immediately.

    Returns list of dicts (id, zone, typology, price, phone, score, lead_type).
    """
    rows = _q(
        "SELECT id, zone, typology, price, contact_phone, score, score_label, "
        "lead_type, agency_name, contact_name "
        "FROM leads "
        "WHERE contact_phone IS NOT NULL AND contact_phone != '' "
        "  AND score IS NOT NULL "
        "ORDER BY score DESC "
        f"LIMIT {int(limit)}"
    )
    return [
        {
            "id":           int(r[0]),
            "zone":         str(r[1] or ""),
            "typology":     str(r[2] or ""),
            "price":        float(r[3]) if r[3] else None,
            "phone":        str(r[4]),
            "score":        int(r[5] or 0),
            "score_label":  str(r[6] or ""),
            "lead_type":    str(r[7] or ""),
            "agency_name":  str(r[8] or ""),
            "contact_name": str(r[9] or ""),
        }
        for r in rows
    ]


def frbo_rate() -> dict:
    """
    FRBO (For Rent By Owner) lead statistics.

    Returns:
        {
            "total_leads": int,
            "frbo_count": int,
            "frbo_rate_pct": float,
            "frbo_with_phone": int,
            "frbo_with_phone_rate_pct": float,
        }
    """
    total      = _scalar("SELECT COUNT(*) FROM leads")
    frbo       = _scalar("SELECT COUNT(*) FROM leads WHERE lead_type = 'frbo'")
    frbo_phone = _scalar(
        "SELECT COUNT(*) FROM leads WHERE lead_type='frbo' "
        "AND contact_phone IS NOT NULL AND contact_phone != ''"
    )
    return {
        "total_leads":              total,
        "frbo_count":               frbo,
        "frbo_rate_pct":            round(frbo / total * 100, 2) if total else 0.0,
        "frbo_with_phone":          frbo_phone,
        "frbo_with_phone_rate_pct": round(frbo_phone / frbo * 100, 2) if frbo else 0.0,
    }


def active_owners_summary() -> dict:
    """
    Active owner (marketplace / vehicle seller) lead statistics.

    Returns:
        {
            "total_leads": int,
            "active_owners_count": int,
            "active_owners_rate_pct": float,
            "active_owners_with_phone": int,
            "by_source": [{"source": str, "count": int}, ...],
        }
    """
    total      = _scalar("SELECT COUNT(*) FROM leads")
    ao         = _scalar("SELECT COUNT(*) FROM leads WHERE lead_type = 'active_owner'")
    ao_phone   = _scalar(
        "SELECT COUNT(*) FROM leads WHERE lead_type='active_owner' "
        "AND contact_phone IS NOT NULL AND contact_phone != ''"
    )
    rows = _q(
        "SELECT discovery_source, COUNT(*) AS cnt "
        "FROM leads WHERE lead_type = 'active_owner' "
        "GROUP BY discovery_source ORDER BY cnt DESC"
    )
    by_source = [{"source": str(r[0] or "unknown"), "count": int(r[1] or 0)} for r in rows]
    return {
        "total_leads":               total,
        "active_owners_count":       ao,
        "active_owners_rate_pct":    round(ao / total * 100, 2) if total else 0.0,
        "active_owners_with_phone":  ao_phone,
        "by_source":                 by_source,
    }


def leads_by_municipality() -> list[dict]:
    """
    Lead counts grouped by municipality (concelho).

    Returns list sorted by total descending:
        [{"municipality": str, "total": int, "with_phone": int, "phone_rate_pct": float}, ...]
    """
    rows = _q(
        "SELECT "
        "  municipality, "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN contact_phone IS NOT NULL AND contact_phone != '' THEN 1 ELSE 0 END) AS phones "
        "FROM leads "
        "WHERE municipality IS NOT NULL AND municipality != '' "
        "GROUP BY municipality "
        "ORDER BY total DESC"
    )
    result = []
    for r in rows:
        total  = int(r[1] or 0)
        phones = int(r[2] or 0)
        result.append({
            "municipality":   str(r[0]),
            "total":          total,
            "with_phone":     phones,
            "phone_rate_pct": round(phones / total * 100, 1) if total else 0.0,
        })
    return result


def phone_coverage_by_source() -> list[dict]:
    """
    Phone coverage breakdown per discovery_source with mobile/relay/landline split.

    Useful for measuring before/after impact of scraper improvements per source.

    Returns list sorted by total_leads descending:
        [{
            "source":       str,
            "total":        int,    -- all leads from this source
            "with_phone":   int,    -- leads with any phone
            "mobile":       int,    -- 9xx numbers (highest quality)
            "relay":        int,    -- 6xx numbers (OLX masking)
            "landline":     int,    -- 2xx numbers (often agency)
            "no_phone":     int,    -- leads with no phone at all
            "coverage_pct": float,  -- with_phone / total * 100
            "mobile_pct":   float,  -- mobile / with_phone * 100
        }, ...]
    """
    rows = _q(
        "SELECT "
        "  discovery_source, "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN contact_phone IS NOT NULL AND contact_phone != '' THEN 1 ELSE 0 END) AS with_phone, "
        "  SUM(CASE WHEN contact_phone LIKE '+3519%' THEN 1 ELSE 0 END) AS mobile, "
        "  SUM(CASE WHEN contact_phone LIKE '+3516%' THEN 1 ELSE 0 END) AS relay, "
        "  SUM(CASE WHEN contact_phone LIKE '+3512%' THEN 1 ELSE 0 END) AS landline "
        "FROM leads "
        "WHERE discovery_source IS NOT NULL "
        "GROUP BY discovery_source "
        "ORDER BY total DESC"
    )
    result = []
    for r in rows:
        total    = int(r[1] or 0)
        phones   = int(r[2] or 0)
        mobile   = int(r[3] or 0)
        relay    = int(r[4] or 0)
        landline = int(r[5] or 0)
        result.append({
            "source":       str(r[0]),
            "total":        total,
            "with_phone":   phones,
            "mobile":       mobile,
            "relay":        relay,
            "landline":     landline,
            "no_phone":     total - phones,
            "coverage_pct": round(phones / total  * 100, 1) if total  else 0.0,
            "mobile_pct":   round(mobile / phones * 100, 1) if phones else 0.0,
        })
    return result


def kpi_summary() -> dict:
    """
    Single-call KPI summary for the dashboard header strip.

    Returns all key numbers in one dict.
    """
    pc    = phone_coverage_rate()
    fsbo_ = fsbo_rate()
    frbo_ = frbo_rate()
    ao    = active_owners_summary()
    qual  = lead_quality_distribution()
    src   = scrape_source_summary()
    ch    = contact_channel_breakdown()

    return {
        # Volume
        "total_leads":              pc["total"],
        # Phone
        "phone_count":              pc["with_phone"],
        "phone_rate_pct":           pc["rate_pct"],
        "mobile_phone_count":       pc["mobile_phone"],
        # FSBO
        "fsbo_count":               fsbo_["fsbo_count"],
        "fsbo_rate_pct":            fsbo_["fsbo_rate_pct"],
        "fsbo_with_phone":          fsbo_["fsbo_with_phone"],
        # FRBO
        "frbo_count":               frbo_["frbo_count"],
        "frbo_rate_pct":            frbo_["frbo_rate_pct"],
        "frbo_with_phone":          frbo_["frbo_with_phone"],
        # Active Owners
        "active_owners_count":      ao["active_owners_count"],
        "active_owners_with_phone": ao["active_owners_with_phone"],
        # Quality
        "hot_count":                qual["by_score_label"]["HOT"],
        "warm_count":               qual["by_score_label"]["WARM"],
        "cold_count":               qual["by_score_label"]["COLD"],
        "high_quality_count":       qual["by_quality"]["high"],
        "no_contact_pct":           qual["no_contact_pct"],
        "conf_ge70_pct":            qual["conf_ge70_pct"],
        # Channels
        "any_contact_rate_pct":     ch["any_contact_rate_pct"],
        "whatsapp_count":           ch["whatsapp"],
        "email_count":              ch["email"],
        "website_count":            ch["website"],
    }

"""
DB hygiene jobs — keep the working set focused on actionable leads.

Stale auto-archive
------------------
A lead nobody touched for >60 days is dead weight. ``auto_archive_stale``
flips ``archived=True`` and stamps a stage of "stale_archived" so the
operator can find them later if needed but they no longer pollute the
HOT board, the CRM Kanban, or exports.

Trigger criteria (any one is enough):
  * ``last_seen_at`` > stale_days ago AND no CRMNote in that window
  * ``crm_stage in ('perdido', 'merged')`` for >7 days

Listing dropped detection
-------------------------
``mark_dropped_listings`` HEAD-checks each Lead's primary URL. When the
portal returns 404 or 410, the listing is gone — sold, retracted, or
expired. We mark ``listing_status='dropped'`` and bump the priority
flag so the operator sees the change in the dashboard.

Run cadence: 200 URLs per call (concurrency 6) so a 8000-lead DB is
swept in ~40 batches over time. The CLI command runs one batch per
invocation; the scheduler runs it daily.

Public API
----------
auto_archive_stale(stale_days=60) -> dict
mark_dropped_listings(limit=200) -> dict
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional

import httpx
from sqlalchemy import desc, or_, select

from storage.database import get_db
from storage.models import CRMNote, Lead
from utils.logger import get_logger

log = get_logger(__name__)


# ── Stale archive ────────────────────────────────────────────────────────────

def auto_archive_stale(stale_days: int = 60) -> dict:
    """
    Archive leads with no activity in the last ``stale_days`` AND no
    recent CRMNote. Idempotent.

    Stats: considered, archived, kept_recent_note, kept_priority
    """
    cutoff = datetime.utcnow() - timedelta(days=stale_days)
    stats = {
        "considered":         0,
        "archived":           0,
        "kept_recent_note":   0,
        "kept_priority":      0,
        "kept_active_stage":  0,
    }

    active_stages = {"contactado", "negociacao"}

    with get_db() as db:
        candidates = db.execute(
            select(Lead)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
            .where(or_(Lead.last_seen_at < cutoff, Lead.last_seen_at.is_(None)))
        ).scalars().all()
        stats["considered"] = len(candidates)

        for lead in candidates:
            if (lead.crm_stage or "").lower() in active_stages:
                stats["kept_active_stage"] += 1
                continue
            if lead.priority_flag:
                stats["kept_priority"] += 1
                continue

            # Skip if any CRMNote within the window
            recent_note = db.execute(
                select(CRMNote.id)
                .where(CRMNote.lead_id == lead.id)
                .where(CRMNote.created_at >= cutoff)
                .limit(1)
            ).first()
            if recent_note:
                stats["kept_recent_note"] += 1
                continue

            lead.archived = True
            if lead.crm_stage not in ("perdido", "merged"):
                lead.crm_stage = "stale_archived"
            stats["archived"] += 1

        db.commit()

    log.info(
        "[maintenance] stale archive — archived={a} kept_note={n} "
        "kept_priority={p} kept_active={c} considered={t}",
        a=stats["archived"], n=stats["kept_recent_note"],
        p=stats["kept_priority"], c=stats["kept_active_stage"],
        t=stats["considered"],
    )
    return stats


# ── Listing dropped detection ────────────────────────────────────────────────

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def mark_dropped_listings(limit: int = 200) -> dict:
    """
    HEAD-check each lead's primary URL. 404 / 410 → mark dropped.
    Concurrency-controlled (6 in parallel) to stay polite on portals.

    Note: ``listing_status`` is stored opportunistically — if the column
    doesn't exist (older DB without the migration applied), we fall back
    to a CRMNote and a priority_flag bump.
    """
    stats = {
        "considered":   0,
        "checked":      0,
        "dropped":      0,
        "errors":       0,
        "alive":        0,
        "skipped_no_url": 0,
    }

    with get_db() as db:
        leads = db.execute(
            select(Lead)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
            .where(or_(
                Lead.listing_status.is_(None),
                Lead.listing_status != "dropped",
            ) if hasattr(Lead, "listing_status") else (Lead.id > 0))
            .order_by(Lead.last_seen_at.asc())
            .limit(limit)
        ).scalars().all()
        stats["considered"] = len(leads)

    if not leads:
        return stats

    targets: list[tuple[int, str]] = []
    for lead in leads:
        srcs = lead.sources
        url = srcs[0]["url"] if srcs and srcs[0].get("url") else None
        if not url:
            stats["skipped_no_url"] += 1
            continue
        targets.append((lead.id, url))

    if not targets:
        return stats

    # Async HEAD with httpx
    async def _check_all(items: list[tuple[int, str]]) -> dict[int, int]:
        sema = asyncio.Semaphore(6)
        out: dict[int, int] = {}

        async with httpx.AsyncClient(
            timeout=12.0,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-PT,pt;q=0.9",
            },
        ) as client:
            async def _one(lid: int, url: str) -> None:
                async with sema:
                    try:
                        # Use GET (HEAD often unreliable on portal CDNs)
                        # but read only the headers
                        async with client.stream("GET", url) as resp:
                            out[lid] = resp.status_code
                    except Exception:
                        out[lid] = 0
            await asyncio.gather(*(_one(lid, url) for lid, url in items))
        return out

    try:
        results = asyncio.run(_check_all(targets))
    except RuntimeError:
        log.debug("[maintenance] event loop already running — skipping")
        return stats

    with get_db() as db:
        for lid, status in results.items():
            stats["checked"] += 1
            if status in (404, 410):
                stats["dropped"] += 1
                lead = db.query(Lead).get(lid)
                if not lead:
                    continue
                if hasattr(lead, "listing_status"):
                    lead.listing_status = "dropped"
                lead.priority_flag = True
                db.add(CRMNote(
                    lead_id    = lid,
                    note       = f"❌ Anúncio removido do portal (HTTP {status}). "
                                 f"Pode estar vendido — confirmar com o vendedor.",
                    note_type  = "listing_dropped",
                    created_by = "maintenance-bot",
                    created_at = datetime.utcnow(),
                ))
            elif status >= 200 and status < 400:
                stats["alive"] += 1
                if hasattr(Lead, "listing_status"):
                    db.query(Lead).filter(Lead.id == lid).update(
                        {"listing_status": "alive"}
                    )
            else:
                stats["errors"] += 1
        db.commit()

    log.info(
        "[maintenance] dropped sweep — checked={c} dropped={d} alive={a} errors={e}",
        c=stats["checked"], d=stats["dropped"], a=stats["alive"], e=stats["errors"],
    )
    return stats

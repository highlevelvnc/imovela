"""
Lead nurture sequences — keep dormant pipeline rows visible.

Why
---
A captured-but-not-contacted lead has the half-life of a banana. After 3
days the price might already have moved; after 7 the seller may have
fielded calls from competitors; after 14 it's effectively dead. Nuno's
biggest leak isn't lead capture — it's failing to follow up on time.

This module reads the time-since-last-touch on every active lead and
generates ``CRMNote`` reminders when the lead crosses a configurable
threshold for its stage.

Stages and default schedule (override per-tenant via .env):

    stage         day_offsets         default action
    ───────────   ─────────────       ─────────────────────────────────
    novo          1, 3                "📞 Primeira chamada hoje"
    contactado    3, 7                "💬 Follow-up WhatsApp"
    negociacao    2, 5, 10            "✉ Nova proposta / oferta"
    ganho         (none)              dormant
    perdido       (none)              dormant

A note is only emitted if no other note for that lead was added in the
last ``min_gap_days`` (default 1). Idempotent — running every hour is
safe and won't spam the operator.

CLI: ``python main.py nurture-tick`` (also wired into scheduler/jobs.py)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import desc, select

from storage.database import get_db
from storage.models import CRMNote, Lead
from utils.logger import get_logger

log = get_logger(__name__)


# ── Default templates ────────────────────────────────────────────────────────
# Override via NURTURE_<STAGE>_DAYS / NURTURE_<STAGE>_TEMPLATE env vars.
# Day numbers are days-since-last-touch on the lead, not absolute dates.

DEFAULT_SCHEDULES: dict[str, list[int]] = {
    "novo":       [1, 3],
    "contactado": [3, 7, 14],
    "negociacao": [2, 5, 10],
    # ganho + perdido stay dormant
}

DEFAULT_TEMPLATES: dict[str, str] = {
    "novo":       (
        "📞 Lembrete · primeira chamada ainda pendente — "
        "tentar contacto direto pelo número/whatsapp."
    ),
    "contactado": (
        "💬 Follow-up sugerido — verificar se o vendedor já ouviu outras "
        "propostas. Tentar WhatsApp se chamada falhar."
    ),
    "negociacao": (
        "✉ Reforçar proposta ou marcar visita. "
        "Considerar pequena revisão de valor para fechar."
    ),
}


def _schedule_for(stage: str) -> list[int]:
    """Read schedule for a stage from env (NURTURE_<STAGE>_DAYS=2,5,10) or default."""
    raw = os.environ.get(f"NURTURE_{stage.upper()}_DAYS", "")
    if raw.strip():
        try:
            return sorted({int(p) for p in raw.split(",") if p.strip()})
        except ValueError:
            pass
    return DEFAULT_SCHEDULES.get(stage, [])


def _template_for(stage: str) -> str:
    """Per-stage template, env-overridable as NURTURE_<STAGE>_TEMPLATE."""
    raw = os.environ.get(f"NURTURE_{stage.upper()}_TEMPLATE", "")
    return raw.strip() or DEFAULT_TEMPLATES.get(stage, "🔔 Follow-up devido")


# ── Tick runner ──────────────────────────────────────────────────────────────

def run_nurture_tick(min_gap_days: int = 1) -> dict:
    """
    Scan every active lead and create CRMNote reminders for those past
    their stage thresholds. Idempotent — won't re-emit a reminder for a
    lead that already received any note in the last ``min_gap_days``.

    Returns stats dict:
      considered, eligible, skipped_recent_note, reminders_added
    """
    stats = {
        "considered":          0,
        "eligible":            0,
        "skipped_recent_note": 0,
        "reminders_added":     0,
    }

    now = datetime.utcnow()
    gap_cutoff = now - timedelta(days=min_gap_days)

    with get_db() as db:
        leads = db.execute(
            select(Lead)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
        ).scalars().all()
        stats["considered"] = len(leads)

        for lead in leads:
            stage = (lead.crm_stage or "novo").lower()
            schedule = _schedule_for(stage)
            if not schedule:
                continue

            # Days since last activity
            anchor = lead.last_seen_at or lead.first_seen_at or now
            days_idle = (now - anchor).days
            if days_idle <= 0:
                continue

            # Find the largest threshold this lead has CROSSED
            crossed = [d for d in schedule if days_idle >= d]
            if not crossed:
                continue
            threshold = max(crossed)
            stats["eligible"] += 1

            # Skip when a note already exists in the gap window — idempotency
            recent_note = db.execute(
                select(CRMNote.id)
                .where(CRMNote.lead_id == lead.id)
                .where(CRMNote.created_at >= gap_cutoff)
                .order_by(desc(CRMNote.created_at))
                .limit(1)
            ).first()
            if recent_note:
                stats["skipped_recent_note"] += 1
                continue

            # Compose the reminder body
            template = _template_for(stage)
            body = (
                f"{template}\n\n"
                f"Stage: {stage} · {days_idle} dias parado · "
                f"threshold {threshold}d · score {lead.score or 0}"
            )

            db.add(CRMNote(
                lead_id    = lead.id,
                note       = body,
                note_type  = "nurture",
                created_by = "nurture-bot",
                created_at = now,
            ))
            # Bump priority_flag so the lead floats to the top of the dashboard
            lead.priority_flag = True
            stats["reminders_added"] += 1

        db.commit()

    log.info(
        "[nurture] considered={c} eligible={e} skipped={s} added={a}",
        c=stats["considered"], e=stats["eligible"],
        s=stats["skipped_recent_note"], a=stats["reminders_added"],
    )
    return stats


# ── Convenience: snapshot of pending reminders for a stage ───────────────────

def pending_per_stage() -> dict[str, int]:
    """
    Return ``{stage: count}`` of leads currently past at least one schedule
    threshold and lacking a recent note. Used by the dashboard to render a
    "X leads need follow-up" badge.
    """
    out: dict[str, int] = {s: 0 for s in DEFAULT_SCHEDULES}
    now = datetime.utcnow()
    gap_cutoff = now - timedelta(days=1)

    with get_db() as db:
        leads = db.execute(
            select(Lead)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
            .where(Lead.crm_stage.in_(list(DEFAULT_SCHEDULES.keys())))
        ).scalars().all()

        for lead in leads:
            stage = (lead.crm_stage or "novo").lower()
            schedule = _schedule_for(stage)
            if not schedule:
                continue
            anchor = lead.last_seen_at or lead.first_seen_at or now
            days_idle = (now - anchor).days
            if days_idle < schedule[0]:
                continue
            recent = db.execute(
                select(CRMNote.id)
                .where(CRMNote.lead_id == lead.id)
                .where(CRMNote.created_at >= gap_cutoff)
                .limit(1)
            ).first()
            if recent:
                continue
            out[stage] = out.get(stage, 0) + 1

    return out

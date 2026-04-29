"""
Price-drop detector — finds leads whose asking price dropped meaningfully
in a recent window and marks them as URGENT.

Why
---
A seller who drops the price ≥5% within 2 weeks is signalling impatience.
For a lead-engine optimised around "be the first to call", these are the
most actionable rows in the entire DB.

Mechanics
---------
The pipeline already records every observed price into the ``price_history``
table on ingest. This detector:

  1. Walks every active (non-archived) lead with ≥2 price observations.
  2. Compares the latest price against the highest price observed inside
     a configurable LOOKBACK window (default 30 days).
  3. Computes the drop_pct = (peak - latest) / peak.
  4. Flags rows where drop_pct ≥ MIN_DROP_PCT and the drop happened within
     RECENT_WINDOW_DAYS (default 14).
  5. Bumps ``priority_flag = True`` so the dashboard surfaces them at the top,
     records the drop in a CRMNote for audit, and bumps score by SCORE_BOOST.

Idempotent — re-runs only re-bump leads with new drops since last detection.

Public API
----------
``PriceDropDetector().run(lookback_days, recent_window_days, min_drop_pct)``
    Returns stats dict:
      considered  — leads with ≥2 history rows
      dropped     — leads where drop_pct exceeded threshold
      newly_flagged — leads moved to priority_flag=True this run
      avg_drop    — mean drop_pct across all flagged leads (display only)

CLI: ``python main.py detect-price-drops``
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select

from utils.logger import get_logger

log = get_logger(__name__)

# Tuned for PT residential market — observed ~10% of FSBOs drop by ≥5%
# in their first 30 days when not getting calls. Threshold catches them
# without surfacing every random ~1-2% revision.
DEFAULT_MIN_DROP_PCT       = 0.05    # 5%
DEFAULT_LOOKBACK_DAYS      = 30
DEFAULT_RECENT_WINDOW_DAYS = 14

# Score boost applied to leads with confirmed drop. Big enough to nudge
# borderline COLD/WARM into HOT, small enough not to dominate other signals.
SCORE_BOOST = 15


@dataclass
class DropResult:
    lead_id:    int
    peak_price: float
    last_price: float
    drop_pct:   float
    drop_eur:   float
    days_since_peak: int


class PriceDropDetector:
    """Detect material price reductions in recent listing history."""

    def run(
        self,
        lookback_days:      int   = DEFAULT_LOOKBACK_DAYS,
        recent_window_days: int   = DEFAULT_RECENT_WINDOW_DAYS,
        min_drop_pct:       float = DEFAULT_MIN_DROP_PCT,
        score_boost:        int   = SCORE_BOOST,
    ) -> dict:
        from storage.database import get_db
        from storage.models import CRMNote, Lead, PriceHistory

        cutoff_lookback = datetime.utcnow() - timedelta(days=lookback_days)
        cutoff_recent   = datetime.utcnow() - timedelta(days=recent_window_days)

        stats = {
            "considered":    0,
            "with_history":  0,
            "dropped":       0,
            "newly_flagged": 0,
            "avg_drop":      0.0,
        }
        drops: list[DropResult] = []

        with get_db() as db:
            # Candidate leads: active, with at least one history row in window
            candidates = db.execute(
                select(Lead)
                .where(Lead.archived == False)            # noqa: E712
                .where(Lead.is_demo  == False)            # noqa: E712
                .where(Lead.price.isnot(None))
            ).scalars().all()
            stats["considered"] = len(candidates)

            for lead in candidates:
                # Pull this lead's price observations within lookback window
                history = db.execute(
                    select(PriceHistory)
                    .where(PriceHistory.lead_id == lead.id)
                    .where(PriceHistory.recorded_at >= cutoff_lookback)
                    .order_by(PriceHistory.recorded_at.asc())
                ).scalars().all()
                if len(history) < 2:
                    continue
                stats["with_history"] += 1

                peak     = max(history, key=lambda h: h.price)
                last     = history[-1]
                if peak.price <= 0:
                    continue
                drop_pct = (peak.price - last.price) / peak.price
                if drop_pct < min_drop_pct:
                    continue

                # Drop must be RECENT to count — old drops aren't actionable
                if last.recorded_at < cutoff_recent:
                    continue

                stats["dropped"] += 1
                drops.append(DropResult(
                    lead_id    = lead.id,
                    peak_price = peak.price,
                    last_price = last.price,
                    drop_pct   = drop_pct,
                    drop_eur   = peak.price - last.price,
                    days_since_peak = max(
                        (last.recorded_at - peak.recorded_at).days, 0
                    ),
                ))

                # Idempotent flagging — already-priority leads don't re-bump.
                # Note: we always re-record the drop in CRMNotes so the audit
                # trail captures progressive drops on the same listing.
                already_flagged = bool(lead.priority_flag)
                lead.priority_flag = True

                if not already_flagged:
                    stats["newly_flagged"] += 1
                    lead.score = min(int((lead.score or 0) + score_boost), 100)

                    # Re-derive label after score bump
                    from config.settings import settings
                    if lead.score >= settings.hot_score_threshold:
                        lead.score_label = "HOT"
                    elif lead.score >= settings.warm_score_threshold:
                        lead.score_label = "WARM"
                    else:
                        lead.score_label = "COLD"

                # Always record the drop in CRMNotes (handy for the dashboard)
                note_body = (
                    f"💸 Price drop {drop_pct:.1%} "
                    f"€{peak.price:,.0f} → €{last.price:,.0f} "
                    f"({last.recorded_at.date()} vs peak {peak.recorded_at.date()})"
                )
                db.add(CRMNote(
                    lead_id    = lead.id,
                    body       = note_body,
                    created_at = datetime.utcnow(),
                ))

            db.commit()

        if drops:
            stats["avg_drop"] = sum(d.drop_pct for d in drops) / len(drops)

        log.info(
            "[price_drop] considered={c} with_history={h} dropped={d} "
            "newly_flagged={f} avg_drop={a:.1%}",
            c=stats["considered"], h=stats["with_history"],
            d=stats["dropped"],   f=stats["newly_flagged"],
            a=stats["avg_drop"],
        )
        return stats

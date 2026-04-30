"""
Listing change detection — surface re-marketing signals.

Why this is uniquely valuable
------------------------------
A seller who edits their own listing's title, description, or switches
to a different agency mid-flight is broadcasting impatience. They tried
the original copy, didn't get traction, and are now experimenting. These
edits are STRONGER signals than even price drops — most sellers tweak
copy 3-5x before they cut the price.

Nobody else aggregates this in PT real estate. We can.

Mechanics
---------
The pipeline already upserts leads on every scrape run, replacing the
title/description/agency_name fields with the freshest values. Without
this module, that history is lost the moment the upsert lands.

``ChangeDetector.detect()`` runs INSIDE the upsert path: receives the
existing Lead and the freshly-enriched payload, computes the diff
across the tracked fields, and emits a ``CRMNote`` with note_type=
"change_detected" describing what changed. Notes carry compact JSON-
encoded deltas so the dashboard can render coloured "title changed",
"new agency", "description rewritten" badges.

Tracked fields
--------------
  * title          — text similarity below threshold = rewrite
  * description    — text similarity below threshold = rewrite
  * agency_name    — non-empty change = re-listing with new agency
  * contact_phone  — phone changed = different agent

Side effects
------------
  * Bumps ``priority_flag = True`` on any tracked-field change so the
    dashboard floats the lead to the top automatically.
  * Score bump of +5 (capped at 100) so re-marketed leads sort above
    untouched competitors of the same base score.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

from utils.logger import get_logger

log = get_logger(__name__)


# Below this similarity we consider the text rewritten (not just a typo fix).
SIMILARITY_REWRITE_THRESHOLD: float = 0.78
SCORE_BUMP_ON_CHANGE: int = 5


@dataclass
class ChangeRecord:
    field:    str
    old:      str
    new:      str
    severity: str          # "rewrite" | "edit" | "switch" | "appended"


def _similarity(a: str, b: str) -> float:
    a, b = (a or "").strip(), (b or "").strip()
    if not a and not b:
        return 1.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _classify_text(old: str, new: str) -> Optional[str]:
    """
    Compare old vs new for a long text field.
    Returns "rewrite" / "edit" / "appended" / None if unchanged or trivial.
    """
    old, new = (old or "").strip(), (new or "").strip()
    if not new or new == old:
        return None
    if not old:
        return None      # first-time fill, not a change
    sim = _similarity(old, new)
    if sim < SIMILARITY_REWRITE_THRESHOLD:
        return "rewrite"
    if old in new and len(new) > len(old) + 30:
        return "appended"
    return "edit"


# ── Public detector ──────────────────────────────────────────────────────────

class ChangeDetector:
    """
    Compares an existing Lead row against incoming enriched data and emits
    a CRMNote whenever a tracked field meaningfully changed.
    """

    def detect(self, existing, enriched: dict) -> list[ChangeRecord]:
        """
        Returns the list of detected changes (empty when nothing changed).
        Caller is expected to be inside an active DB session.
        """
        changes: list[ChangeRecord] = []

        # ── Title
        new_title = (enriched.get("title") or "").strip()
        old_title = (existing.title or "").strip()
        verdict = _classify_text(old_title, new_title)
        if verdict:
            changes.append(ChangeRecord("title", old_title, new_title, verdict))

        # ── Description
        new_desc = (enriched.get("description") or "").strip()
        old_desc = (existing.description or "").strip()
        verdict = _classify_text(old_desc, new_desc)
        if verdict:
            changes.append(ChangeRecord(
                "description",
                old_desc[:200] + ("..." if len(old_desc) > 200 else ""),
                new_desc[:200] + ("..." if len(new_desc) > 200 else ""),
                verdict,
            ))

        # ── Agency switch (only when both sides non-empty AND different)
        new_agency = (enriched.get("agency_name") or "").strip()
        old_agency = (existing.agency_name or "").strip()
        if new_agency and old_agency and new_agency.lower() != old_agency.lower():
            if _similarity(new_agency, old_agency) < 0.85:
                changes.append(ChangeRecord("agency_name", old_agency, new_agency, "switch"))

        # ── Phone replacement
        new_phone = (enriched.get("contact_phone") or "").strip()
        old_phone = (existing.contact_phone or "").strip()
        if new_phone and old_phone and new_phone != old_phone:
            changes.append(ChangeRecord("contact_phone", old_phone, new_phone, "switch"))

        return changes

    def emit_note(self, db, existing, changes: list[ChangeRecord]) -> None:
        """
        Persist a single CRMNote summarising the changes, mark the lead as
        priority, and bump its score by SCORE_BUMP_ON_CHANGE (capped at 100).
        """
        if not changes:
            return
        from storage.models import CRMNote

        # Build human-readable body + machine-readable JSON tail
        emoji = {
            "rewrite":  "✏️",
            "edit":     "📝",
            "appended": "➕",
            "switch":   "🔄",
        }
        lines = ["Re-marketing detectado:"]
        payload = []
        for c in changes:
            ico = emoji.get(c.severity, "•")
            lines.append(f"{ico} {c.field} ({c.severity})")
            if c.severity in ("switch",) and len(c.old) < 60 and len(c.new) < 60:
                lines.append(f"   {c.old}  →  {c.new}")
            payload.append({
                "field":    c.field,
                "severity": c.severity,
                "old":      c.old[:240],
                "new":      c.new[:240],
            })

        body = "\n".join(lines)
        body += "\n\n[changeset]" + json.dumps(payload, ensure_ascii=False)

        db.add(CRMNote(
            lead_id    = existing.id,
            note       = body,
            note_type  = "change_detected",
            created_by = "change-detector",
            created_at = datetime.utcnow(),
        ))

        # Bump priority + score
        existing.priority_flag = True
        existing.score = min(int((existing.score or 0) + SCORE_BUMP_ON_CHANGE), 100)
        # Re-derive label
        from config.settings import settings
        if existing.score >= settings.hot_score_threshold:
            existing.score_label = "HOT"
        elif existing.score >= settings.warm_score_threshold:
            existing.score_label = "WARM"
        else:
            existing.score_label = "COLD"

        log.debug(
            "[change_detector] lead #{id} → {n} changes: {fields}",
            id=existing.id, n=len(changes),
            fields=[c.field for c in changes],
        )

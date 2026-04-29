"""
CRM Manager — handles pipeline stage transitions and interaction notes.

Stages:
  novo → contactado → negociação → ganho | perdido | arquivado

All transitions are logged as CRM notes automatically.
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from storage.database import get_db
from storage.models import CRMNote, Lead
from storage.repository import CRMNoteRepo, LeadRepo
from utils.logger import get_logger

log = get_logger(__name__)

# Valid CRM stages and their allowed transitions
STAGES = ["novo", "contactado", "negociação", "ganho", "perdido", "arquivado"]

VALID_TRANSITIONS: dict[str, list[str]] = {
    "novo":        ["contactado", "arquivado"],
    "contactado":  ["negociação", "perdido", "arquivado"],
    "negociação":  ["ganho", "perdido", "arquivado"],
    "ganho":       ["arquivado"],
    "perdido":     ["arquivado", "novo"],
    "arquivado":   ["novo"],
}


class CRMManager:

    # ── Stage management ──────────────────────────────────────────────────────

    def move_to_stage(
        self,
        lead_id: int,
        new_stage: str,
        note: str = "",
        by: str = "Nuno Reis",
    ) -> bool:
        """
        Transition a lead to a new CRM stage.
        Validates the transition, updates the DB, and logs a note.
        Returns True on success.
        """
        if new_stage not in STAGES:
            log.error("Invalid stage: {s}", s=new_stage)
            return False

        with get_db() as db:
            repo = LeadRepo(db)
            note_repo = CRMNoteRepo(db)
            lead = repo.get_by_id(lead_id)

            if not lead:
                log.warning("Lead {id} not found", id=lead_id)
                return False

            current_stage = lead.crm_stage
            allowed = VALID_TRANSITIONS.get(current_stage, [])

            if new_stage not in allowed:
                log.warning(
                    "Invalid transition {from_} → {to} for lead {id}",
                    **{"from_": current_stage, "to": new_stage, "id": lead_id},
                )
                return False

            # Update stage
            lead.crm_stage = new_stage
            lead.updated_at = datetime.utcnow()

            if new_stage == "arquivado":
                lead.archived = True

            # Auto-log transition note
            auto_note = f"Stage alterado: {current_stage} → {new_stage}"
            if note:
                auto_note += f"\n{note}"

            note_repo.add(
                lead_id=lead_id,
                note=auto_note,
                note_type="internal",
                created_by=by,
            )

        log.info("Lead {id} moved {from_} → {to}", **{"id": lead_id, "from_": current_stage, "to": new_stage})
        return True

    def add_note(
        self,
        lead_id: int,
        note: str,
        note_type: str = "internal",
        by: str = "Nuno Reis",
    ) -> Optional[CRMNote]:
        """Add an interaction note to a lead."""
        with get_db() as db:
            repo = CRMNoteRepo(db)
            return repo.add(lead_id=lead_id, note=note, note_type=note_type, created_by=by)

    def set_priority(self, lead_id: int, priority: bool = True) -> bool:
        """Toggle the priority flag on a lead."""
        with get_db() as db:
            repo = LeadRepo(db)
            lead = repo.get_by_id(lead_id)
            if not lead:
                return False
            lead.priority_flag = priority
            lead.updated_at = datetime.utcnow()
        return True

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_pipeline_summary(self) -> dict[str, int]:
        """Return count of leads per stage (for Kanban header)."""
        with get_db() as db:
            repo = LeadRepo(db)
            return repo.count_by_stage()

    def get_notes(self, lead_id: int) -> List[CRMNote]:
        """Return all notes for a lead, newest first."""
        with get_db() as db:
            repo = CRMNoteRepo(db)
            return repo.list_for_lead(lead_id)

    def get_leads_by_stage(self, stage: str) -> List[Lead]:
        """Return all non-archived leads for a given CRM stage."""
        with get_db() as db:
            repo = LeadRepo(db)
            return repo.list_active(crm_stage=stage)

    def get_all_pipeline_leads(self) -> dict[str, List[Lead]]:
        """Return all leads grouped by stage — useful for Kanban rendering."""
        result: dict[str, List[Lead]] = {}
        for stage in STAGES:
            result[stage] = self.get_leads_by_stage(stage)
        return result

    def get_recent_activity(self, limit: int = 20) -> List[CRMNote]:
        """Return the most recent CRM notes across all leads."""
        with get_db() as db:
            return (
                db.query(CRMNote)
                .order_by(CRMNote.created_at.desc())
                .limit(limit)
                .all()
            )

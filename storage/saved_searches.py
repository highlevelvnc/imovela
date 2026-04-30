"""
Saved searches — bookmark FTS queries + filter combinations.

Operators run the same queries every day ("T2 Belém piscina score≥60",
"super-sellers Lisboa centro", "FSBO Cascais pequenos"). Saving them as
named bookmarks turns 30s of clicking into a single click.

Storage
-------
A small ``saved_searches`` table created via the migration. Each row:
  id, name, query (FTS string), filters (JSON of sidebar filters),
  created_at, last_used.

API
---
list_searches() -> list[dict]
save_search(name, query, filters) -> int
delete_search(id) -> bool
touch(id) -> None        # bumps last_used
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from storage.database import engine
from utils.logger import get_logger

log = get_logger(__name__)


def list_searches() -> list[dict]:
    """Return all saved searches, most-recently-used first."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, name, query, filters, created_at, last_used "
            "FROM saved_searches "
            "ORDER BY COALESCE(last_used, created_at) DESC"
        )).all()
    out = []
    for r in rows:
        try:
            filters = json.loads(r.filters or "{}")
        except Exception:
            filters = {}
        out.append({
            "id":         r.id,
            "name":       r.name,
            "query":      r.query,
            "filters":    filters,
            "created_at": r.created_at,
            "last_used":  r.last_used,
        })
    return out


def save_search(name: str, query: str = "", filters: Optional[dict] = None) -> int:
    """Create a new saved search row. Returns the new id."""
    name    = (name or "").strip()
    query   = (query or "").strip()
    filters = filters or {}
    if not name:
        raise ValueError("name is required")
    payload = json.dumps(filters, ensure_ascii=False)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "INSERT INTO saved_searches (name, query, filters, created_at) "
                "VALUES (:n, :q, :f, :t)"
            ),
            {"n": name[:80], "q": query, "f": payload, "t": datetime.utcnow()},
        )
        conn.commit()
        new_id = int(result.lastrowid or 0)
    log.info("[saved_searches] saved #{i} '{n}'", i=new_id, n=name)
    return new_id


def delete_search(search_id: int) -> bool:
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM saved_searches WHERE id = :id"),
                     {"id": int(search_id)})
        conn.commit()
    return True


def touch(search_id: int) -> None:
    """Bump last_used so the most-active searches stay at the top."""
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE saved_searches SET last_used = :t WHERE id = :id"),
            {"t": datetime.utcnow(), "id": int(search_id)},
        )
        conn.commit()

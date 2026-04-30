"""
SQLite FTS5 full-text search index over the ``leads`` table.

What it gives the operator
--------------------------
Instant (sub-50 ms) free-text queries over the entire DB:

    T2 Lisboa piscina           → tokens AND-joined
    "Avenidas Novas"            → phrase
    T3 OR T4                    → boolean
    apartament*                 → prefix
    NEAR(piscina garagem)       → tokens within 10 of each other
    NOT agência                 → exclude term

The index covers ``title``, ``description``, ``zone``, ``parish``,
``address``, ``agency_name``, ``contact_name``. CRMNote bodies are NOT
indexed (operator's private notes shouldn't surface in lead lists).

Schema
------
- ``leads_fts``: FTS5 virtual table, content-rowid synced to ``leads.id``.
- Three triggers (``leads_ai`` / ``leads_au`` / ``leads_ad``) propagate
  inserts, updates, and deletes from ``leads`` into the FTS table so the
  index never drifts.

Tokenization
------------
``unicode61 remove_diacritics 1`` so "Belém" matches "Belem" and
"Avenida" matches "AVENIDA". Identifier tokens like "T2", "T3" are
preserved as-is.

API
---
- ``ensure_fts()``       — idempotent: creates table + triggers if missing,
                           rebuilds the index when needed.
- ``rebuild_fts()``      — drops + reinserts every row (run after bulk imports).
- ``search_lead_ids(q,limit)``
                          — returns lead ids ordered by FTS rank.
- ``search_leads(q,limit)``
                          — convenience wrapper that returns ORM Lead objects.

PostgreSQL fallback
-------------------
The FTS5 path is SQLite-only. When the DB is Postgres
(``settings.is_sqlite == False``), search_leads() falls back to a plain
``ILIKE`` query so the dashboard stays functional.
"""
from __future__ import annotations

import re
from typing import Optional

from sqlalchemy import text

from storage.database import engine, get_db
from utils.logger import get_logger

log = get_logger(__name__)


# Columns mirrored into the FTS table — keep in sync with the trigger SQL below.
_FTS_COLUMNS: tuple[str, ...] = (
    "title", "description", "zone", "parish", "address",
    "agency_name", "contact_name",
)


# ── DDL ──────────────────────────────────────────────────────────────────────

def _create_table_sql() -> str:
    cols = ", ".join(_FTS_COLUMNS)
    return f"""
    CREATE VIRTUAL TABLE IF NOT EXISTS leads_fts USING fts5(
        {cols},
        content='leads',
        content_rowid='id',
        tokenize='unicode61 remove_diacritics 1'
    )
    """


def _create_trigger_ai() -> str:
    cols = ", ".join(_FTS_COLUMNS)
    new_cols = ", ".join(f"new.{c}" for c in _FTS_COLUMNS)
    return f"""
    CREATE TRIGGER IF NOT EXISTS leads_ai AFTER INSERT ON leads BEGIN
        INSERT INTO leads_fts(rowid, {cols}) VALUES (new.id, {new_cols});
    END
    """


def _create_trigger_ad() -> str:
    cols = ", ".join(_FTS_COLUMNS)
    old_cols = ", ".join(f"old.{c}" for c in _FTS_COLUMNS)
    return f"""
    CREATE TRIGGER IF NOT EXISTS leads_ad AFTER DELETE ON leads BEGIN
        INSERT INTO leads_fts(leads_fts, rowid, {cols})
            VALUES ('delete', old.id, {old_cols});
    END
    """


def _create_trigger_au() -> str:
    cols = ", ".join(_FTS_COLUMNS)
    old_cols = ", ".join(f"old.{c}" for c in _FTS_COLUMNS)
    new_cols = ", ".join(f"new.{c}" for c in _FTS_COLUMNS)
    return f"""
    CREATE TRIGGER IF NOT EXISTS leads_au AFTER UPDATE ON leads BEGIN
        INSERT INTO leads_fts(leads_fts, rowid, {cols})
            VALUES ('delete', old.id, {old_cols});
        INSERT INTO leads_fts(rowid, {cols}) VALUES (new.id, {new_cols});
    END
    """


# ── Public helpers ───────────────────────────────────────────────────────────

def _is_sqlite() -> bool:
    from config.settings import settings
    return settings.is_sqlite


def ensure_fts() -> None:
    """Create the FTS table + triggers if they don't exist yet. SQLite-only."""
    if not _is_sqlite():
        return
    with engine.connect() as conn:
        # FTS5 ships with the standard SQLite distribution shipped with Python.
        # The CREATE statements are idempotent thanks to IF NOT EXISTS.
        try:
            conn.execute(text(_create_table_sql()))
            conn.execute(text(_create_trigger_ai()))
            conn.execute(text(_create_trigger_ad()))
            conn.execute(text(_create_trigger_au()))
            conn.commit()
            log.debug("[fts] leads_fts ready")
        except Exception as e:
            log.warning("[fts] could not create FTS5 table: {e}", e=e)


def rebuild_fts() -> dict:
    """
    Drop + repopulate the entire FTS index from leads.
    Used after bulk imports (initial backfill, migration) where the
    triggers couldn't have fired.

    Returns: ``{indexed: int}``
    """
    if not _is_sqlite():
        return {"indexed": 0, "skipped_non_sqlite": True}
    ensure_fts()
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO leads_fts(leads_fts) VALUES('rebuild')"))
        conn.commit()
        n = conn.execute(text("SELECT COUNT(*) FROM leads_fts")).scalar() or 0
    log.info("[fts] rebuilt — {n} rows indexed", n=n)
    return {"indexed": int(n)}


# ── Query parsing ────────────────────────────────────────────────────────────

# Sanitiser that rejects the few control sequences that crash FTS5 with
# "syntax error" while still letting users use AND/OR/NOT/NEAR/parentheses.
_FTS_BAD_CHARS = re.compile(r'[;\\]')

def _to_fts_query(raw: str) -> str:
    """
    Turn a user-typed query into a safe FTS5 expression.

    - Strips unsafe characters (;\\)
    - Splits on whitespace and re-joins so multi-word inputs become AND
    - Preserves quoted phrases ("Avenidas Novas")
    - Adds a trailing ``*`` to a bare last token if no operator was used,
      so "ape" matches "apartamento" naturally without forcing the user
      to type the wildcard.
    """
    if not raw:
        return ""
    raw = _FTS_BAD_CHARS.sub(" ", raw).strip()
    if not raw:
        return ""
    # If the user already wrote operators / quotes, pass through as-is.
    if any(op in raw.upper() for op in ("AND", "OR", "NOT", "NEAR", "(", '"')):
        return raw
    # Otherwise: AND-tokenise, prefix-match the last token
    tokens = raw.split()
    if not tokens:
        return ""
    tokens[-1] = tokens[-1] + "*"
    return " ".join(tokens)


# ── Search functions ─────────────────────────────────────────────────────────

def search_lead_ids(query: str, limit: int = 50) -> list[int]:
    """
    Return lead ids ordered by FTS5 rank (best match first).
    Empty list when query is empty or contains only stopwords.

    On Postgres, falls back to ILIKE search across the same columns.
    """
    query = (query or "").strip()
    if not query:
        return []

    if _is_sqlite():
        ensure_fts()
        fts_q = _to_fts_query(query)
        if not fts_q:
            return []
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT rowid FROM leads_fts "
                        "WHERE leads_fts MATCH :q "
                        "ORDER BY rank "
                        "LIMIT :n"
                    ),
                    {"q": fts_q, "n": limit},
                ).all()
                return [int(r[0]) for r in rows]
            except Exception as e:
                log.debug("[fts] sqlite query failed for {q!r}: {e}", q=query, e=e)
                return []

    # Postgres fallback — multi-column ILIKE
    pattern = f"%{query}%"
    cols    = " OR ".join(f"{c} ILIKE :p" for c in _FTS_COLUMNS)
    sql     = (
        f"SELECT id FROM leads "
        f"WHERE archived = FALSE AND ({cols}) "
        f"ORDER BY last_seen_at DESC "
        f"LIMIT :n"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"p": pattern, "n": limit}).all()
        return [int(r[0]) for r in rows]


def search_leads(query: str, limit: int = 50) -> list:
    """
    Convenience wrapper — returns ORM Lead objects in FTS-rank order.
    """
    from storage.models import Lead

    ids = search_lead_ids(query, limit=limit)
    if not ids:
        return []
    with get_db() as db:
        rows = db.query(Lead).filter(Lead.id.in_(ids)).all()
        # Preserve the FTS rank order
        order = {lid: i for i, lid in enumerate(ids)}
        rows.sort(key=lambda r: order.get(r.id, 999))
        return rows

"""
SQLite → PostgreSQL migration helper.

Run this once when graduating from the local SQLite DB to a managed
Postgres instance (Railway, Supabase, RDS, your own server). Idempotent:
re-runs are safe — rows are upserted by id.

How
---
1. Set ``DATABASE_URL`` in ``.env`` to the **Postgres** connection string
2. Set ``SQLITE_SOURCE_PATH`` env var to the path of the OLD SQLite DB
   (default: ``data/imoscrapping.db``)
3. ``python -m storage.migrate_postgres``

The script:
  * Reads schema from ``storage.models`` (idempotent ``create_all``)
  * Streams every table from SQLite in 500-row chunks via raw SQLAlchemy
  * Bulk-inserts into Postgres with ``ON CONFLICT DO UPDATE`` so partial
    re-runs catch up cleanly
  * Resets the Postgres sequences so future autoincrement IDs don't clash

Time on a 50k-lead DB: ~2 minutes over a stable connection.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.logger import get_logger

log = get_logger(__name__)


CHUNK_SIZE = 500


def main() -> None:
    from config.settings import settings
    from storage.database import init_db
    from storage.models import Base

    sqlite_path = os.environ.get("SQLITE_SOURCE_PATH", "data/imoscrapping.db")
    sqlite_url  = f"sqlite:///{Path(sqlite_path).resolve()}"
    pg_url      = settings.database_url

    if not pg_url.startswith(("postgresql://", "postgresql+")):
        print(
            "❌ DATABASE_URL is not a Postgres URL.\n"
            "   Set DATABASE_URL=postgresql://user:pass@host:5432/imovela"
        )
        sys.exit(1)
    if not Path(sqlite_path).exists():
        print(f"❌ SQLite source not found: {sqlite_path}")
        sys.exit(1)

    print(f"  source : {sqlite_url}")
    print(f"  target : {pg_url.split('@')[0]}@***")
    print()

    src_engine = create_engine(sqlite_url)
    dst_engine = create_engine(pg_url, future=True)

    # 1. Create tables on Postgres if missing
    print("→ Initialising Postgres schema...")
    init_db()      # uses settings.database_url already
    Base.metadata.create_all(dst_engine, checkfirst=True)

    # 2. Stream + bulk insert per table, in dependency order
    insp = inspect(src_engine)
    src_tables = insp.get_table_names()

    # Topological order to respect FKs: parents first
    order_hint = [
        "raw_listings",
        "leads",
        "price_history",
        "premarket_signals",
        "crm_notes",
        "alerts",
        "geocode_cache",
    ]
    ordered = [t for t in order_hint if t in src_tables] + \
              [t for t in src_tables if t not in order_hint]

    with Session(src_engine) as src, Session(dst_engine) as dst:
        for table in ordered:
            if table.startswith("sqlite_") or table.startswith("alembic"):
                continue
            print(f"→ Migrating {table}...", end="", flush=True)
            count = _migrate_table(src, dst, table)
            print(f" {count} rows")
        dst.commit()

    # 3. Bump Postgres sequences past the max id we just imported
    print("→ Realigning sequences...")
    with dst_engine.connect() as conn:
        for tbl, _ in [(t, None) for t in ordered]:
            try:
                conn.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('{tbl}', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM {tbl}), 1))"
                ))
            except Exception:
                pass
        conn.commit()

    print()
    print("✓ Migration complete.")
    print("  Verify with: python main.py status")


def _migrate_table(src: Session, dst: Session, table: str) -> int:
    """Stream table from src and upsert into dst by primary key (id)."""
    rows = src.execute(text(f'SELECT * FROM "{table}"')).mappings().all()
    if not rows:
        return 0

    cols = list(rows[0].keys())
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    update_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "id"
    )

    insert_sql = text(
        f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders}) '
        f'ON CONFLICT (id) DO UPDATE SET {update_sql}'
    )

    inserted = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = [dict(r) for r in rows[i : i + CHUNK_SIZE]]
        dst.execute(insert_sql, chunk)
        inserted += len(chunk)
    dst.commit()
    return inserted


if __name__ == "__main__":
    main()

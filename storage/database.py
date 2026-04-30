"""
Database engine and session factory.
Phase 1: SQLite via SQLAlchemy.
Phase 2 migration: change DATABASE_URL in .env to PostgreSQL — no code changes needed.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from config.settings import settings
from utils.logger import get_logger

log = get_logger(__name__)

# ─── Engine ───────────────────────────────────────────────────────────────────

connect_args = {}
if settings.is_sqlite:
    connect_args = {
        "check_same_thread": False,  # allow use across threads (safe with scoped sessions)
        "timeout": 20,               # wait up to 20s for write lock
    }

engine = create_engine(
    settings.database_url,
    connect_args=connect_args,
    echo=False,             # set True to log all SQL for debugging
    pool_pre_ping=True,     # verify connection before using from pool
)

# Enable WAL mode for SQLite — allows concurrent reads during writes
if settings.is_sqlite:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


# ─── Session factory ──────────────────────────────────────────────────────────

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,   # keep objects accessible after commit
)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables if they don't exist. Safe to call repeatedly."""
    from storage.models import Base  # local import avoids circular deps
    Base.metadata.create_all(bind=engine)
    _migrate_is_demo()
    _migrate_provenance_fields()
    _migrate_contact_channels()
    _migrate_lead_classification()
    _migrate_premarket_signals()
    _migrate_recalculate_confidence()
    _migrate_name_fields()
    _migrate_birthday_field()
    _migrate_seller_profile_fields()
    _migrate_image_phash()
    log.info("Database initialised — {url}", url=settings.database_url)


def _migrate_is_demo() -> None:
    """
    Idempotent migration: adds `is_demo` column to `leads` if absent,
    then marks pre-existing demo leads (identified by seeded source URLs
    containing the word 'demo') so they are correctly classified.
    """
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE leads ADD COLUMN is_demo BOOLEAN NOT NULL DEFAULT 0"))
            conn.commit()
            log.info("Migration: added 'is_demo' column to leads")
    except Exception:
        pass  # Column already exists — safe to ignore

    # Back-fill: any lead whose seeded sources contain 'demo' is a demo lead
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE leads SET is_demo = 1 "
                "WHERE sources_json LIKE '%demo%' AND is_demo = 0"
            ))
            conn.commit()
    except Exception:
        pass


def _migrate_provenance_fields() -> None:
    """
    Idempotent migration: adds provenance/classification columns introduced in Phase 1.2.
    Each ALTER TABLE is wrapped in try/except — column-already-exists is silently ignored.
    New columns added:
        discovery_source  VARCHAR(50)   — source that first found the lead
        contact_source    VARCHAR(50)   — source that provided contact info
        contact_confidence INTEGER      — 0-100 quality score for contact info
        owner_type        VARCHAR(20)   — fsbo / agency / developer / unknown
    """
    from sqlalchemy import text

    migrations = [
        "ALTER TABLE leads ADD COLUMN discovery_source VARCHAR(50)",
        "ALTER TABLE leads ADD COLUMN contact_source VARCHAR(50)",
        "ALTER TABLE leads ADD COLUMN contact_confidence INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN owner_type VARCHAR(20)",
    ]
    for sql in migrations:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception:
            pass  # Column already exists — idempotent, safe to ignore

    # Back-fill owner_type for existing leads from is_owner flag
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE leads SET owner_type = 'fsbo' "
                "WHERE is_owner = 1 AND owner_type IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET owner_type = 'agency' "
                "WHERE is_owner = 0 AND owner_type IS NULL"
            ))
            conn.commit()
    except Exception:
        pass

    # Back-fill contact_confidence for existing leads
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE leads SET contact_confidence = 100 "
                "WHERE contact_phone IS NOT NULL AND contact_confidence = 0"
            ))
            conn.execute(text(
                "UPDATE leads SET contact_confidence = 70 "
                "WHERE contact_phone IS NULL AND contact_email IS NOT NULL AND contact_confidence = 0"
            ))
            conn.commit()
    except Exception:
        pass


def _migrate_contact_channels() -> None:
    """
    Idempotent migration: adds contact_whatsapp and contact_website columns
    introduced in Phase 1.3.

    New columns:
        contact_whatsapp  VARCHAR(50)   — WhatsApp number in canonical +351XXXXXXXXX form
        contact_website   VARCHAR(200)  — clean domain extracted from listing text (empresa.pt)
    """
    from sqlalchemy import text

    migrations = [
        "ALTER TABLE leads ADD COLUMN contact_whatsapp VARCHAR(50)",
        "ALTER TABLE leads ADD COLUMN contact_website VARCHAR(200)",
    ]
    for sql in migrations:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception:
            pass  # Column already exists — idempotent, safe to ignore


def _migrate_lead_classification() -> None:
    """
    Idempotent migration: adds lead_type and lead_quality columns (Phase 1.4).
    New columns:
        lead_type    VARCHAR(20)  — fsbo / frbo / agency_listing / developer_listing / unknown
        lead_quality VARCHAR(10)  — high / medium / low
    """
    from sqlalchemy import text

    migrations = [
        "ALTER TABLE leads ADD COLUMN lead_type VARCHAR(20)",
        "ALTER TABLE leads ADD COLUMN lead_quality VARCHAR(10)",
    ]
    for sql in migrations:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception:
            pass  # Column already exists — idempotent, safe to ignore

    # Back-fill lead_type from existing owner_type
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE leads SET lead_type = 'fsbo' "
                "WHERE owner_type = 'fsbo' AND lead_type IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET lead_type = 'agency_listing' "
                "WHERE owner_type = 'agency' AND lead_type IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET lead_type = 'developer_listing' "
                "WHERE owner_type = 'developer' AND lead_type IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET lead_type = 'unknown' "
                "WHERE lead_type IS NULL"
            ))
            conn.commit()
    except Exception:
        pass

    # Back-fill lead_quality from contact availability + lead_type
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE leads SET lead_quality = 'high' "
                "WHERE (contact_phone IS NOT NULL OR contact_whatsapp IS NOT NULL) "
                "AND lead_type IN ('fsbo', 'frbo') AND lead_quality IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET lead_quality = 'medium' "
                "WHERE (contact_email IS NOT NULL OR contact_website IS NOT NULL "
                "OR lead_type IN ('fsbo', 'frbo')) AND lead_quality IS NULL"
            ))
            conn.execute(text(
                "UPDATE leads SET lead_quality = 'low' "
                "WHERE lead_quality IS NULL"
            ))
            conn.commit()
    except Exception:
        pass


def _migrate_premarket_signals() -> None:
    """
    Idempotent migration: creates the `premarket_signals` table and any
    columns that may be missing on databases created before Phase 1.5.

    SQLAlchemy's create_all() already handles table creation for new DBs.
    This function handles the ALTER TABLE additions for existing DBs where
    the table might exist without newer columns (e.g. extra_json added later).
    """
    from sqlalchemy import text

    # Ensure the table exists (create_all already does this for new DBs;
    # this is a safety net for partial migrations)
    try:
        from storage.models import PremktSignal, Base
        PremktSignal.__table__.create(bind=engine, checkfirst=True)
    except Exception:
        pass

    # Add any columns that might be missing in an older schema
    optional_columns = [
        "ALTER TABLE premarket_signals ADD COLUMN company VARCHAR(200)",
        "ALTER TABLE premarket_signals ADD COLUMN role VARCHAR(200)",
        "ALTER TABLE premarket_signals ADD COLUMN extra_json TEXT DEFAULT '{}'",
        "ALTER TABLE premarket_signals ADD COLUMN promoted BOOLEAN NOT NULL DEFAULT 0",
    ]
    for sql in optional_columns:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception:
            pass  # Column already exists — idempotent, safe to ignore


def _migrate_recalculate_confidence() -> None:
    """
    Idempotent fix: earlier migrations set contact_confidence=100 for leads
    where contact_phone='' (empty string, not NULL) — which is falsy/useless.

    This migration recalculates confidence for leads that have confidence=100
    but no actual phone OR whatsapp value (both NULL or empty string).

    Hierarchy (matching enricher.py logic):
      phone/whatsapp present → 100  (unchanged — these are truly correct)
      email present          → 70
      website present        → 40
      name present           → 30
      nothing                → 0
    """
    from sqlalchemy import text

    fix_sql = [
        # Step 1: zero out wrongly-set confidence=100 leads (phone='', wa empty)
        # Set to name-based confidence first (will be overridden by steps below)
        """UPDATE leads SET contact_confidence = 0
           WHERE contact_confidence = 100
             AND (contact_phone    IS NULL OR contact_phone    = '')
             AND (contact_whatsapp IS NULL OR contact_whatsapp = '')""",
        # Step 2: restore to name=30 when name is present
        """UPDATE leads SET contact_confidence = 30
           WHERE contact_confidence = 0
             AND (contact_name IS NOT NULL AND contact_name != '')""",
        # Step 3: website=40 overrides name=30
        """UPDATE leads SET contact_confidence = 40
           WHERE contact_confidence IN (0, 30)
             AND (contact_website IS NOT NULL AND contact_website != '')""",
        # Step 4: email=70 overrides everything below it
        """UPDATE leads SET contact_confidence = 70
           WHERE contact_confidence IN (0, 30, 40)
             AND (contact_email IS NOT NULL AND contact_email != '')""",
    ]
    try:
        with engine.connect() as conn:
            for sql in fix_sql:
                conn.execute(text(sql))
            conn.commit()
    except Exception as e:
        log.debug("_migrate_recalculate_confidence: {e}", e=e)


def _migrate_name_fields() -> None:
    """
    Idempotent migration: adds first_name and last_name columns,
    then backfills from existing contact_name using PT name splitting.
    """
    from sqlalchemy import text

    for col in ["first_name VARCHAR(100)", "last_name VARCHAR(100)"]:
        try:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE leads ADD COLUMN {col}"))
                conn.commit()
        except Exception:
            pass

    # Backfill from contact_name where first_name is still NULL
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, contact_name FROM leads "
                "WHERE contact_name IS NOT NULL AND contact_name != '' "
                "AND first_name IS NULL"
            )).fetchall()
        if rows:
            from utils.helpers import split_pt_name
            with engine.connect() as conn:
                for row_id, name in rows:
                    first, last = split_pt_name(name)
                    conn.execute(text(
                        "UPDATE leads SET first_name = :f, last_name = :l WHERE id = :id"
                    ), {"f": first or None, "l": last or None, "id": row_id})
                conn.commit()
            log.info("Migration: backfilled first_name/last_name for {n} leads", n=len(rows))
    except Exception as e:
        log.debug("_migrate_name_fields backfill: {e}", e=e)


def _migrate_birthday_field() -> None:
    """Idempotent migration: adds birthday column to leads."""
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE leads ADD COLUMN birthday VARCHAR(50)"))
            conn.commit()
            log.info("Migration: added 'birthday' column to leads")
    except Exception:
        pass


def _migrate_image_phash() -> None:
    """
    Idempotent migration: add ``image_phash`` for the photo dedup pass.
    Stores the perceptual hash as 16-char hex; populated by the
    ``utils.image_hasher`` backfill task and consumed by photo_dedup_sweep.
    """
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE leads ADD COLUMN image_phash VARCHAR(20)"))
            conn.commit()
    except Exception:
        pass     # column already exists


def _migrate_seller_profile_fields() -> None:
    """
    Idempotent migration for the OLX seller-profile sweep.

    Adds columns used by ``pipeline.seller_profile_enricher``:
        seller_profile_url      — link to the seller's OLX profile
        seller_total_listings   — count of active ads on that profile
        seller_member_since     — raw ``member-since`` text from profile
        seller_super_flag       — TRUE when profile has ≥5 active ads
                                  (signals camouflaged-agency or repeat investor)
    """
    from sqlalchemy import text

    migrations = [
        "ALTER TABLE leads ADD COLUMN seller_profile_url VARCHAR(300)",
        "ALTER TABLE leads ADD COLUMN seller_total_listings INTEGER",
        "ALTER TABLE leads ADD COLUMN seller_member_since VARCHAR(80)",
        "ALTER TABLE leads ADD COLUMN seller_super_flag BOOLEAN NOT NULL DEFAULT 0",
    ]
    for sql in migrations:
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception:
            pass        # column already exists — idempotent


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    Provide a transactional session context.

    Usage:
        with get_db() as db:
            leads = db.query(Lead).all()
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

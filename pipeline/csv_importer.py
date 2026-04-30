"""
CSV import — bring-your-own-leads.

Operators come to Imovela carrying a years-deep contact list (Excel,
old CRM exports). This module turns those rows into Lead records that
participate in the same pipeline as scraped data: scoring, nurture,
funnel reports.

Schema flexibility
------------------
Real-world CSVs are messy: column names vary in language, casing, and
ordering. We accept any subset of the columns below, mapping common
synonyms automatically. Any unmapped column is preserved in
``sources_json`` under a ``raw_csv`` envelope so nothing is lost.

Recognised input columns (case-insensitive, accent-insensitive):
    name | nome                       → contact_name
    phone | telefone | tlm | tlmovel  → contact_phone
    email | e-mail | mail             → contact_email
    whatsapp | wa                     → contact_whatsapp
    title | titulo                    → title (description-safe fallback)
    description | descricao | notas   → description
    zone | zona | concelho            → zone
    parish | freguesia                → parish
    address | morada | endereco       → address
    price | preco | valor             → price (numeric)
    typology | tipologia              → typology
    area | area_m2 | m2               → area_m2 (numeric)
    agency | agency_name | agencia    → agency_name
    is_owner | particular | proprietario → is_owner (1/0/yes/no)
    notes | observacoes               → appended to description
    url                                → kept in sources_json

Dedup
-----
Duplicate detection happens against:
    1. contact_phone (canonical via validate_pt_phone)
    2. contact_email (lowercase)
    3. zone + typology + price (fingerprint-style)
Hits update the existing lead instead of creating a new one.

Public API
----------
import_csv(path: str | Path | bytes, source: str = "csv_import") -> dict
    Returns counts: read, created, updated, errors, skipped_invalid.
"""
from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import IO, Optional, Union

from utils.logger import get_logger

log = get_logger(__name__)


# ── Column synonym map ──────────────────────────────────────────────────────
# Each canonical key maps to a list of accent-stripped lowercase synonyms.

_SYNONYMS: dict[str, tuple[str, ...]] = {
    "contact_name":     ("name", "nome", "fullname", "full_name", "cliente"),
    "contact_phone":    ("phone", "telefone", "tel", "tlm", "telemovel",
                         "mobile", "telef", "telefon"),
    "contact_email":    ("email", "e-mail", "mail"),
    "contact_whatsapp": ("whatsapp", "wa", "wpp"),
    "title":            ("title", "titulo", "imovel"),
    "description":      ("description", "descricao", "obs", "observacoes",
                         "notes", "notas", "comentario", "comentarios"),
    "zone":             ("zone", "zona", "concelho", "municipio"),
    "parish":           ("parish", "freguesia"),
    "address":          ("address", "morada", "endereco", "rua"),
    "price":            ("price", "preco", "valor", "valor_pedido"),
    "typology":         ("typology", "tipologia", "tipo"),
    "area_m2":          ("area", "area_m2", "m2", "metros", "areautil"),
    "agency_name":      ("agency", "agency_name", "agencia", "imobiliaria",
                         "imobiliária"),
    "is_owner":         ("is_owner", "owner", "particular", "proprietario",
                         "proprietário"),
    "url":              ("url", "link", "anuncio"),
}


def _norm_header(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (s or "").lower())
    ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", ascii_)


def _build_column_map(headers: list[str]) -> dict[str, str]:
    """Match every CSV header to a canonical key when possible."""
    mapping: dict[str, str] = {}
    norm_headers = {h: _norm_header(h) for h in headers}
    for canonical, synonyms in _SYNONYMS.items():
        targets = {_norm_header(s) for s in synonyms}
        for original, norm in norm_headers.items():
            if norm in targets and canonical not in mapping.values():
                mapping[original] = canonical
                break
    return mapping


# ── Field parsers ────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[^\d,\.]")
_AREA_RE  = re.compile(r"[^\d\.,]")


def _parse_price(raw: str) -> Optional[float]:
    """
    Parse PT-style prices: '250.000 EUR' = 250 000 (period = thousands).
    '250,50' = 250.50 (comma = decimal). Mixed '250.000,75' is also handled.
    """
    if not raw:
        return None
    s = _PRICE_RE.sub("", str(raw)).strip()
    if not s:
        return None
    has_comma = "," in s
    has_dot   = "." in s
    if has_comma and has_dot:
        # Last separator before digits is the decimal one (PT/EU style)
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_comma:
        # Comma alone in PT means decimal (e.g. "250,50")
        s = s.replace(",", ".")
    elif has_dot:
        # Period alone — heuristic: when the chunk after the LAST period
        # is exactly 3 digits, treat as thousands separator.
        last_chunk = s.split(".")[-1]
        if len(last_chunk) == 3 and last_chunk.isdigit():
            s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_area(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = _AREA_RE.sub("", str(raw)).replace(",", ".")
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _parse_bool(raw: str) -> Optional[bool]:
    if not raw:
        return None
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "sim", "y", "s", "particular", "owner"):
        return True
    if s in ("0", "false", "no", "nao", "não", "n", "agencia", "agência"):
        return False
    return None


# ── Main import function ─────────────────────────────────────────────────────

def import_csv(
    payload: Union[str, Path, bytes, IO],
    source: str = "csv_import",
) -> dict:
    """
    Read ``payload`` (path, bytes, or file-like) and upsert each row into
    the leads table. Returns stats:
        read, created, updated, skipped_invalid, errors
    """
    from utils.phone import validate_pt_phone
    from storage.database import get_db
    from storage.models import Lead

    stats = {
        "read":             0,
        "created":          0,
        "updated":          0,
        "skipped_invalid":  0,
        "errors":           0,
    }

    # Open the payload as a text stream
    if isinstance(payload, bytes):
        stream = io.StringIO(payload.decode("utf-8-sig", errors="replace"))
    elif isinstance(payload, (str, Path)) and Path(str(payload)).exists():
        stream = open(payload, "r", encoding="utf-8-sig", errors="replace")
    elif isinstance(payload, (str, Path)):
        # Treat as raw CSV text
        stream = io.StringIO(str(payload))
    else:
        stream = payload    # already a file-like

    try:
        sample = stream.read(8192)
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel

        reader = csv.DictReader(stream, dialect=dialect)
        if not reader.fieldnames:
            stats["errors"] = 1
            return stats

        column_map = _build_column_map(list(reader.fieldnames))
        log.info(
            "[csv_import] column map: {m}", m=column_map,
        )

        with get_db() as db:
            for raw_row in reader:
                stats["read"] += 1
                try:
                    canonical = _build_canonical(raw_row, column_map)
                    if not _row_is_actionable(canonical):
                        stats["skipped_invalid"] += 1
                        continue

                    # Normalise phone if present
                    phone = canonical.get("contact_phone") or ""
                    if phone:
                        result = validate_pt_phone(phone)
                        if result.valid:
                            canonical["contact_phone"] = result.canonical
                            canonical["phone_type"]    = result.phone_type

                    # Try to find an existing lead
                    existing = _find_existing(db, canonical)
                    if existing:
                        _apply_update(existing, canonical, source, raw_row)
                        stats["updated"] += 1
                    else:
                        _create_lead(db, canonical, source, raw_row)
                        stats["created"] += 1
                except Exception as e:
                    log.debug("[csv_import] row #{n}: {e}", n=stats["read"], e=e)
                    stats["errors"] += 1

            db.commit()

    finally:
        if hasattr(stream, "close") and not isinstance(payload, (bytes,)):
            try:
                stream.close()
            except Exception:
                pass

    log.info(
        "[csv_import] {r} read · +{c} created · ↑{u} updated · "
        "✗{e} errors · {s} skipped",
        r=stats["read"], c=stats["created"], u=stats["updated"],
        e=stats["errors"], s=stats["skipped_invalid"],
    )
    return stats


# ── Internals ────────────────────────────────────────────────────────────────

def _build_canonical(row: dict, column_map: dict[str, str]) -> dict:
    """Rewrite raw row keys to canonical keys + parse numeric fields."""
    out: dict = {}
    for orig, canonical in column_map.items():
        val = (row.get(orig) or "").strip()
        if not val:
            continue
        if canonical == "price":
            out[canonical] = _parse_price(val)
        elif canonical == "area_m2":
            out[canonical] = _parse_area(val)
        elif canonical == "is_owner":
            parsed = _parse_bool(val)
            if parsed is not None:
                out[canonical] = parsed
        else:
            out[canonical] = val
    return out


def _row_is_actionable(c: dict) -> bool:
    """Return True if the row carries enough data to be usable as a lead."""
    has_contact = bool(c.get("contact_phone") or c.get("contact_email"))
    has_subject = bool(c.get("title") or c.get("zone") or c.get("address"))
    return has_contact or has_subject


def _find_existing(db, c: dict):
    """Look up an existing Lead matching by phone, email, or fingerprint."""
    from storage.models import Lead

    if c.get("contact_phone"):
        hit = db.query(Lead).filter(Lead.contact_phone == c["contact_phone"]).first()
        if hit:
            return hit
    if c.get("contact_email"):
        hit = db.query(Lead).filter(
            Lead.contact_email == c["contact_email"].lower()
        ).first()
        if hit:
            return hit
    # Fingerprint-style match: same zone + typology + ±5% price
    if c.get("zone") and c.get("typology") and c.get("price"):
        lo = float(c["price"]) * 0.95
        hi = float(c["price"]) * 1.05
        hit = (
            db.query(Lead)
            .filter(Lead.zone == c["zone"])
            .filter(Lead.typology == c["typology"])
            .filter(Lead.price.between(lo, hi))
            .first()
        )
        if hit:
            return hit
    return None


def _apply_update(existing, c: dict, source: str, raw_row: dict) -> None:
    """Fill missing fields on an existing lead. Never overwrite non-empty values."""
    fields = (
        "contact_name", "contact_phone", "contact_email", "contact_whatsapp",
        "title", "description", "zone", "parish", "address",
        "price", "typology", "area_m2", "agency_name",
    )
    for f in fields:
        if not c.get(f):
            continue
        current = getattr(existing, f, None)
        if not current:
            setattr(existing, f, c[f])

    if c.get("is_owner") is not None and existing.is_owner is False:
        existing.is_owner = c["is_owner"]

    # Append the raw row to sources_json as an audit trail
    sources = existing.sources
    sources.append({
        "source":  source,
        "url":     c.get("url") or "",
        "seen_at": datetime.utcnow().isoformat(),
        "raw_csv": {k: v for k, v in raw_row.items() if v},
    })
    existing.sources = sources


def _create_lead(db, c: dict, source: str, raw_row: dict) -> None:
    """Create a new Lead row from canonical CSV data."""
    from storage.models import Lead

    lead = Lead(
        title          = c.get("title") or "Importado via CSV",
        typology       = c.get("typology"),
        price          = c.get("price"),
        area_m2        = c.get("area_m2"),
        zone           = c.get("zone"),
        parish         = c.get("parish"),
        address        = c.get("address"),
        description    = c.get("description"),
        contact_name   = c.get("contact_name"),
        contact_phone  = c.get("contact_phone"),
        contact_email  = (c.get("contact_email") or "").lower() or None,
        contact_whatsapp = c.get("contact_whatsapp"),
        agency_name    = c.get("agency_name"),
        is_owner       = bool(c.get("is_owner")),
        owner_type     = "fsbo" if c.get("is_owner") else "agency",
        discovery_source = source,
        contact_source = source if (c.get("contact_phone") or c.get("contact_email")) else None,
        sources_json   = json.dumps(
            [{
                "source":  source,
                "url":     c.get("url") or "",
                "seen_at": datetime.utcnow().isoformat(),
                "raw_csv": {k: v for k, v in raw_row.items() if v},
            }],
            ensure_ascii=False, default=str,
        ),
        first_seen_at  = datetime.utcnow(),
        last_seen_at   = datetime.utcnow(),
        crm_stage      = "novo",
        score          = 0,
        score_label    = "COLD",
    )
    db.add(lead)

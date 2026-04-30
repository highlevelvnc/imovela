"""
Amenity tag extractor — keyword-based property feature detection.

Why
---
Real-estate buyers filter by features ("piscina", "garagem", "varanda") far
more often than by typology+zone alone. Adding a tag column lets the
operator answer "which T2 in Lisboa has a pool AND a garage?" with a
single SQL ``LIKE``, without manual triage of descriptions.

Approach
--------
A curated dictionary maps each amenity to a list of trigger phrases (with
diacritic-insensitive matching). When the lead's title+description
contains any phrase, the canonical tag is added. Output is a comma-
separated string — easy to filter via ``LIKE`` in SQLite or trigram
search in Postgres.

Scope deliberately narrow: only features Portuguese sellers reliably
mention in copy. Anything ML-grade goes into the owner_classifier path,
not here.

Public API
----------
extract_amenities(text: str) -> list[str]
    Returns canonical tag list (sorted). Empty list when nothing matched.

format_tags(tags: list[str]) -> str
    Comma-joined string ready for storage. Empty string when no tags.

backfill_amenity_tags(limit: int) -> dict
    Walk leads with title/description but no amenity_tags; populate.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Iterable

from utils.logger import get_logger

log = get_logger(__name__)


# Curated dictionary — keep terms lowercase, accent-stripped, in PT.
# Multi-word phrases are matched as substrings of the normalised text.
_AMENITY_RULES: dict[str, tuple[str, ...]] = {
    "piscina":      ("piscina", "piscinas"),
    "garagem":      ("garagem", "lugar garagem", "boxe", "boxes",
                     "lugar de estacionamento", "parqueamento", "parking"),
    "varanda":      ("varanda", "varandas"),
    "terraco":      ("terraco", "terracos", "rooftop"),
    "jardim":       ("jardim", "jardins", "espaco verde"),
    "suite":        ("suite", "suites", "suíte"),
    "vista_mar":    ("vista mar", "vista para o mar", "vista de mar",
                     "frente mar", "primeira linha"),
    "vista_rio":    ("vista rio", "vista para o rio", "frente rio"),
    "elevador":     ("elevador", "ascensor"),
    "remodelado":   ("remodelado", "totalmente remodelado", "renovado",
                     "renovada", "remodelacao", "novo total", "como novo"),
    "para_recuperar": ("para recuperar", "para reabilitar", "p/recuperar",
                       "para restauro", "para obras", "reabilitacao"),
    "aluguer_curta": ("alojamento local", "al ", "rentabilidade",
                      "investimento", "yield", "rendimento garantido"),
    "ar_condicionado": ("ar condicionado", "a/c", "climatizacao", "clima"),
    "lareira":      ("lareira", "salamandra", "recuperador de calor"),
    "arrecadacao":  ("arrecadacao", "arrumos"),
    "cozinha_equipada": ("cozinha equipada", "cozinha totalmente equipada",
                         "cozinha c equipamentos", "totalmente equipada"),
    "duplex":       ("duplex", "triplex"),
    "moradia":      ("moradia", "vivenda", "casa unifamiliar"),
    "novo_construcao": ("novo", "construcao nova", "primeira ocupacao",
                        "novo de construcao", "novo edificio", "obra nova"),
    "metro_perto":  ("perto do metro", "metro a", "estacao de metro",
                     "junto ao metro", "metro proximo", "linha do metro"),
    "centro":       ("centro historico", "centro da cidade", "baixa de"),
    "praia_perto":  ("perto da praia", "junto a praia", "praia a",
                     "5 minutos da praia", "10 min da praia"),
    "preco_negociavel": ("preco negociavel", "negociavel", "aceitamos propostas",
                         "aceito proposta", "abertos a propostas",
                         "abre proposta"),
    "venda_urgente": ("venda urgente", "vende-se urgente", "urge venda",
                      "preciso vender", "necessidade de venda"),
}


def _norm(text: str) -> str:
    """Lowercase + strip accents + collapse whitespace."""
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_).strip()


# ── Public API ───────────────────────────────────────────────────────────────

def extract_amenities(text: str) -> list[str]:
    """Return matching canonical amenity tags for ``text``."""
    norm = _norm(text)
    if not norm:
        return []
    out: set[str] = set()
    for tag, phrases in _AMENITY_RULES.items():
        for phrase in phrases:
            # Each phrase is already accent-stripped; we wrap with word
            # boundaries so "AL " doesn't match "alarme".
            if " " in phrase:
                if phrase in norm:
                    out.add(tag)
                    break
            else:
                if re.search(rf"\b{re.escape(phrase)}\b", norm):
                    out.add(tag)
                    break
    return sorted(out)


def format_tags(tags: Iterable[str]) -> str:
    """Comma-join tags for DB storage. Empty string when iterable is empty."""
    return ",".join(sorted({t for t in (tags or []) if t}))


def parse_tags(raw: str) -> list[str]:
    """Inverse of format_tags: split a stored string back into a list."""
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


# ── Bulk backfill ────────────────────────────────────────────────────────────

def backfill_amenity_tags(limit: int = 1000) -> dict:
    """
    Walk leads where ``amenity_tags`` is null but title/description has
    content; compute and persist tags. Idempotent.
    """
    from sqlalchemy import or_, select
    from storage.database import get_db
    from storage.models import Lead

    stats = {"considered": 0, "tagged": 0, "skipped_empty": 0}

    with get_db() as db:
        leads = db.execute(
            select(Lead.id, Lead.title, Lead.description)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
            .where(or_(Lead.amenity_tags.is_(None), Lead.amenity_tags == ""))
            .where(or_(Lead.title.isnot(None), Lead.description.isnot(None)))
            .limit(limit)
        ).all()
        stats["considered"] = len(leads)

        for lid, title, desc in leads:
            text = " ".join(filter(None, [title, desc or ""]))
            tags = extract_amenities(text)
            if not tags:
                stats["skipped_empty"] += 1
                continue
            db.query(Lead).filter(Lead.id == lid).update(
                {"amenity_tags": format_tags(tags)}
            )
            stats["tagged"] += 1
        db.commit()

    log.info(
        "[amenity_tags] backfill — tagged={t} skipped_empty={s} from {n}",
        t=stats["tagged"], s=stats["skipped_empty"], n=stats["considered"],
    )
    return stats

"""
Shared utility functions used across the project.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Optional

from slugify import slugify


# ─── Text ─────────────────────────────────────────────────────────────────────

def slugify_text(text: str) -> str:
    """Return a URL-safe lowercase slug from any text."""
    return slugify(text or "", allow_unicode=False, separator="-")


def clean_text(text: str) -> str:
    """Strip excess whitespace and normalize unicode."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    return re.sub(r"\s+", " ", text).strip()


# ─── Fingerprint / Deduplication ─────────────────────────────────────────────

def fingerprint(*parts: Optional[str | float | int]) -> str:
    """
    Build a stable 16-char hex fingerprint from the given parts.
    Used to detect duplicate listings across sources.
    """
    combined = "|".join(str(p).strip().lower() if p is not None else "" for p in parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


# ─── Price ────────────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d\s.,]+")

def parse_price(raw: str) -> Optional[float]:
    """
    Parse Portuguese price strings like '280.000 €', '195 000€', '195,000'.
    Returns float or None if unparseable.
    """
    if not raw:
        return None
    raw = raw.replace("\xa0", " ")
    match = _PRICE_RE.search(raw)
    if not match:
        return None
    s = match.group().replace(" ", "").replace(".", "").replace(",", ".")
    # Handle case where comma is thousands separator (195,000 → 195000)
    if s.count(".") > 1:
        s = s.replace(".", "", s.count(".") - 1)
    try:
        val = float(s)
        # Sanity-check: real estate in PT is typically 10k–5M
        if 5_000 < val < 10_000_000:
            return val
        return None
    except ValueError:
        return None


# ─── Area ─────────────────────────────────────────────────────────────────────

_AREA_RE = re.compile(r"(\d+[\.,]?\d*)\s*m²?", re.IGNORECASE)

def parse_area(raw: str) -> Optional[float]:
    """Parse area strings like '85 m²', '120m2', '85,5 m²'."""
    if not raw:
        return None
    match = _AREA_RE.search(raw)
    if not match:
        return None
    s = match.group(1).replace(",", ".")
    try:
        val = float(s)
        if 5 < val < 50_000:
            return val
        return None
    except ValueError:
        return None


# ─── Phone ────────────────────────────────────────────────────────────────────

def clean_phone(raw: str) -> Optional[str]:
    """
    Normalise and validate a Portuguese phone number.
    Returns "+351XXXXXXXXX" or None.

    Delegates to utils.phone.validate_pt_phone for full validation
    (suspicious sequences, premium numbers, type classification).
    """
    from utils.phone import validate_pt_phone
    result = validate_pt_phone(raw or "")
    return result.canonical if result.valid else None


# ─── Typology ─────────────────────────────────────────────────────────────────

_TYPOLOGY_RE = re.compile(r"\bT\s*([0-9])\b", re.IGNORECASE)
_STUDIO_RE   = re.compile(r"\bestúdio|studio|kitchenette\b", re.IGNORECASE)
_VILLA_RE    = re.compile(r"\bmoradia|vivenda|villa|v\s*[1-9]\b", re.IGNORECASE)
_LAND_RE     = re.compile(r"\bterreno|lote\b", re.IGNORECASE)
_COMMERCIAL_RE = re.compile(r"\bcomercial|loja|escritório|armazém\b", re.IGNORECASE)

def extract_typology(text: str) -> str:
    """Extract standardised typology from listing title/description."""
    if not text:
        return "Desconhecido"
    if _LAND_RE.search(text):
        return "Terreno"
    if _COMMERCIAL_RE.search(text):
        return "Comercial"
    if _VILLA_RE.search(text):
        return "Moradia"
    if _STUDIO_RE.search(text):
        return "T0"
    match = _TYPOLOGY_RE.search(text)
    if match:
        n = int(match.group(1))
        return f"T{min(n, 5)}+" if n >= 5 else f"T{n}"
    return "Desconhecido"


# ─── Zone normalisation ───────────────────────────────────────────────────────

_ZONE_MAP: dict[str, str] = {
    # ── Lisboa — city + all civil parishes ───────────────────────────────────
    "lisboa": "Lisboa",
    "lisbonne": "Lisboa",
    "lisbon": "Lisboa",
    "alcântara": "Lisboa",
    "alcantara": "Lisboa",
    "arroios": "Lisboa",
    "avenidas novas": "Lisboa",
    "beato": "Lisboa",
    "belém": "Lisboa",
    "belem": "Lisboa",
    "benfica": "Lisboa",
    "campo de ourique": "Lisboa",
    "carnide": "Lisboa",
    "estrela": "Lisboa",
    "lumiar": "Lisboa",
    "marvila": "Lisboa",
    "misericórdia": "Lisboa",
    "misericordia": "Lisboa",
    "olivais": "Lisboa",
    "parque das nações": "Lisboa",
    "parque das nacoes": "Lisboa",
    "penha de france": "Lisboa",
    "santa clara": "Lisboa",
    "santa maria maior": "Lisboa",
    "são domingos de benfica": "Lisboa",
    "sao domingos de benfica": "Lisboa",
    "são vicente": "Lisboa",
    "sao vicente": "Lisboa",
    "areeiro": "Lisboa",
    "alvalade": "Lisboa",
    "moscavide": "Lisboa",
    "portela": "Lisboa",
    "odivelas": "Lisboa",
    "amadora": "Lisboa",
    "loures": "Lisboa",
    "sacavém": "Lisboa",
    "sacavem": "Lisboa",
    "oriente": "Lisboa",
    # ── Cascais — municipality + sub-zones ────────────────────────────────────
    "cascais": "Cascais",
    "estoril": "Cascais",
    "alcabideche": "Cascais",
    "são domingos de rana": "Cascais",
    "sao domingos de rana": "Cascais",
    "parede": "Cascais",
    "carcavelos": "Cascais",
    "monte estoril": "Cascais",
    "birre": "Cascais",
    "trajouce": "Cascais",
    # ── Sintra — municipality + sub-zones ─────────────────────────────────────
    "sintra": "Sintra",
    "queluz": "Sintra",
    "algueirão": "Sintra",
    "algueirao": "Sintra",
    "mem martins": "Sintra",
    "rio de mouro": "Sintra",
    "agualva": "Sintra",
    "mira-sintra": "Sintra",
    "mira sintra": "Sintra",
    "cacém": "Sintra",
    "cacem": "Sintra",
    "são marcos": "Sintra",
    "sao marcos": "Sintra",
    "colares": "Sintra",
    "montelavar": "Sintra",
    "pêro pinheiro": "Sintra",
    "pero pinheiro": "Sintra",
    "mina de água": "Sintra",
    "mina de agua": "Sintra",
    "terrugem": "Sintra",
    # ── Almada ────────────────────────────────────────────────────────────────
    "almada": "Almada",
    "costa da caparica": "Almada",
    "caparica": "Almada",
    "cacilhas": "Almada",
    "pragal": "Almada",
    "charneca de caparica": "Almada",
    "sobreda": "Almada",
    "cova da piedade": "Almada",
    "laranjeiro": "Almada",
    "feijó": "Almada",
    "feijo": "Almada",
    "laranjeiro e feijó": "Almada",
    "laranjeiro e feijo": "Almada",
    "monte de caparica": "Almada",
    "trafaria": "Almada",
    "charneca": "Almada",
    # ── Seixal ────────────────────────────────────────────────────────────────
    "seixal": "Seixal",
    "amora": "Seixal",
    "fernão ferro": "Seixal",
    "fernao ferro": "Seixal",
    "corroios": "Seixal",
    "aldeia de paio pires": "Seixal",
    "arrentela": "Seixal",
    "quinta do conde": "Seixal",
    "torre da marinha": "Seixal",
    "paio pires": "Seixal",
    "fogueteiro": "Seixal",
    "cruz de pau": "Seixal",
    # ── Sesimbra ──────────────────────────────────────────────────────────────
    "sesimbra": "Sesimbra",
    "santana": "Sesimbra",
    "castelo": "Sesimbra",
    "zambujal": "Sesimbra",
    "quinta do conde": "Sesimbra",
    "azeitão": "Sesimbra",
    "azeitao": "Sesimbra",
    # ── Cascais — freguesias em falta ─────────────────────────────────────────
    "oeiras": "Cascais",
    "algés": "Cascais",
    "alges": "Cascais",
    "linda-a-velha": "Cascais",
    "linda a velha": "Cascais",
    "cruz quebrada": "Cascais",
    "dafundo": "Cascais",
    "algés, linda-a-velha e cruz quebrada-dafundo": "Cascais",
    "oeiras e são julião da barra": "Cascais",
    "oeiras e s.julião da barra": "Cascais",
    "paço de arcos": "Cascais",
    "paco de arcos": "Cascais",
    "caxias": "Cascais",
    "carnaxide": "Cascais",
    "barcarena": "Cascais",
    "porto salvo": "Cascais",
    # ── Sintra — freguesias em falta ──────────────────────────────────────────
    "massamá": "Sintra",
    "massama": "Sintra",
    "monte abraão": "Sintra",
    "monte abraao": "Sintra",
    "massamá e monte abraão": "Sintra",
    "massama e monte abraao": "Sintra",
    "algueirão-mem martins": "Sintra",
    "algueirão mem martins": "Sintra",
    "algueirao-mem martins": "Sintra",
    "belas": "Sintra",
    "casal de cambra": "Sintra",
    "são pedro de penaferrim": "Sintra",
    "sao pedro de penaferrim": "Sintra",
    # ── Lisboa — freguesias em falta ──────────────────────────────────────────
    "águas livres": "Lisboa",
    "aguas livres": "Lisboa",
    "venteira": "Lisboa",
    "alfragide": "Lisboa",
    "damaia": "Lisboa",
    "reboleira": "Lisboa",
    "buraca": "Lisboa",
    "brandoa": "Lisboa",
    "pontinha": "Lisboa",
    "pontinha e famões": "Lisboa",
    "pontinha e famoes": "Lisboa",
    "famões": "Lisboa",
    "famoes": "Lisboa",
    "santo antónio dos cavaleiros": "Lisboa",
    "santo antonio dos cavaleiros": "Lisboa",
    "santo antónio dos cavaleiros e frielas": "Lisboa",
    "frielas": "Lisboa",
    "santa iria de azóia": "Lisboa",
    "santa iria de azoia": "Lisboa",
    "são joão da talha": "Lisboa",
    "sao joao da talha": "Lisboa",
    "bobadela": "Lisboa",
    "prior velho": "Lisboa",
    "camarate": "Lisboa",
    "unhos": "Lisboa",
    "apelação": "Lisboa",
    "apelacao": "Lisboa",
    "vila franca de xira": "Lisboa",
    "alverca": "Lisboa",
    "alverca do ribatejo": "Lisboa",
    "forte da casa": "Lisboa",
    "vialonga": "Lisboa",
    "póvoa de santa iria": "Lisboa",
    "povoa de santa iria": "Lisboa",
    "azambuja": "Lisboa",
    "penha de frança": "Lisboa",
    "mafra": "Lisboa",
    "ericeira": "Lisboa",
}

# Garbage zone strings to discard (Custojusto spam, broken data)
_ZONE_GARBAGE = frozenset({
    "gostaria de saber mais",
    "gostaria de saber mais?",
    "contacte-nos",
    "ver mais",
    "clique aqui",
    "none",
    "null",
    "n/a",
    "undefined",
})

def normalise_zone(raw: str) -> str:
    """Map raw location strings to canonical zone names."""
    if not raw:
        return "Outra"
    key = raw.strip().lower()
    # Reject garbage strings
    if key in _ZONE_GARBAGE or len(key) < 3:
        return "Outra"
    for fragment, zone in _ZONE_MAP.items():
        if fragment in key:
            return zone
    return raw.strip().title()


def extract_parish(location_raw: str) -> str:
    """
    Extract the most specific sub-zone (parish/locality) from a raw location string.
    Uses the same _ZONE_MAP keys as normalise_zone — returns the longest matching
    fragment title-cased (e.g. "Carcavelos", "Parede", "Costa da Caparica").
    Returns empty string if no match is found.
    """
    if not location_raw:
        return ""
    key = location_raw.strip().lower()
    best = ""
    for fragment in _ZONE_MAP:
        if fragment in key and len(fragment) > len(best):
            best = fragment
    return best.title() if best else ""


# ─── Keywords / Urgency ───────────────────────────────────────────────────────

URGENCY_KEYWORDS: list[tuple[str, int]] = [
    # (regex pattern, points)
    (r"\burgente\b|\bvendo urgente\b|\bpreciso vender\b|\bvendo r[aá]pido\b", 25),
    (r"\bheran[çc]a\b|\bpartilha\b|\bdivorc[io]\w*\b|\bsepara[çc][aã]o\b", 20),
    (r"\bbanco\b|\bexecu[çc][aã]o\b|\bpenhora\b|\bpig[nñ]or\w*\b", 20),
    (r"\bemigra[çc][aã]o?\b|\bmuda[nñ]ça de pa[ií]s\b|\bpreciso sair\b", 15),
    (r"\bprecisa de obras\b|\bpara remodelar\b|\bpara renovar\b|\bruina\b", 10),
    (r"\bnegocia[çc][aã]o?\b|\bflex[ií]vel\b|\baceito propostas\b", 8),
]

def detect_urgency(text: str) -> tuple[int, list[str]]:
    """
    Scan description for urgency signals.
    Returns (max_points, matched_patterns).
    """
    if not text:
        return 0, []
    text_lower = text.lower()
    matched: list[str] = []
    max_pts = 0
    for pattern, pts in URGENCY_KEYWORDS:
        if re.search(pattern, text_lower):
            matched.append(pattern)
            max_pts = max(max_pts, pts)
    return max_pts, matched


AGENCY_KEYWORDS = re.compile(
    r"\bimobili[aá]ria\b|\bagência\b|\bconsultora\b|\bmediad\w+\b|"
    r"\bera\b|\bremax\b|\bcentury\b|\bkw\b|\bkeller\w*\b|\bsotheby\b",
    re.IGNORECASE,
)

def is_owner_listing(title: str, description: str, contact_name: str = "") -> bool:
    """Heuristic: returns True if the listing appears to be from a direct owner."""
    combined = f"{title} {description} {contact_name}"
    return not bool(AGENCY_KEYWORDS.search(combined))


# ─── Name splitting (Portuguese conventions) ─────────────────────────────────

_PT_PARTICLES = frozenset({"da", "de", "do", "dos", "das", "e", "d'"})

def split_pt_name(full_name: str) -> tuple[str, str]:
    """
    Split a Portuguese full name into (first_name, last_name).

    Rules:
      - Particles (da, de, do, dos, das, e) stay attached to the surname.
      - Single-word names → (nome, "")
      - "João Silva"          → ("João", "Silva")
      - "Maria da Costa"      → ("Maria", "da Costa")
      - "Ana dos Santos Lima" → ("Ana", "dos Santos Lima")
      - "Pedro"               → ("Pedro", "")
      - "" / None             → ("", "")
    """
    if not full_name or not full_name.strip():
        return ("", "")
    parts = full_name.strip().split()
    if len(parts) == 1:
        return (parts[0], "")
    # First name is always the first token
    first = parts[0]
    # Everything from the second token onward is the surname
    # (includes particles like "da", "de", "dos")
    last = " ".join(parts[1:])
    return (first, last)

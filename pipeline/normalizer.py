"""
Normalizer — transforms raw scraped data into a canonical Lead dict.
Each source has its own normalization logic.

Extended (Phase 1.2):
  - Dispatches to sapo / custojusto normalizers
  - Classifies owner_type: fsbo / agency / developer / unknown
  - Extracts contact from free-text description (phone regex)
  - Tracks contact_source separately from discovery_source
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from utils.helpers import (
    clean_text, clean_phone, extract_typology, extract_parish,
    is_owner_listing, normalise_zone, parse_area, parse_price,
)
from utils.phone import validate_pt_phone, extract_phone_from_text as _extract_phone_validated
from utils.logger import get_logger

log = get_logger(__name__)


def _resolve_phone(*candidates: Optional[str]) -> tuple[Optional[str], str, int]:
    """
    Try each candidate phone string in order, return first valid phone as
    (canonical, phone_type, contact_confidence).

    Accepts raw strings from scraper fields or text-extracted candidates.
    Returns (None, "unknown", 0) if no valid phone found in any candidate.
    """
    for raw in candidates:
        if not raw:
            continue
        result = validate_pt_phone(raw)
        if result.valid:
            return result.canonical, result.phone_type, result.confidence
    return None, "unknown", 0

# ── Owner classification helpers ─────────────────────────────────────────────

_AGENCY_RE = re.compile(
    r"\bimobili[aá]ria\b|\bagência\b|\bconsultora\b|\bmediad\w+\b|"
    r"\bera\b|\bremax\b|\bcentury\b|\bkw\b|\bkeller\w*\b|\bsotheby\b|"
    r"\bpromot\w+\b|\bcomercializ\w+\b|\bimóvel\s+na\s+mão\b",
    re.IGNORECASE,
)
_DEVELOPER_RE = re.compile(
    r"\bpromotor\b|\bpromotora\b|\bconstru[çc]\w+\b|\bempreiteiro\b|"
    r"\bloteamento\b|\bcondomin\w+\s+nov\w+\b|\bobra\s+nova\b|\bentrega\s+\d{4}\b",
    re.IGNORECASE,
)
_FSBO_RE = re.compile(
    r"\bparticular\b|\bpropriet[aá]rio\b|\bdono\b|\bsem\s+agência\b|"
    r"\bsem\s+media[çc]\w+\b|\bvendo\s+directamente\b|\bvendo\s+diretamente\b|"
    r"\bcontacto\s+directo\b|\bcontacto\s+direto\b|"
    r"\boferta\s+privada\b|"           # Imovirtual label for private/owner listings
    r"\bsem\s+comiss[aã]o\b|"         # no commission — direct owner signal
    r"\bsem\s+mediadora\b|"            # no broker
    r"\bpropriet[aá]rio\s+vende\b|"   # owner sells
    r"\bpor\s+conta\s+pr[oó]pria\b|"  # on own account
    r"\bdiret[ao]\s+do\s+(dono|proprietário)\b",  # direct from owner
    re.IGNORECASE,
)

# Rental keywords — used to detect FRBO (For Rent By Owner) listings
_RENTAL_RE = re.compile(
    r"\barrendar\b|\barrendamento\b|\bpara\s+arrendar\b|"
    r"\balugar\b|\baluguer\b|\baluguel\b|\barrendado\b|"
    r"\brenda\s+mensal\b|\bpor\s+m[eê]s\b|\bmensal\b",
    re.IGNORECASE,
)


def classify_lead_type(owner_type: str, title: str, description: str, url: str = "", source: str = "") -> str:
    """
    Derive lead_type from owner_type + rental keyword detection.

    Returns one of:
      'fsbo'             — For Sale By Owner
      'frbo'             — For Rent By Owner (owner ad on a rental)
      'agency_listing'   — listed by a real-estate agency
      'developer_listing'— new development / promotor
      'active_owner'     — OLX marketplace seller (non-real-estate product)
      'unknown'          — cannot determine
    """
    # Marketplace / signal sources → always "active_owner" (classified by source, not keywords)
    if source in ("olx_marketplace", "standvirtual", "linkedin"):
        return "active_owner"

    is_rental = bool(_RENTAL_RE.search(f"{title} {description} {url}"))

    if owner_type == "fsbo":
        return "frbo" if is_rental else "fsbo"
    if owner_type == "agency":
        return "agency_listing"
    if owner_type == "developer":
        return "developer_listing"
    # unknown — check for rental signal
    return "frbo" if is_rental else "unknown"


# ── Property type extraction ──────────────────────────────────────────────────
# Maps text signals to a semantic property category.
# URL-based signals (from Imovirtual's path) are most reliable.
# Text signals are a fallback for sources without structured URLs.

_PROPERTY_TYPE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bapartamento\b|\bflat\b|\best[uú]dio\b|\bloft\b|\bduplex\b|\bpiso\b", re.I), "Apartamento"),
    (re.compile(r"\bmoradia\b|\bvivenda\b|\btownhouse\b|\bcasa\s+(de\s+campo|geminada|isolada)\b", re.I), "Moradia"),
    (re.compile(r"\bterreno\b|\blote\b|\blote\s+urbano\b", re.I), "Terreno"),
    (re.compile(r"\bquinta\b|\bherdade\b", re.I), "Quinta"),
    (re.compile(r"\bescrit[oó]rio\b|\bgabinete\b|\bsala\s+comercial\b", re.I), "Escritório"),
    (re.compile(r"\barmazém\b|\barm[ae]z[ae]m\b|\bself.?storage\b", re.I), "Armazém"),
    (re.compile(r"\bloja\b(?!\s+de\s+m[oó]veis)", re.I), "Loja"),  # "loja de móveis" → skip (it's a product)
    (re.compile(r"\bgaragem\b|\bparking\b|\bestacionamento\b", re.I), "Garagem"),
]


def extract_property_type(title: str, description: str = "", url: str = "") -> str:
    """
    Extract semantic property type from text and/or URL.

    URL-path detection (Imovirtual) is checked first — most reliable.
    Falls back to text keyword matching.

    Returns one of: Apartamento, Moradia, Terreno, Quinta, Escritório, Armazém,
                    Loja, Garagem — or "" if no clear signal.
    """
    url_lower = (url or "").lower()

    # URL-path-based detection (Imovirtual uses /comprar/apartamento/, /comprar/moradia/, etc.)
    if "/apartamento" in url_lower or "/apartamentos" in url_lower:
        return "Apartamento"
    if "/moradia" in url_lower or "/moradias" in url_lower or "/vivendas" in url_lower:
        return "Moradia"
    if "/terreno" in url_lower or "/terrenos" in url_lower:
        return "Terreno"
    if "/quinta" in url_lower or "/quintas" in url_lower:
        return "Quinta"
    if "/escritorio" in url_lower or "/escritórios" in url_lower:
        return "Escritório"

    # Text fallback
    combined = f"{title} {description}"
    for pattern, ptype in _PROPERTY_TYPE_PATTERNS:
        if pattern.search(combined):
            return ptype
    return ""


# ── Municipality / Parish extraction from Imovirtual location strings ─────────
# Imovirtual location format (from card <p> element):
#   "Street / Sub-area, Parish, Municipality, District"
#   Examples:
#     "Rua do Carmo, Chiado, Misericórdia, Lisboa, Lisboa"
#     "Monte Estoril, Cascais e Estoril, Cascais, Lisboa"
#     "Sesimbra, Sesimbra"  (short form)
#
# Strategy: split by comma, last part = district (usually = zone), second-to-last = municipality.
# Municipality is validated against known zone names for reliability.

_KNOWN_MUNICIPALITIES: frozenset[str] = frozenset({
    "Lisboa", "Cascais", "Sintra", "Almada", "Seixal", "Sesimbra",
    "Setúbal", "Setubal", "Oeiras", "Amadora", "Loures", "Odivelas",
    "Barreiro", "Moita", "Montijo", "Palmela",
})

_ZONE_MUNICIPALITY_DEFAULT: dict[str, str] = {
    "Lisboa":   "Lisboa",
    "Cascais":  "Cascais",
    "Sintra":   "Sintra",
    "Almada":   "Almada",
    "Seixal":   "Seixal",
    "Sesimbra": "Sesimbra",
}


def extract_municipality_parish(location_raw: str, zone: str = "") -> tuple[str, str]:
    """
    Parse an Imovirtual location string into (municipality, parish).

    Imovirtual location strings follow the pattern:
        "Street/Sub-area, Parish, Municipality, District"

    The last token is the district (equal to zone for our targets).
    The second-to-last token is the municipality.
    The third-to-last token is the best-effort parish.

    Returns ("", "") on failure — callers treat empty string as not-available.
    """
    if not location_raw:
        return _ZONE_MUNICIPALITY_DEFAULT.get(zone, ""), ""

    parts = [p.strip() for p in location_raw.split(",") if p.strip()]

    # Too short: just a city name or zone slug like "setubal/almada"
    if len(parts) < 2:
        return _ZONE_MUNICIPALITY_DEFAULT.get(zone, ""), ""

    # With 2+ parts: last = district, second-to-last = municipality (best bet)
    municipality = parts[-2]
    parish = parts[-3] if len(parts) >= 3 else ""

    # Sanity check: if extracted "municipality" is a district string
    # (e.g. "Lisboa" or "Setúbal"), it's correct.  If it's an obvious street
    # or sub-area (contains digits, very long string), fall back to zone default.
    if len(municipality) > 60 or any(c.isdigit() for c in municipality):
        return _ZONE_MUNICIPALITY_DEFAULT.get(zone, ""), ""

    return municipality, parish

def extract_phone_from_text(text: str) -> str:
    """
    Scan free-form text for a valid Portuguese phone number.
    Returns "+351XXXXXXXXX" or "" — delegates to utils.phone for validation.
    """
    return _extract_phone_validated(text)


def classify_owner_type(
    title: str,
    description: str,
    contact_name: str,
    agency_name: str,
    source: str,
    raw_owner_type: str = "",
) -> tuple[str, bool]:
    """
    Classify owner type from all available text signals.

    Returns (owner_type, is_owner) where:
      owner_type: 'fsbo' | 'agency' | 'developer' | 'unknown'
      is_owner:   True if FSBO (backwards-compatible bool for scoring)

    Priority:
      1. Explicit raw_owner_type passed by scraper (e.g. Custojusto 'Particular' label)
      2. Developer signals in text
      3. Agency signals in text
      4. FSBO signals in text
      5. Source bias (custojusto → fsbo by default)
      6. Unknown
    """
    # 1. Explicit from scraper
    if raw_owner_type in ("fsbo", "agency", "developer"):
        return raw_owner_type, raw_owner_type == "fsbo"

    # 1b. agency_name set by scraper → confirmed agency regardless of text signals
    if agency_name and agency_name.strip():
        return "agency", False

    combined = f"{title} {description} {contact_name} {agency_name}"

    # 2. Developer
    if _DEVELOPER_RE.search(combined):
        return "developer", False

    # 3. Agency
    if _AGENCY_RE.search(combined):
        return "agency", False

    # 4. Explicit FSBO signal
    if _FSBO_RE.search(combined):
        return "fsbo", True

    # 5. Source bias
    if source in ("custojusto", "olx"):
        return "fsbo", True      # these sources lean FSBO
    if source in ("era", "remax", "century21"):
        return "agency", False   # pure agency portals

    return "unknown", not bool(_AGENCY_RE.search(combined))


def extract_phone_from_text(text: str) -> str:
    """
    Scan free-form text for Portuguese phone numbers.
    Delegates to the validated implementation in utils.phone.
    Returns '+351XXXXXXXXX' or '' if none found.
    """
    return _extract_phone_validated(text)


# Email extraction from free text — standard RFC 5321 subset
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)


def extract_email_from_text(text: str) -> str:
    """
    Scan free-form text for an email address.
    Returns the first match (lowercased) or '' if none found.
    """
    if not text:
        return ""
    m = _EMAIL_RE.search(text)
    return m.group(0).lower() if m else ""


# ── WhatsApp extraction ───────────────────────────────────────────────────────
# Pass 1: wa.me/351XXXXXXXXX  or  api.whatsapp.com/send?phone=351XXXXXXXXX
_WA_URL_RE = re.compile(
    r"(?:wa\.me/|(?:api\.)?whatsapp\.com/send\?phone=)"
    r"(?:351)?"                            # optional PT country code in URL
    r"(\d{9,12})",
    re.IGNORECASE,
)
# Pass 2: keyword ("whatsapp", "zap", "wapp", "wa") followed by a PT number
_WA_KEYWORD_RE = re.compile(
    r"(?:whats\s*app|wapp|zap|(?<!\w)wa(?!\w))\s*[:\-–]?\s*"
    r"(?:\+351|00351)?\s*"
    r"([29]\d{2}[\s\-]?\d{3}[\s\-]?\d{3}"
    r"|[67]\d{2}[\s\-]?\d{3}[\s\-]?\d{3})",
    re.IGNORECASE,
)


def extract_whatsapp_from_text(text: str) -> str:
    """
    Scan free-form text for a WhatsApp contact.
    Detects wa.me links, api.whatsapp.com URLs, and keyword + PT number combos.
    Returns canonical '+351XXXXXXXXX' or '' if none found.
    """
    if not text:
        return ""
    # Pass 1: URL-based (most reliable)
    m = _WA_URL_RE.search(text)
    if m:
        digits = re.sub(r"[\s\-]", "", m.group(1))
        if digits.startswith("351") and len(digits) == 12:
            digits = digits[3:]
        if len(digits) == 9:
            return f"+351{digits}"
    # Pass 2: keyword-based
    m = _WA_KEYWORD_RE.search(text)
    if m:
        digits = re.sub(r"[\s\-]", "", m.group(1))
        if len(digits) == 9:
            return f"+351{digits}"
    return ""


# ── Website extraction ────────────────────────────────────────────────────────
# Portals whose domains must be silently ignored — they are the scraped sources,
# not the advertiser's own site.
_PORTAL_DOMAINS: frozenset[str] = frozenset({
    "olx.pt", "imovirtual.com", "idealista.pt", "idealista.com",
    "casa.sapo.pt", "sapo.pt", "custojusto.pt",
    "facebook.com", "instagram.com", "youtube.com",
    "google.com", "google.pt", "maps.google.com",
    "goo.gl", "bit.ly", "t.co", "twitter.com", "linkedin.com",
    "whatsapp.com", "wa.me",
})
# Matches https://(domain)/path, http://(domain)/path, www.(domain)/path
_WEBSITE_RE = re.compile(
    r"(?:https?://|www\.)"
    r"([a-zA-Z0-9][a-zA-Z0-9\-\.]{1,60}\.[a-zA-Z]{2,})"
    r"(?:/[^\s\"'<>]*)?",
    re.IGNORECASE,
)


def extract_website_from_text(text: str) -> str:
    """
    Scan free-form text for an advertiser website URL.
    Returns the clean lowercase domain (e.g. 'agencia-xyz.pt'),
    ignoring known classified/portal domains.
    Returns '' if nothing useful found.
    """
    if not text:
        return ""
    for m in _WEBSITE_RE.finditer(text):
        raw_domain = m.group(1).lower()
        # Strip leading 'www.'
        domain = raw_domain.lstrip("www.").split(":")[0]
        # Skip portals and social networks
        if domain in _PORTAL_DOMAINS:
            continue
        if any(domain == p or domain.endswith("." + p) for p in _PORTAL_DOMAINS):
            continue
        return domain
    return ""


# ── OLX residential quality filter ──────────────────────────────────────────
# Applied in _normalize_olx to drop clear non-residential noise.
#
# STRATEGY: two-layer approach
#   Layer 1 — title/description keyword match for unambiguously
#             non-residential property types.
#   Layer 2 — area_m2 sanity: anything under 10 m² is not a dwelling.
#
# CONSERVATIVE: only very clear patterns are blocked.
#   "loja" alone is NOT blocked (could be "loja + apartamento" or R/C).
#   "quinta" is NOT blocked (many residential quintas exist).
#   Borderline cases pass through — scoring will naturally rank them low.
#
# OVERRIDE: any listing with a residential keyword in the title keeps
#   it regardless of non-residential keyword presence.

_OLX_NON_RES_RE = re.compile(
    r"\bgaragem\b"
    r"|\bestacionamento\b|\blugar\s+(de\s+)?parking\b|\blugar\s+de\s+garagem\b"
    r"|\barrecada[çc]\w+\b"
    r"|\barmazém\b|\barmazem\b|\barmazenamento\b|\bself.?storage\b|\bmini.?armazém\b"
    r"|\btrespasse?\b"                # commercial transfer — always non-residential
    r"|\bgabinete\b"                  # office/consulting room
    r"|\bescrit[oó]rio\b"             # office space
    r"|\bsala\s+de\s+(reuniões|conferência|espera)\b"
    r"|\bconcessão\b",                # franchise/commercial concession
    re.IGNORECASE,
)
_OLX_RES_OVERRIDE_RE = re.compile(
    r"\bapartamento\b|\bmoradia\b|\bvivenda\b|\bflat\b|\bstudio\b"
    r"|\bquarto\b|\bandar\b|\bhabita[çc]\w+\b|\bT[0-9]\b",
    re.IGNORECASE,
)


class Normalizer:
    """
    Takes a RawListing's data dict and returns a normalised dict
    ready to be passed to Deduplicator → Enricher → Lead creation.
    """

    @staticmethod
    def _split_name(result: dict) -> None:
        """
        Split contact_name into first_name/last_name using PT conventions.
        Mutates result in-place. Called once per normalize() return.
        """
        from utils.helpers import split_pt_name
        name = result.get("contact_name", "") or ""
        first, last = split_pt_name(name)
        result["first_name"] = first or None
        result["last_name"] = last or None

    def normalize(self, source: str, raw: dict) -> Optional[dict]:
        """
        Dispatch to per-source normalizer.
        Returns None if the listing is invalid/incomplete.
        """
        try:
            if source == "olx":
                result = self._normalize_olx(raw)
            elif source == "olx_marketplace":
                result = self._normalize_olx_marketplace(raw)
            elif source == "standvirtual":
                result = self._normalize_standvirtual(raw)
            elif source == "imovirtual":
                result = self._normalize_imovirtual(raw)
            elif source == "idealista":
                result = self._normalize_idealista(raw)
            elif source == "sapo":
                result = self._normalize_sapo(raw)
            elif source == "custojusto":
                result = self._normalize_custojusto(raw)
            elif source == "linkedin":
                result = self._normalize_linkedin(raw)
            else:
                log.warning("Unknown source: {s}", s=source)
                result = self._normalize_generic(raw, source)
            # Apply name splitting + birthday to all sources
            if result:
                self._split_name(result)
                if "birthday" not in result:
                    result["birthday"] = raw.get("birthday")
            return result
        except Exception as e:
            log.error("Normalizer error for source={s}: {e}", s=source, e=e)
            return None

    # ── OLX ──────────────────────────────────────────────────────────────────

    def _normalize_olx(self, raw: dict) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        price = parse_price(raw.get("price_raw", ""))
        area = parse_area(raw.get("area_raw", "") or self._extract_area_from_text(title))
        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        typology = extract_typology(title)
        description = clean_text(raw.get("description", ""))

        # ── Residential quality filter ─────────────────────────────────────
        # Drop clear non-residential listings (garagens, armazéns, trespasses…)
        # before spending any further resources on them.
        if self._is_non_residential_olx(title, description or "", area):
            log.debug(
                "[normalizer/olx] Skipping non-residential: '{t}' area={a}",
                t=title[:80], a=area,
            )
            return None
        agency_name = clean_text(raw.get("agency_name", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        # Phone: prefer explicit field; fallback to validated text extraction
        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
            extract_phone_from_text(title),
        )
        # Email: prefer explicit field; fallback to text extraction from description
        contact_email = (
            raw.get("contact_email") or
            extract_email_from_text(description)
        )
        contact_whatsapp = (
            raw.get("contact_whatsapp") or
            extract_whatsapp_from_text(description) or
            extract_whatsapp_from_text(title)
        )
        contact_website = (
            raw.get("contact_website") or
            extract_website_from_text(description)
        )
        # Preserve contact_source set by the scraper (e.g. "olx_playwright").
        # Fall back to generic "olx" tag so the origin is always traceable.
        contact_source = (
            raw.get("contact_source") or
            ("olx" if (contact_phone or contact_email or contact_whatsapp or contact_website) else None)
        )

        # trader-title from detail page is the most reliable signal on OLX PT:
        #   "Utilizador" → "fsbo"  |  "Empresa"/"Profissional" → "agency"
        # Same pattern already used by the Custojusto normalizer.
        owner_type, is_owner = classify_owner_type(
            title, description or "", contact_name, agency_name, "olx",
            raw_owner_type=raw.get("owner_type_raw", ""),
        )
        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")
        municipality, parish = extract_municipality_parish(location_raw, zone)

        return {
            "title": title,
            "typology": typology,
            "property_type": extract_property_type(title, description or "", url),
            "area_m2": area,
            "price": price,
            "zone": zone,
            "municipality": municipality,
            "parish": parish or extract_parish(location_raw),
            "address": clean_text(location_raw),
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": contact_email,
            "contact_whatsapp": contact_whatsapp,
            "contact_website": contact_website,
            "contact_source": contact_source,
            "agency_name": agency_name,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description or "", url),
            "condition": self._detect_condition(title + " " + (description or "")),
            "source": "olx",
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── OLX Marketplace ──────────────────────────────────────────────────────

    def _normalize_olx_marketplace(self, raw: dict) -> Optional[dict]:
        """
        Normalise a raw dict from OLXMarketplaceScraper.

        Marketplace sellers are a proxy for property owners: people selling
        physical goods (furniture, electronics) are frequently moving/downsizing.

        Key differences from _normalize_olx:
          - lead_type is always "active_owner" (not fsbo/frbo)
          - product_title = listing title (what they're selling)
          - product_value = asking price for the item
          - price / area_m2 / typology left None (not real estate)
          - owner_type forced to "fsbo" (all marketplace sellers = private)
        """
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        description = clean_text(raw.get("description", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        # Product price (not a real estate price — stored in product_value)
        product_value = None
        price_raw = raw.get("price_raw", "")
        if price_raw:
            parsed = parse_price(price_raw)
            product_value = parsed if parsed and parsed < 50_000 else None  # sanity cap

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
            extract_phone_from_text(title),
        )
        contact_source = (
            raw.get("contact_source") or
            ("olx_marketplace" if contact_phone else None)
        )

        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")
        municipality, parish = extract_municipality_parish(location_raw, zone)

        return {
            "title":           title,               # product name (displayed in dashboard)
            "product_title":   title,               # explicit product_title field
            "product_value":   product_value,       # item price
            "typology":        None,                # not a property listing
            "property_type":   None,
            "area_m2":         None,
            "price":           None,                # no real estate price
            "zone":            zone,
            "municipality":    municipality,
            "parish":          parish,
            "address":         clean_text(location_raw),
            "description":     description or title,
            "contact_name":    contact_name,
            "contact_phone":   contact_phone,
            "phone_type":       phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email":   raw.get("contact_email") or extract_email_from_text(description),
            "contact_whatsapp": raw.get("contact_whatsapp") or extract_whatsapp_from_text(description),
            "contact_website": None,
            "contact_source":  contact_source,
            "agency_name":     None,
            "is_owner":        True,    # marketplace sellers are always private individuals
            "owner_type":      "fsbo",  # private seller — not an agency
            "lead_type":       "active_owner",
            "condition":       None,
            "source":          "olx_marketplace",
            "url":             url,
            "external_id":     raw.get("external_id"),
        }

    # ── Standvirtual ──────────────────────────────────────────────────────────

    def _normalize_standvirtual(self, raw: dict) -> Optional[dict]:
        """
        Normalise a raw dict from StandvirtualScraper.

        Vehicle sellers above €25k are property owner signals.
        Same pattern as _normalize_olx_marketplace but with:
          - Higher product_value cap (500k for luxury vehicles)
          - source = "standvirtual"
        """
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        description = clean_text(raw.get("description", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        product_value = raw.get("product_value")
        if product_value is None:
            price_raw = raw.get("price_raw", "")
            if price_raw:
                parsed = parse_price(price_raw)
                product_value = parsed if parsed and parsed < 500_000 else None

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
            extract_phone_from_text(title),
        )
        contact_source = (
            raw.get("contact_source") or
            ("standvirtual" if contact_phone else None)
        )

        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")
        municipality, parish = extract_municipality_parish(location_raw, zone)

        return {
            "title":           title,
            "product_title":   title,
            "product_value":   product_value,
            "typology":        None,
            "property_type":   None,
            "area_m2":         None,
            "price":           None,
            "zone":            zone,
            "municipality":    municipality,
            "parish":          parish,
            "address":         clean_text(location_raw),
            "description":     description or title,
            "contact_name":    contact_name,
            "contact_phone":   contact_phone,
            "phone_type":       phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email":   raw.get("contact_email") or extract_email_from_text(description),
            "contact_whatsapp": raw.get("contact_whatsapp") or extract_whatsapp_from_text(description),
            "contact_website": None,
            "contact_source":  contact_source,
            "agency_name":     None,
            "is_owner":        True,
            "owner_type":      "fsbo",
            "lead_type":       "active_owner",
            "condition":       None,
            "source":          "standvirtual",
            "url":             url,
            "external_id":     raw.get("external_id"),
        }

    # ── Imovirtual ───────────────────────────────────────────────────────────

    def _normalize_imovirtual(self, raw: dict) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        price = parse_price(raw.get("price_raw", ""))
        area = parse_area(raw.get("area_raw", ""))
        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        typology = extract_typology(
            raw.get("typology_raw", "") or
            raw.get("rooms_raw", "") or
            title
        )
        description = clean_text(raw.get("description", ""))
        agency_name = clean_text(raw.get("agency_name", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
        )
        contact_email = (
            raw.get("contact_email") or
            extract_email_from_text(description)
        )
        contact_whatsapp = (
            raw.get("contact_whatsapp") or
            extract_whatsapp_from_text(description)
        )
        contact_website = (
            raw.get("contact_website") or
            extract_website_from_text(description)
        )
        # Preserve contact_source set by the scraper ("imov_html" / "imov_playwright").
        # Fall back to generic "imovirtual" so the origin is always traceable.
        contact_source = (
            raw.get("contact_source") or
            ("imovirtual" if (contact_phone or contact_email or contact_whatsapp or contact_website) else None)
        )

        owner_type, is_owner = classify_owner_type(
            title, description or "", contact_name, agency_name, "imovirtual",
            raw_owner_type=raw.get("owner_type_raw", ""),
        )
        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")
        zone_query = raw.get("zone_query", "")
        municipality, parish = extract_municipality_parish(location_raw, zone)

        return {
            "title": title,
            "typology": typology,
            "property_type": extract_property_type(title, description or "", url),
            "area_m2": area,
            "price": price,
            "zone": zone,
            "municipality": municipality,
            "parish": parish or extract_parish(location_raw),
            "address": clean_text(location_raw),
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": contact_email,
            "contact_whatsapp": contact_whatsapp,
            "contact_website": contact_website,
            "contact_source": contact_source,
            "agency_name": agency_name,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description or "", url),
            "condition": self._detect_condition(title + " " + (description or "")),
            "source": "imovirtual",
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── Idealista ────────────────────────────────────────────────────────────

    def _normalize_idealista(self, raw: dict) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        price = parse_price(raw.get("price_raw", ""))
        area = parse_area(raw.get("area_raw", "") or self._extract_area_from_text(title))
        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        typology = extract_typology(
            raw.get("typology_raw", "") or title
        )
        description = clean_text(raw.get("description", ""))
        agency_name = clean_text(raw.get("agency_name", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
        )
        contact_email = (
            raw.get("contact_email") or
            extract_email_from_text(description)
        )
        contact_whatsapp = (
            raw.get("contact_whatsapp") or
            extract_whatsapp_from_text(description)
        )
        contact_website = (
            raw.get("contact_website") or
            extract_website_from_text(description)
        )
        # Preserve contact_source set by the scraper; fall back to generic tag.
        contact_source = (
            raw.get("contact_source") or
            ("idealista" if (contact_phone or contact_email or contact_whatsapp or contact_website) else None)
        )

        owner_type, is_owner = classify_owner_type(
            title, description or "", contact_name, agency_name, "idealista",
            raw_owner_type=raw.get("owner_type_raw", ""),
        )
        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")
        municipality, parish = extract_municipality_parish(location_raw, zone)

        return {
            "title": title,
            "typology": typology,
            "property_type": extract_property_type(title, description or "", url),
            "area_m2": area,
            "price": price,
            "zone": zone,
            "municipality": municipality,
            "parish": parish or extract_parish(location_raw),
            "address": clean_text(location_raw),
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": contact_email,
            "contact_whatsapp": contact_whatsapp,
            "contact_website": contact_website,
            "contact_source": contact_source,
            "agency_name": agency_name,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description or "", url),
            "condition": self._detect_condition(title + " " + (description or "")),
            "source": "idealista",
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── Sapo ─────────────────────────────────────────────────────────────────

    def _normalize_sapo(self, raw: dict) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        price = parse_price(raw.get("price_raw", ""))
        area = parse_area(raw.get("area_raw", "") or self._extract_area_from_text(title))
        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        typology = extract_typology(
            raw.get("typology_raw", "") or
            raw.get("rooms_raw", "") or
            title
        )
        description = clean_text(raw.get("description", ""))
        agency_name = clean_text(raw.get("agency_name", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
        )
        contact_email = (
            raw.get("contact_email") or
            extract_email_from_text(description)
        )
        contact_whatsapp = (
            raw.get("contact_whatsapp") or
            extract_whatsapp_from_text(description)
        )
        contact_website = (
            raw.get("contact_website") or
            extract_website_from_text(description)
        )
        contact_source = "sapo" if (contact_phone or contact_email or contact_whatsapp or contact_website) else None

        owner_type, is_owner = classify_owner_type(
            title, description or "", contact_name, agency_name, "sapo",
        )
        url = raw.get("url", "")

        return {
            "title": title,
            "typology": typology,
            "area_m2": area,
            "price": price,
            "zone": zone,
            "parish": extract_parish(raw.get("location_raw", "")),
            "address": clean_text(raw.get("location_raw", "")),
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": contact_email,
            "contact_whatsapp": contact_whatsapp,
            "contact_website": contact_website,
            "contact_source": contact_source,
            "agency_name": agency_name,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description or "", url),
            "condition": self._detect_condition(title + " " + (description or "")),
            "source": "sapo",
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── Custojusto ───────────────────────────────────────────────────────────

    def _normalize_custojusto(self, raw: dict) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        price = parse_price(raw.get("price_raw", ""))
        area = parse_area(raw.get("area_raw", "") or self._extract_area_from_text(title))
        zone = normalise_zone(raw.get("location_raw") or raw.get("zone_query", ""))
        typology = extract_typology(title)
        description = clean_text(raw.get("description", ""))
        contact_name = clean_text(raw.get("contact_name", ""))

        # Phone: prefer card-level extraction (scraper already ran _PHONE_RE);
        # fall back to text regex on description
        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
            extract_phone_from_text(title),
        )
        contact_source = "custojusto" if contact_phone else None

        # Scraper already classified owner_type from seller label — honour it
        raw_owner_type = raw.get("owner_type", "")
        owner_type, is_owner = classify_owner_type(
            title, description or "", contact_name, "", "custojusto",
            raw_owner_type=raw_owner_type,
        )

        url = raw.get("url", "")

        return {
            "title": title,
            "typology": typology,
            "area_m2": area,
            "price": price,
            "zone": zone,
            "municipality": raw.get("municipality") or zone,
            "parish": raw.get("parish") or extract_parish(raw.get("location_raw", "")),
            "address": clean_text(raw.get("location_raw", "")),
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": raw.get("contact_email"),
            "contact_source": contact_source,
            "agency_name": "",
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description or "", url),
            "condition": self._detect_condition(title + " " + (description or "")),
            "source": "custojusto",
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── LinkedIn ─────────────────────────────────────────────────────────────

    def _normalize_linkedin(self, raw: dict) -> Optional[dict]:
        """LinkedIn posts tend to have less structured data — best-effort extraction."""
        title = clean_text(raw.get("title") or raw.get("text", "")[:200])
        if not title:
            return None

        full_text = clean_text(raw.get("text", ""))
        price = parse_price(full_text)
        area = parse_area(full_text)
        zone = normalise_zone(raw.get("location", ""))
        typology = extract_typology(full_text)
        company = clean_text(raw.get("company", ""))
        author = clean_text(raw.get("author_name", ""))

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(full_text),
        )
        contact_source = "linkedin" if contact_phone else None

        owner_type, is_owner = classify_owner_type(
            title, full_text, author, company, "linkedin",
        )
        url = raw.get("url", "")

        return {
            "title": title,
            "typology": typology,
            "area_m2": area,
            "price": price,
            "zone": zone,
            "parish": extract_parish(raw.get("location", "")),
            "address": clean_text(raw.get("location", "")),
            "description": full_text,
            "contact_name": author,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": raw.get("author_email"),
            "contact_source": contact_source,
            "agency_name": company,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, full_text, url),
            "condition": self._detect_condition(full_text),
            "source": "linkedin",
            "url": url,
            "external_id": raw.get("post_id"),
        }

    # ── Generic fallback ──────────────────────────────────────────────────────

    def _normalize_generic(self, raw: dict, source: str) -> Optional[dict]:
        title = clean_text(raw.get("title", ""))
        if not title:
            return None

        description = clean_text(raw.get("description", ""))
        agency_name = clean_text(raw.get("agency_name", "") or "")
        contact_name = clean_text(raw.get("contact_name", "") or "")

        contact_phone, phone_type, phone_conf = _resolve_phone(
            raw.get("contact_phone"),
            extract_phone_from_text(description),
        )
        contact_source = source if contact_phone else None

        owner_type, is_owner = classify_owner_type(
            title, description, contact_name, agency_name, source,
        )
        url = raw.get("url", "")
        location_raw = raw.get("location_raw", "")

        return {
            "title": title,
            "typology": extract_typology(title),
            "area_m2": parse_area(raw.get("area_raw", "")),
            "price": parse_price(raw.get("price_raw", "")),
            "zone": normalise_zone(location_raw),
            "parish": extract_parish(location_raw),
            "address": location_raw,
            "description": description or title,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "phone_type": phone_type,
            "contact_confidence": phone_conf if contact_phone else 0,
            "contact_email": raw.get("contact_email"),
            "contact_source": contact_source,
            "agency_name": agency_name,
            "is_owner": is_owner,
            "owner_type": owner_type,
            "lead_type": classify_lead_type(owner_type, title, description, url),
            "condition": "desconhecido",
            "source": source,
            "url": url,
            "external_id": raw.get("external_id"),
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _is_non_residential_olx(title: str, description: str, area_m2: float | None) -> bool:
        """
        Return True when the OLX listing is clearly NOT a residential property.

        Two-layer check:
          1. Area sanity: < 10 m² → not a dwelling (garagem, lugar, arrecadação)
          2. Keyword: matches non-residential pattern AND has no residential override.

        Conservative by design — only unambiguous non-residential listings are
        filtered. Borderline cases pass through and are ranked low by the scorer.
        """
        # Layer 1 — area too small for any dwelling
        if area_m2 is not None and area_m2 < 10:
            return True

        # Layer 2 — keyword match (only when no residential override)
        combined = f"{title} {description}"
        if _OLX_NON_RES_RE.search(combined) and not _OLX_RES_OVERRIDE_RE.search(combined):
            return True

        return False

    def _extract_area_from_text(self, text: str) -> str:
        import re
        match = re.search(r"\d+\s*m²?", text or "", re.IGNORECASE)
        return match.group(0) if match else ""

    def _detect_condition(self, text: str) -> str:
        import re
        text_l = (text or "").lower()
        if re.search(r"\bnovo\b|\bnovíssimo\b|\bpronto\b|\bentrega imediata\b", text_l):
            return "novo"
        if re.search(r"\bruína\b|\bruina\b|\bpara demolir\b", text_l):
            return "ruína"
        if re.search(r"\bprecisa obras\b|\bpara renovar\b|\bpara remodelar\b|\bhabitar e renovar\b", text_l):
            return "renovar"
        if re.search(r"\busado\b|\bsemi-novo\b|\bbom estado\b", text_l):
            return "usado"
        return "desconhecido"

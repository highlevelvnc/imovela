"""
Portuguese phone validation, normalisation and classification.

Centralises all phone logic previously scattered across helpers.py,
normalizer.py, contact_export.py and scorer.py.

Public API
----------
validate_pt_phone(raw)   → PhoneResult
classify_phone_type(nat) → "mobile" | "landline" | "relay" | "premium" | "unknown"
extract_phone_from_text(text) → "+351XXXXXXXXX" | ""

Phone type semantics
--------------------
  mobile   — 9[1236]x (MEO/Vodafone/NOS standard ranges) — highest conversion
  landline — 2xx (Lisboa=21x, Porto=22x, others=23x-29x) — often agency
  relay    — 6xx (OLX phone-masking / VoIP forwarding) — valid but indirect
  premium  — 707/708/800/808 (premium-rate) — reject, never call
  unknown  — valid digit count but unclassified prefix

Valid examples
--------------
  "+351912345678"   → mobile, confidence=90
  "+351211234567"   → landline, confidence=70
  "+351661234567"   → relay, confidence=50
  "912 345 678"     → mobile, confidence=90  (spaces stripped)
  "+351917000000"   → mobile, confidence=90

Invalid examples
----------------
  "+351000000000"   → rejected (suspicious sequence)
  "+351123456789"   → rejected (suspicious sequence)
  "+35121123456"    → rejected (8 digits, not 9)
  "+3517071234567"  → rejected (premium number)
  "abc"             → rejected (no digits)
  ""                → rejected (empty)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# ── Classification sets ───────────────────────────────────────────────────────

# Standard Portuguese mobile prefixes (first 2 digits of national number)
# MEO: 91x | Vodafone: 96x | NOS: 93x | NOWO/others: 92x, 90x
_MOBILE_PREFIXES: frozenset[str] = frozenset([
    "90", "91", "92", "93", "96",
])

# OLX Portugal phone-masking service and general VoIP relay ranges.
# These numbers RING through to the real phone but are not direct owner numbers.
# Prefix 6xx is not assigned to standard PT consumers by ANACOM.
_RELAY_PREFIXES: frozenset[str] = frozenset([
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
])

# Premium / special services — never call, strip from pipeline
_PREMIUM_PREFIXES: frozenset[str] = frozenset([
    "707", "708", "800", "808", "809",
])

# Sequences that look like test/placeholder data — reject outright
_SUSPICIOUS_SEQUENCES: frozenset[str] = frozenset([
    "000000000",
    "111111111", "222222222", "333333333", "444444444",
    "555555555", "666666666", "777777777", "888888888", "999999999",
    "123456789", "987654321",
    "123123123", "112112112",
    "999999998", "000000001",
])


# ── Regex helpers ─────────────────────────────────────────────────────────────

# Matches a fully-formed canonical number stored in the DB
_CANONICAL_RE = re.compile(
    r"^\+351([29]\d{8}|[67]\d{8})$"
)

# Extracts phone numbers from free text (descriptions, titles)
# Handles formats:
#   "917 654 321", "917.654.321", "917-654-321", "+351917654321"
#   "(917) 654 321", "Tlm: 917654321", "Telf.: 21 345 6789"
_TEXT_PHONE_RE = re.compile(
    r"(?:(?:Tlm|Telf?|Tel|Telemóvel|Telef|Telefone)\s*[:.\-]?\s*)?"  # optional Tlm: label
    r"(?:\+351|00351)?"                                                  # optional country code
    r"\s*"
    r"(\(?"                                                              # optional open paren
    r"(?:[29]\d{2}|[67]\d{2})"                                         # 3-digit prefix (9xx/2xx/6xx/7xx)
    r"\)?"                                                               # optional close paren
    r"[\s\-\.]?"                                                         # separator (space/hyphen/dot)
    r"\d{3}"                                                             # middle 3 digits
    r"[\s\-\.]?"                                                         # separator
    r"\d{3}"                                                             # last 3 digits
    r")"
    r"(?!\d)",
    re.IGNORECASE,
)


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class PhoneResult:
    """
    Result of validate_pt_phone().

    Fields
    ------
    valid      — True if the number passed all checks
    canonical  — "+351XXXXXXXXX" (only set when valid=True)
    national   — 9-digit string without country code (set when parseable)
    phone_type — "mobile" | "landline" | "relay" | "premium" | "unknown"
    suspicious — True if the sequence matched a known placeholder pattern
    confidence — 0-100 score for downstream use (stored in contact_confidence)
    reason     — human-readable rejection reason (empty string when valid)
    """
    valid:      bool
    canonical:  Optional[str]
    national:   Optional[str]
    phone_type: str
    suspicious: bool
    confidence: int
    reason:     str


# ── Core functions ────────────────────────────────────────────────────────────

def classify_phone_type(national: str) -> str:
    """
    Return the semantic type of a 9-digit national number (no country code).

      "mobile"   — starts with 91/92/93/96
      "landline" — starts with 2
      "relay"    — starts with 6x (OLX masking / VoIP forwarding)
      "premium"  — starts with 707/708/800/808/809
      "unknown"  — valid digit count but unrecognised prefix
    """
    if not national or len(national) != 9 or not national.isdigit():
        return "unknown"

    if national[:3] in _PREMIUM_PREFIXES:
        return "premium"
    if national[:2] in _MOBILE_PREFIXES:
        return "mobile"
    if national[:2] in _RELAY_PREFIXES:
        return "relay"
    if national[0] == "2":
        return "landline"
    return "unknown"


def validate_pt_phone(raw: str) -> PhoneResult:
    """
    Full validation pipeline for a Portuguese phone number.

    Accepts any reasonable input format:
      "+351 912 345 678", "00351912345678", "912345678", "+351912345678"

    Returns a PhoneResult.  Check ``.valid`` before using ``.canonical``.
    """
    if not raw or not raw.strip():
        return PhoneResult(
            valid=False, canonical=None, national=None,
            phone_type="unknown", suspicious=False, confidence=0,
            reason="empty input",
        )

    # Step 1 — strip all formatting characters
    stripped = re.sub(r"[\s\-\(\)\.\/\+]", "", raw.strip())

    # Step 2 — isolate the 9-digit national number
    if stripped.startswith("351") and len(stripped) == 12:
        national = stripped[3:]
    elif stripped.startswith("00351") and len(stripped) == 14:
        national = stripped[5:]
    elif len(stripped) == 9:
        national = stripped
    else:
        # Try stripping a leading "351" prefix even without "00"
        if stripped.startswith("351") and len(stripped) > 12:
            return PhoneResult(
                valid=False, canonical=None, national=None,
                phone_type="unknown", suspicious=False, confidence=0,
                reason=f"unrecognised format: {raw!r}",
            )
        # Could be 9+3=12 chars with country code appended without prefix marker
        national = stripped

    # Step 3 — must be exactly 9 decimal digits
    if not national.isdigit() or len(national) != 9:
        return PhoneResult(
            valid=False, canonical=None, national=national or None,
            phone_type="unknown", suspicious=False, confidence=0,
            reason=f"expected 9 digits, got {len(national)}: {national!r}",
        )

    # Step 4 — suspicious / placeholder sequence check
    if national in _SUSPICIOUS_SEQUENCES:
        return PhoneResult(
            valid=False, canonical=None, national=national,
            phone_type="unknown", suspicious=True, confidence=0,
            reason=f"suspicious placeholder sequence: {national}",
        )

    # 6+ consecutive identical digits (e.g. 990000000, 912000000, 911111111)
    # almost always indicate a placeholder or test number.
    if re.search(r"(\d)\1{5,}", national):
        return PhoneResult(
            valid=False, canonical=None, national=national,
            phone_type="unknown", suspicious=True, confidence=0,
            reason=f"6+ repeated digits: {national}",
        )

    # Step 5 — first digit must be 2, 6, 7 or 9  (PT valid range)
    if national[0] not in "2679":
        return PhoneResult(
            valid=False, canonical=None, national=national,
            phone_type="unknown", suspicious=False, confidence=0,
            reason=f"invalid first digit '{national[0]}' for PT number",
        )

    # Step 6 — classify type and check for premium numbers
    phone_type = classify_phone_type(national)

    if phone_type == "premium":
        return PhoneResult(
            valid=False, canonical=None, national=national,
            phone_type="premium", suspicious=False, confidence=0,
            reason=f"premium/special-rate number: {national[:3]}xxx",
        )

    # Step 7 — all checks passed
    canonical = f"+351{national}"
    confidence = {"mobile": 90, "landline": 70, "relay": 50, "unknown": 40}.get(phone_type, 40)

    return PhoneResult(
        valid=True, canonical=canonical, national=national,
        phone_type=phone_type, suspicious=False, confidence=confidence,
        reason="",
    )


def extract_phone_from_text(text: str) -> str:
    """
    Scan free-form text for a Portuguese phone number.
    Returns "+351XXXXXXXXX" if found and valid, or "" if not found.

    Tries all matches and returns the first valid one (mobile preferred
    over landline when both are present at the same position is not
    guaranteed — just returns first valid canonical result).

    Handles formats: "917 654 321", "917.654.321", "+351917654321",
    "(917) 654 321", "Tlm: 917654321", "Telf.: 21 345 6789"
    """
    if not text:
        return ""
    # Try all matches, return first that validates
    for m in _TEXT_PHONE_RE.finditer(text):
        raw_digits = re.sub(r"[\s\-\.\(\)]", "", m.group(1))
        result = validate_pt_phone(raw_digits)
        if result.valid:
            return result.canonical
    return ""


# ── Priority helpers ──────────────────────────────────────────────────────────

_TYPE_PRIORITY: dict[str, int] = {
    "mobile":   100,
    "landline": 70,
    "relay":    40,
    "unknown":  20,
    "premium":  0,
}


def best_phone(candidates: list[str]) -> Optional[PhoneResult]:
    """
    Given multiple raw phone candidates, return the highest-priority valid one.

    Priority: mobile > landline > relay > unknown. When ties exist,
    the first-seen candidate wins (caller should pass candidates in
    source-preference order: description > tel: href > button reveal).

    Returns None when no candidate validates.
    """
    best: Optional[PhoneResult] = None
    best_score = -1
    for raw in candidates:
        if not raw:
            continue
        result = validate_pt_phone(raw)
        if not result.valid:
            continue
        score = _TYPE_PRIORITY.get(result.phone_type, 0)
        if score > best_score:
            best = result
            best_score = score
    return best


# ── WhatsApp extraction ──────────────────────────────────────────────────────

# Matches wa.me/351XXXXXXXXX or api.whatsapp.com/send?phone=351XXXXXXXXX
_WA_LINK_RE = re.compile(
    r"(?:wa\.me/|api\.whatsapp\.com/send\?phone=|whatsapp\.com/send\?phone=)"
    r"(?:00)?(351)?(\d{9,12})",
    re.IGNORECASE,
)

# Inline WhatsApp mentions — "WhatsApp 912 345 678", "WA: 912345678"
_WA_TEXT_RE = re.compile(
    r"(?:whats\s*app|wa|wpp|zap)[\s:\.\-]*"
    r"(?:\+351|00351)?"
    r"\s*(9[1236]\d[\s\-\.]?\d{3}[\s\-\.]?\d{3})",
    re.IGNORECASE,
)


def extract_whatsapp(text_or_html: str) -> str:
    """
    Scan text or HTML for a WhatsApp phone number.

    Checks (in order):
      1. Direct wa.me/ or api.whatsapp.com/send?phone= links
      2. "WhatsApp: 912..." inline mentions

    Returns "+351XXXXXXXXX" if found and valid, or "" otherwise.
    """
    if not text_or_html:
        return ""

    # 1. wa.me / api.whatsapp.com links
    for m in _WA_LINK_RE.finditer(text_or_html):
        digits = m.group(2)
        # Link might include country code inside digits (e.g. wa.me/351912345678)
        if len(digits) == 12 and digits.startswith("351"):
            digits = digits[3:]
        if len(digits) >= 9:
            result = validate_pt_phone(digits[-9:])
            if result.valid:
                return result.canonical

    # 2. Inline "WhatsApp 912..."
    for m in _WA_TEXT_RE.finditer(text_or_html):
        digits = re.sub(r"[\s\-\.]", "", m.group(1))
        result = validate_pt_phone(digits)
        if result.valid:
            return result.canonical

    return ""


# ── tel: href helper ─────────────────────────────────────────────────────────

def extract_phone_from_tel_href(href: str) -> str:
    """
    Extract + validate a phone number from a tel: href.

    Handles `tel:+351912345678`, `tel:912345678`, `tel:00351912 345 678`.
    Returns "+351XXXXXXXXX" on success, or "" on failure/invalid.
    """
    if not href:
        return ""
    raw = href.replace("tel:", "").replace("callto:", "").strip()
    result = validate_pt_phone(raw)
    return result.canonical if result.valid else ""

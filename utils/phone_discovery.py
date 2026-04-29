"""
Aggressive phone discovery — finds the seller's REAL number across every
surface a portal might leak it on, BEFORE we resort to any reveal-button
that returns the masking-relay 6XX.

Why this matters
----------------
Portals route phone numbers through three tiers:

  1. Hidden static surfaces — JSON-LD, microdata, ``data-*`` attrs,
     hidden form fields, ``__NEXT_DATA__``, ``window.__APOLLO_STATE__``,
     server-rendered comments. The seller's REAL number leaks here often
     enough to be worth scanning every time. Capturing it pre-reveal
     lets us SKIP the masking JS and keep a mobile-grade contact.

  2. Reveal-button click → relay number (the 6XX you observed). Last
     resort: only invoked when steps 1 + 2 found nothing better.

  3. Owner-profile page on the portal — separate sweep.

This module is the *first-tier* extractor. It produces a list of every
plausible PT phone candidate found in the input HTML, lets ``best_phone``
pick the best (mobile > landline > relay), and never adds the relay
number unless that's all that exists.

Public API
----------
``discover_phones(html_or_text, *, allow_relay=True) -> list[str]``
    Returns canonical "+351XXXXXXXXX" entries, deduplicated, in source
    preference order so ``best_phone`` ties resolve toward higher-trust
    sources. ``allow_relay=False`` filters out any 6XX result.

``discover_whatsapp(html_or_text) -> list[str]``
    Same idea, restricted to WhatsApp-targeted numbers.
"""
from __future__ import annotations

import re
from typing import Iterable, Optional

from utils.phone import (
    extract_phone_from_tel_href,
    extract_phone_from_text,
    extract_whatsapp,
    validate_pt_phone,
)


# ── Regex toolkit ────────────────────────────────────────────────────────────

# Generic PT phone regex used against JSON / inline scripts. Same body as the
# one in utils/phone but stricter: we discard 6XX matches in the post-filter
# instead of pre-filter, because we want to DETECT relay candidates before
# rejecting them.
_PT_PHONE_RE = re.compile(
    r"(?<!\d)"
    r"(?:\+?351|00351)?"            # optional country code
    r"\s*"
    r"([2367]\d{2}[\s\-\.]?\d{3}[\s\-\.]?\d{3}|9\d{2}[\s\-\.]?\d{3}[\s\-\.]?\d{3})"
    r"(?!\d)"
)

# JSON-style key:value capture for "telephone": "+351...", "phone": "...",
# "contactNumber": "...", "tel": "..." anywhere in the page payload.
_JSON_PHONE_RE = re.compile(
    r'"(?:telephone|tel|phone(?:Number)?|contactNumber|mobile|telemovel|telef'
    r'|contact_phone|sellerPhone|owner_phone)"\s*:\s*"([^"]+)"',
    re.IGNORECASE,
)

# WhatsApp deep link variants — including custom URL schemes
_WA_DEEPLINK_RE = re.compile(
    r"(?:wa\.me/|api\.whatsapp\.com/send\?phone=|whatsapp\.com/send\?phone="
    r"|whatsapp://send\?phone=|whatsapp://chat/\?phone=)"
    r"(?:00)?(351)?(\d{9,12})",
    re.IGNORECASE,
)


# ── BeautifulSoup helpers ────────────────────────────────────────────────────

def _phones_from_soup(soup) -> list[str]:
    """Pull phone candidates from microdata, data-* attrs, hidden inputs."""
    out: list[str] = []
    if soup is None:
        return out

    # 1. tel: anchors (anywhere)
    for a in soup.select("a[href^='tel:'], a[href^='callto:']"):
        phone = extract_phone_from_tel_href(a.get("href", ""))
        if phone:
            out.append(phone)

    # 2. itemprop microdata
    for el in soup.select("[itemprop='telephone'], [itemprop='phone']"):
        candidate = (el.get("content") or el.get_text(strip=True) or "").strip()
        result = validate_pt_phone(candidate)
        if result.valid:
            out.append(result.canonical)

    # 3. data-* attributes commonly used by SPAs to expose pre-rendered numbers
    DATA_KEYS = (
        "data-phone", "data-tel", "data-telephone", "data-mobile",
        "data-contact", "data-contact-phone", "data-seller-phone",
        "data-number", "data-telemovel",
    )
    for key in DATA_KEYS:
        for el in soup.select(f"[{key}]"):
            raw = (el.get(key) or "").strip()
            result = validate_pt_phone(raw)
            if result.valid:
                out.append(result.canonical)

    # 4. Hidden input fields — `<input type="hidden" name="phone" value="...">`
    for el in soup.select(
        "input[name*='phone' i], input[name*='telef' i], "
        "input[name*='telemovel' i], input[name*='contact' i]"
    ):
        raw = (el.get("value") or "").strip()
        result = validate_pt_phone(raw)
        if result.valid:
            out.append(result.canonical)

    # 5. <meta> tags — og:phone_number, contact, telephone
    for sel in (
        "meta[property*='phone' i]",
        "meta[name*='phone' i]",
        "meta[name*='telef' i]",
        "meta[itemprop='telephone']",
    ):
        for el in soup.select(sel):
            raw = (el.get("content") or "").strip()
            result = validate_pt_phone(raw)
            if result.valid:
                out.append(result.canonical)

    # 6. Visible button/anchor text after reveal-click — sometimes the number
    #    appears inside the button itself rather than as a tel: href
    for el in soup.select("button, a"):
        text = el.get_text(strip=True)
        if not text or len(text) > 30:
            continue
        m = _PT_PHONE_RE.search(text)
        if m:
            digits = re.sub(r"[\s\-\.]", "", m.group(1))
            result = validate_pt_phone(digits)
            if result.valid:
                out.append(result.canonical)

    return out


# ── HTML/text-level extractors ───────────────────────────────────────────────

def _phones_from_json_blobs(html: str) -> list[str]:
    """Extract phone candidates from JSON-style key/value pairs in the HTML."""
    out: list[str] = []
    for m in _JSON_PHONE_RE.finditer(html):
        raw = m.group(1)
        result = validate_pt_phone(raw)
        if result.valid:
            out.append(result.canonical)
    return out


def _phones_from_inline_scripts(html: str) -> list[str]:
    """
    Catch numbers embedded inside ``<script>`` tags — Vue/React stores often
    serialise the seller object directly. Restrict to script blocks so we
    don't pull random numbers out of the visible body twice.
    """
    out: list[str] = []
    for script_match in re.finditer(
        r"<script[^>]*>([\s\S]*?)</script>", html, re.IGNORECASE
    ):
        body = script_match.group(1)
        # Common seller-data patterns
        for pattern in (
            r'sellerPhone["\']?\s*[:=]\s*["\']([^"\']+)',
            r'phone(?:Number)?["\']?\s*[:=]\s*["\']([^"\']+)',
            r'telephone["\']?\s*[:=]\s*["\']([^"\']+)',
            r'tel["\']?\s*[:=]\s*["\']([^"\']+)',
        ):
            for m in re.finditer(pattern, body, re.IGNORECASE):
                raw = m.group(1)
                result = validate_pt_phone(raw)
                if result.valid:
                    out.append(result.canonical)
    return out


def _whatsapp_from_soup(soup) -> list[str]:
    """Pull WhatsApp deep links from anchors + buttons."""
    out: list[str] = []
    if soup is None:
        return out
    for a in soup.select(
        "a[href*='wa.me'], a[href*='whatsapp.com'], "
        "a[href^='whatsapp://'], button[onclick*='whatsapp' i]"
    ):
        href = (a.get("href") or a.get("onclick") or "")
        wa = extract_whatsapp(href)
        if wa:
            out.append(wa)
    return out


# ── Public API ───────────────────────────────────────────────────────────────

def discover_phones(
    html_or_text: str,
    *,
    soup=None,
    description: str = "",
    allow_relay: bool = True,
) -> list[str]:
    """
    Aggressive multi-source phone discovery, ordered by source trust.

    Source order in the returned list (higher trust first → ``best_phone``
    picks the right one when scores tie):

        1. WhatsApp deep links (always direct mobile)
        2. Microdata + data-* attrs + hidden inputs (JS-injected real number)
        3. JSON key/value pairs in raw HTML (SSR seller payloads)
        4. Inline ``<script>`` blocks (Vue/React stores)
        5. Description / inner text
        6. tel: hrefs (often the relay on portals — last)

    ``soup`` is optional but recommended; pass it when you've already parsed
    the page so we don't re-parse internally.

    Returns canonical "+351XXXXXXXXX" strings, deduplicated, preserving
    discovery order.

    ``allow_relay=False`` filters out validated 6XX results — useful when
    you only want a "real" number even at the cost of having no number.
    """
    seen: set[str] = set()
    ordered: list[str] = []

    def _add(items: Iterable[str]) -> None:
        for it in items:
            if not it or it in seen:
                continue
            if not allow_relay:
                check = validate_pt_phone(it)
                if check.valid and check.phone_type == "relay":
                    continue
            seen.add(it)
            ordered.append(it)

    if soup is None:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_or_text or "", "html.parser") if html_or_text else None
        except Exception:
            soup = None

    # 1. WhatsApp deep links
    _add(_whatsapp_from_soup(soup))
    if html_or_text:
        for m in _WA_DEEPLINK_RE.finditer(html_or_text):
            digits = m.group(2)
            if len(digits) == 12 and digits.startswith("351"):
                digits = digits[3:]
            if len(digits) >= 9:
                result = validate_pt_phone(digits[-9:])
                if result.valid:
                    _add([result.canonical])

    # 2. Microdata / data-* / hidden inputs / meta — soup-based
    _add(_phones_from_soup(soup))

    # 3. JSON key/value scan over raw HTML
    if html_or_text:
        _add(_phones_from_json_blobs(html_or_text))

    # 4. Inline script blocks
    if html_or_text:
        _add(_phones_from_inline_scripts(html_or_text))

    # 5. Description scan (passed separately — more precise than full body)
    if description:
        wa = extract_whatsapp(description)
        if wa:
            _add([wa])
        phone = extract_phone_from_text(description)
        if phone:
            _add([phone])

    # 6. Plain regex on full page text — last resort, noisier
    if html_or_text:
        for m in _PT_PHONE_RE.finditer(html_or_text):
            digits = re.sub(r"[\s\-\.]", "", m.group(1))
            result = validate_pt_phone(digits)
            if result.valid:
                _add([result.canonical])

    return ordered


def discover_whatsapp(html_or_text: str, *, soup=None) -> list[str]:
    """Return every distinct WhatsApp number discoverable in the page."""
    out: list[str] = []
    seen: set[str] = set()

    if soup is None:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_or_text or "", "html.parser") if html_or_text else None
        except Exception:
            soup = None

    for n in _whatsapp_from_soup(soup):
        if n not in seen:
            seen.add(n)
            out.append(n)

    if html_or_text:
        for m in _WA_DEEPLINK_RE.finditer(html_or_text):
            digits = m.group(2)
            if len(digits) == 12 and digits.startswith("351"):
                digits = digits[3:]
            if len(digits) >= 9:
                result = validate_pt_phone(digits[-9:])
                if result.valid and result.canonical not in seen:
                    seen.add(result.canonical)
                    out.append(result.canonical)

        wa = extract_whatsapp(html_or_text)
        if wa and wa not in seen:
            seen.add(wa)
            out.append(wa)

    return out


def best_real_phone(html_or_text: str, *, soup=None, description: str = "") -> Optional[str]:
    """
    Return the single best NON-RELAY number, or None when none found.

    Convenience wrapper: discovers candidates with ``allow_relay=False``
    and applies ``best_phone`` so the caller gets a one-liner.
    """
    from utils.phone import best_phone
    candidates = discover_phones(
        html_or_text, soup=soup, description=description, allow_relay=False,
    )
    if not candidates:
        return None
    result = best_phone(candidates)
    return result.canonical if result and result.valid else None

"""
Email extraction from scraped HTML + free text.

Centralises all email-scraping logic. Used by every detail-page scraper
(OLX, Imovirtual, Sapo, Custojusto) to maximise contact completeness.

Sources handled, in priority order:
  1. ``mailto:`` hrefs                  → highest confidence, direct link
  2. JSON-LD ``email`` fields           → structured, always-real
  3. Plain text e-mail regex            → standard RFC-5321 subset
  4. Obfuscated patterns                → "foo [at] bar [dot] com"

Filtering:
  * Portal / platform accounts are excluded: `noreply@`, `info@olx.pt`,
    `support@imovirtual.com`, abuse/DPO/legal mailboxes — these are never
    direct seller contacts.
  * Common fake patterns rejected: ``test@test.*``, ``a@a.*``,
    ``example.com`` domain.
  * Image-hash / file-extension captures stripped: ``foo@2x.png``,
    ``avatar@1.5x.jpg``.

Public API
----------
extract_emails(text_or_html)           -> list[str]   # every unique valid email
extract_first_email(text_or_html)      -> str         # best match, or ""
is_generic_portal_email(addr)          -> bool
"""
from __future__ import annotations

import re
from typing import Iterable


# ── Regex patterns ────────────────────────────────────────────────────────────

# RFC-5321-ish — liberal on the local-part, strict on TLD (≥2 chars).
# The negative look-ahead avoids matching image dpi suffixes (@2x, @1.5x).
_EMAIL_RE = re.compile(
    r"(?<![a-zA-Z0-9._%+\-@])"                  # start-of-token
    r"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})"
    r"(?![a-zA-Z0-9._%+\-@])",                  # end-of-token
)

# Obfuscation patterns — sellers dodging portal filters.
# Word-boundary anchors (\b) avoid false positives on "att", "dote" etc.
_OBFUSCATED_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]{2,})"                  # local-part
    r"\s*"
    r"(?:\[at\]|\(at\)|\{at\}|\bat\b|\barroba\b|@)"  # @-marker
    r"\s*"
    r"([a-zA-Z0-9\-]+"
      r"(?:\s*(?:\[dot\]|\(dot\)|\{dot\}|\bdot\b|\bponto\b|\.)\s*[a-zA-Z0-9\-]+){1,3}"
    r")",
    re.IGNORECASE,
)

# mailto: href extractor — works on raw HTML or parsed href strings
_MAILTO_RE = re.compile(
    r"mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})(?:[?&][^\"'>\s]*)?",
    re.IGNORECASE,
)

# JSON-LD / JSON: `"email": "foo@bar.com"`
_JSON_EMAIL_RE = re.compile(
    r'"email"\s*:\s*"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})"',
    re.IGNORECASE,
)

# Image / DPI suffixes — capture "@2x.png", "@1.5x.jpeg", "@.5x.svg"
_IMG_SUFFIX_RE = re.compile(
    r"@\d+(?:\.\d+)?x\.(?:png|jpe?g|gif|svg|webp|ico|avif)$",
    re.IGNORECASE,
)


# ── Exclusion lists ──────────────────────────────────────────────────────────

# Portal / platform / automation accounts — never direct seller contacts.
_GENERIC_LOCAL_PARTS: frozenset[str] = frozenset([
    "noreply", "no-reply", "no.reply", "nao-responder",
    "info", "contact", "contacto", "contactos", "hello",
    "support", "suporte", "help", "ajuda", "servicocliente",
    "admin", "administrator", "root", "webmaster", "postmaster",
    "dpo", "privacy", "privacidade", "rgpd", "gdpr",
    "legal", "juridico", "compliance",
    "abuse", "security", "seguranca",
    "marketing", "newsletter", "comunicacao",
    "billing", "faturacao", "cobranca",
    "api", "noticias", "news",
    "geral",     # OLX uses geral@olx.pt for outreach
    "example",   # placeholders
    "test", "teste",
])

# Known automation / infrastructure domains — always platform-side.
# Domains AND subdomains are rejected (suffix match), so
# `o1338151.ingest.sentry.io` matches the `sentry.io` entry.
#
# (Excluded: sapo.pt / olx.pt / imovirtual.com / idealista.pt / custojusto.pt
# — these ARE the portals, but can also be legitimate personal ISP emails;
# rejection is done at local-part level via _GENERIC_LOCAL_PARTS instead.)
_PORTAL_DOMAIN_SUFFIXES: tuple[str, ...] = (
    "adevinta.com",                                 # OLX parent group infra
    "example.com", "example.pt", "test.com", "test.pt",
    "localhost", "localhost.localdomain",
    "sentry.io", "ingest.sentry.io",                # error-tracking ingest
    "datadoghq.com", "datadog-internal.net",
    "google-analytics.com", "googletagmanager.com", "googleapis.com",
    "facebook.com", "fbcdn.net", "fb.com",
    "wixpress.com", "wix.com",
    "amplitude.com", "segment.io", "segment.com",
    "hotjar.com", "mixpanel.com",
    "cloudflare.com", "cf-ipfs.com",
    "hubspot.com", "hs-analytics.net",
)

# Additional portal automation prefixes — <prefix>@sapo.pt, <prefix>@olx.pt
# are always platform-side, while e.g. joao@sapo.pt is a real user.
# These are matched alongside _GENERIC_LOCAL_PARTS when the domain is one
# of the dual-use portal/ISP domains.
_PORTAL_HOST_DOMAINS: frozenset[str] = frozenset([
    "olx.pt", "imovirtual.com", "sapo.pt", "idealista.pt", "custojusto.pt",
    "standvirtual.com",
])

# Character sequences that almost always indicate a placeholder/garbage match
_SUSPICIOUS_SUBSTRINGS: frozenset[str] = frozenset([
    "@email.com", "@email.pt",
    "@domain.", "@dominio.",
    "your@", "seu@", "yours@",
])


# ── Public helpers ───────────────────────────────────────────────────────────

# Long hex local-parts (≥20 chars, all hex) are almost always telemetry/
# session IDs, not human addresses (e.g. Sentry ingest DSN project keys).
_HEX_HASH_RE = re.compile(r"^[a-f0-9]{20,}$", re.IGNORECASE)

# Asset versioning patterns like ``owl.carousel@2.3.4`` appearing in
# inline <script src> references get matched by the plain regex. The @-part
# after looks like a domain but is actually a semver number, so the
# "domain" matches ``^\d+\.\d+(?:\.\d+)?$``.
_VERSION_DOMAIN_RE = re.compile(r"^\d+\.\d+(?:\.\d+)?$")

# Lorem-ipsum tokens commonly seeded in theme templates as placeholder copy.
# Any of these appearing as the local-part or as a domain label almost
# always indicates template noise, not a real address.
_LOREM_TOKENS: frozenset[str] = frozenset([
    "lorem", "ipsum", "dolor", "amet", "consectetur", "adipiscing",
    "sed", "eiusmod", "tempor", "incidunt", "labore", "dolore",
    "magna", "aliqua", "veniam", "nostrud", "ullamco", "laboris",
    "nisi", "nisl", "donec", "duis", "aute", "irure", "reprehenderit",
    "voluptate", "cillum", "fugiat", "pariatur", "excepteur", "sint",
    "occaecat", "cupidatat", "proident", "culpa", "officia", "deserunt",
    "mollit", "anim", "est", "laborum", "incurred", "fruits",
])


def _domain_matches_portal_suffix(domain: str) -> bool:
    """True when domain equals or ends with any _PORTAL_DOMAIN_SUFFIXES entry."""
    domain = domain.lower()
    for suffix in _PORTAL_DOMAIN_SUFFIXES:
        if domain == suffix or domain.endswith("." + suffix):
            return True
    return False


def is_generic_portal_email(addr: str) -> bool:
    """
    True when the address is an automation / platform / placeholder contact
    rather than a direct seller email.

    Logic:
      * Automation-domain suffix match (sentry.io, fbcdn.net, etc.) → reject.
      * Dual-use portal/ISP domains (sapo.pt, olx.pt, etc.) → rejected only
        when the local-part is a known generic/automation username. A real
        user ``joao@sapo.pt`` is accepted.
      * Generic local-parts on ANY domain (noreply@, info@, dpo@, …) → rejected.
      * Long hex hashes as local-part (Sentry project keys) → rejected.
      * Placeholder patterns and single-char locals+domains → rejected.
    """
    if not addr or "@" not in addr:
        return True
    local, _, domain = addr.lower().partition("@")
    if _domain_matches_portal_suffix(domain):
        return True
    if local in _GENERIC_LOCAL_PARTS:
        return True
    if domain in _PORTAL_HOST_DOMAINS and local in _GENERIC_LOCAL_PARTS:
        return True
    if _HEX_HASH_RE.match(local):
        return True
    # Asset-version false positives: "owl.carousel@2.3.4"
    if _VERSION_DOMAIN_RE.match(domain):
        return True
    # Lorem-ipsum template placeholders in either side
    labels = [local] + domain.split(".")
    if any(lbl in _LOREM_TOKENS for lbl in labels):
        return True
    # TLD sanity: real TLDs are 2-10 chars, alphabetic only
    tld = domain.rsplit(".", 1)[-1]
    if not tld.isalpha() or not (2 <= len(tld) <= 10):
        return True
    # Consecutive hyphens in domain — invalid in real hostnames, indicates
    # placeholder / garbled template copy (e.g. "qp7it--.py").
    if "--" in domain:
        return True
    # Local-part with 10+ chars and almost no vowels is almost certainly
    # a session/hash token, not a human inbox. "aeiouy" covers English +
    # Portuguese; rejection threshold kept permissive (≥10% vowels) so
    # names like "mariaclara" and "joaopedro" still pass.
    if len(local) >= 10:
        vowels = sum(1 for c in local if c in "aeiouy")
        if vowels / len(local) < 0.10:
            return True
    if any(sub in addr.lower() for sub in _SUSPICIOUS_SUBSTRINGS):
        return True
    # Detect aN@aN.* placeholders (a@a.com, a@b.pt, 1@1.com)
    if len(local) <= 1 and len(domain.split(".")[0]) <= 1:
        return True
    return False


def _clean_candidate(raw: str) -> str:
    """Normalise a candidate email — lowercase, strip wrappers, sanity check."""
    addr = raw.strip().lower().rstrip(".,;:)\"'>]}")
    # Image-file captures: "logo@2x.png" — strip the dpi suffix and discard
    if _IMG_SUFFIX_RE.search(addr):
        return ""
    # Minimum format sanity
    if addr.count("@") != 1:
        return ""
    local, _, domain = addr.partition("@")
    if not local or not domain or "." not in domain:
        return ""
    if len(domain) < 4 or len(local) < 1:
        return ""
    return addr


def _deobfuscate(local: str, domain_parts: str) -> str:
    """Reassemble an obfuscated match into a real email string."""
    local = re.sub(r"\s+", "", local).lower()
    # Collapse every dot-equivalent into "."
    reassembled = re.sub(
        r"\s*(?:\[dot\]|\(dot\)|\{dot\}|\bdot\b|\bponto\b|\.)\s*",
        ".",
        domain_parts,
        flags=re.IGNORECASE,
    )
    reassembled = re.sub(r"\s+", "", reassembled).lower()
    if "." not in reassembled:
        return ""
    return f"{local}@{reassembled}"


def extract_emails(text_or_html: str) -> list[str]:
    """
    Return every unique, non-generic email address found in the input.

    Preserves source order (first occurrence wins) so callers using
    ``[0]`` get the most prominent candidate — usually the mailto href
    or JSON-LD email rather than an obfuscated mention deep in copy.
    """
    if not text_or_html:
        return []

    seen: set[str] = set()
    found: list[str] = []

    def _try_add(raw: str) -> None:
        addr = _clean_candidate(raw)
        if not addr or addr in seen:
            return
        if is_generic_portal_email(addr):
            return
        seen.add(addr)
        found.append(addr)

    # 1. mailto hrefs (highest priority — direct click-to-email links)
    for m in _MAILTO_RE.finditer(text_or_html):
        _try_add(m.group(1))

    # 2. JSON-LD and JSON blobs (structured, always-real)
    for m in _JSON_EMAIL_RE.finditer(text_or_html):
        _try_add(m.group(1))

    # 3. Plain regex (picks up visible addresses in description / footer)
    for m in _EMAIL_RE.finditer(text_or_html):
        _try_add(m.group(1))

    # 4. Obfuscated patterns — lowest priority, noisiest
    for m in _OBFUSCATED_RE.finditer(text_or_html):
        addr = _deobfuscate(m.group(1), m.group(2))
        if addr:
            _try_add(addr)

    return found


def extract_first_email(text_or_html: str) -> str:
    """Return the single best email candidate, or '' when none found."""
    results = extract_emails(text_or_html)
    return results[0] if results else ""

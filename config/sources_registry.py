"""
Sources Registry — catalogue of all known real-estate data sources for Portugal.

Each entry describes:
  key          : internal identifier used throughout the pipeline
  name         : human-readable name
  base_url     : root URL of the portal
  scraper_class: Python class to use (or None if not yet implemented)
  category     : portal | classified | marketplace | agency | public_record
  owner_bias   : typical owner type found on this source
  contact_rate : rough observed rate of leads that include direct contact (0.0-1.0)
  zones        : dict mapping canonical zone → source-specific slug/path
  is_active    : whether this source is currently enabled in the pipeline
  notes        : implementation notes / quirks

Adding a new source:
  1. Add entry to SOURCE_REGISTRY below.
  2. Create scrapers/<key>.py implementing BaseScraper.scrape_zone().
  3. Update scrapers/__init__.py.
  4. Update pipeline/runner.py scraper_map.
  5. Set is_active=True.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SourceMeta:
    key:           str
    name:          str
    base_url:      str
    scraper_class: Optional[str]          # "scrapers.olx.OLXScraper" or None
    category:      str                    # portal | classified | marketplace | agency | public_record
    owner_bias:    str                    # fsbo | agency | developer | mixed
    contact_rate:  float                  # 0.0–1.0 estimated rate of leads with direct contact
    zones:         dict[str, str]         # canonical zone → source slug/path
    is_active:     bool = True
    notes:         str  = ""


SOURCE_REGISTRY: dict[str, SourceMeta] = {

    # ── Active scrapers ──────────────────────────────────────────────────────

    "olx": SourceMeta(
        key           = "olx",
        name          = "OLX Portugal",
        base_url      = "https://www.olx.pt",
        scraper_class = "scrapers.olx.OLXScraper",
        category      = "classified",
        owner_bias    = "fsbo",
        contact_rate  = 0.55,
        zones         = {
            "Lisboa":   "lisboa",
            "Cascais":  "cascais",
            "Sintra":   "sintra",
            "Almada":   "almada",
            "Seixal":   "seixal",
            "Sesimbra": "sesimbra",
        },
        notes = "Server-side rendered. Largest classified in PT. Mix of owner+agency.",
    ),

    "imovirtual": SourceMeta(
        key           = "imovirtual",
        name          = "Imovirtual",
        base_url      = "https://www.imovirtual.com",
        scraper_class = "scrapers.imovirtual.ImovirtualScraper",
        category      = "portal",
        owner_bias    = "mixed",
        contact_rate  = 0.30,
        zones         = {
            "Lisboa":   "lisboa",
            "Cascais":  "cascais-gmina",
            "Sintra":   "sintra-gmina",
            "Almada":   "almada",
            "Seixal":   "seixal-gmina",
            "Sesimbra": "sesimbra-gmina",
        },
        notes = "Server-side rendered. Contact often hidden behind agency phone.",
    ),

    "idealista": SourceMeta(
        key           = "idealista",
        name          = "Idealista Portugal",
        base_url      = "https://www.idealista.pt",
        scraper_class = "scrapers.idealista.IdealistaScraper",
        category      = "portal",
        owner_bias    = "agency",
        contact_rate  = 0.20,
        zones         = {
            "Lisboa":   "comprar-casas/lisboa/",
            "Cascais":  "comprar-casas/cascais/",
            "Sintra":   "comprar-casas/sintra/",
            "Almada":   "comprar-casas/almada/",
            "Seixal":   "comprar-casas/seixal/",
            "Sesimbra": "comprar-casas/sesimbra/",
        },
        notes = "Requires Playwright. Strong bot detection. Agency-heavy.",
    ),

    "sapo": SourceMeta(
        key           = "sapo",
        name          = "Sapo Casa",
        base_url      = "https://casa.sapo.pt",
        scraper_class = "scrapers.sapo.SapoScraper",
        category      = "portal",
        owner_bias    = "mixed",
        contact_rate  = 0.35,
        zones         = {
            # Path slug format: /venda/{category}/{slug}/
            # Validated 2026-03: returns genuine zone-filtered listings
            "Lisboa":   "lisboa",
            "Cascais":  "cascais",
            "Sintra":   "sintra",
            "Almada":   "almada",
            "Seixal":   "seixal",
            "Sesimbra": "sesimbra",
        },
        notes = (
            "Major PT portal. URL pattern: /venda/{apartamentos|moradias}/{zone_slug}/. "
            "Validated 2026-03 — 28 listings/page, zone-filtered. "
            "Old ?address= param returned nationwide featured listings — do NOT use."
        ),
    ),

    "custojusto": SourceMeta(
        key           = "custojusto",
        name          = "Custojusto Imóveis",
        base_url      = "https://www.custojusto.pt",
        scraper_class = "scrapers.custojusto.CustojustoScraper",
        category      = "classified",
        owner_bias    = "fsbo",
        contact_rate  = 0.70,
        zones         = {
            "Lisboa":   "lisboa/imoveis",
            "Cascais":  "cascais/imoveis",
            "Sintra":   "sintra/imoveis",
            "Almada":   "almada/imoveis",
            "Seixal":   "seixal/imoveis",
            "Sesimbra": "sesimbra/imoveis",
        },
        is_active = True,
        notes = (
            "⚠️ EXPERIMENTAL — disabled pending URL-first strategy rewrite. "
            "Grid page is Next.js CSR: card content (title, price) not in server HTML. "
            "Only <a href> links visible. Fix: (1) extract listing URLs from grid, "
            "(2) fetch each detail page, (3) parse Product JSON-LD schema. "
            "High FSBO rate (0.70 contact_rate) makes it high-priority for Phase 2."
        ),
    ),

    "olx_marketplace": SourceMeta(
        key           = "olx_marketplace",
        name          = "OLX Portugal Marketplace",
        base_url      = "https://www.olx.pt",
        scraper_class = "scrapers.olx_marketplace.OLXMarketplaceScraper",
        category      = "marketplace",
        owner_bias    = "fsbo",
        contact_rate  = 0.40,
        zones         = {
            "Lisboa":   "lisboa",
            "Cascais":  "cascais",
            "Sintra":   "sintra",
            "Almada":   "almada",
            "Seixal":   "seixal",
            "Sesimbra": "sesimbra",
        },
        notes = "General marketplace sellers as property owner signals. Non-real-estate listings only.",
    ),

    "standvirtual": SourceMeta(
        key           = "standvirtual",
        name          = "Standvirtual",
        base_url      = "https://www.standvirtual.com",
        scraper_class = "scrapers.standvirtual.StandvirtualScraper",
        category      = "marketplace",
        owner_bias    = "fsbo",
        contact_rate  = 0.45,
        zones         = {
            "Lisboa":   "lisboa",
            "Cascais":  "cascais",
            "Sintra":   "sintra",
            "Almada":   "almada",
            "Seixal":   "seixal",
            "Sesimbra": "sesimbra",
        },
        notes = (
            "OLX Group auto marketplace. Private sellers of vehicles >=25k EUR "
            "as property owner signals. Requires Playwright (403 on httpx). "
            "Dealers/stands are filtered out."
        ),
    ),

    "linkedin": SourceMeta(
        key           = "linkedin",
        name          = "LinkedIn",
        base_url      = "https://www.linkedin.com",
        scraper_class = "scrapers.linkedin.LinkedInScraper",
        category      = "social",
        owner_bias    = "mixed",
        contact_rate  = 0.30,
        zones         = {
            "Lisboa":   "105723847",
            "Cascais":  "105723847",
            "Sintra":   "105723847",
            "Almada":   "105723847",
            "Seixal":   "105723847",
            "Sesimbra": "105723847",
        },
        is_active = True,
        notes = (
            "Requires manual LinkedIn login first: python -m scrapers.linkedin --login. "
            "Max 25 profiles/session. Not in default run_full() — run with --sources linkedin."
        ),
    ),

    # ── Planned scrapers (is_active=False — not yet implemented) ─────────────

    "era": SourceMeta(
        key           = "era",
        name          = "ERA Imobiliária Portugal",
        base_url      = "https://www.era.pt",
        scraper_class = None,
        category      = "agency",
        owner_bias    = "agency",
        contact_rate  = 0.80,
        zones         = {
            "Lisboa":   "comprar/apartamento/lisboa/",
            "Cascais":  "comprar/apartamento/cascais/",
            "Sintra":   "comprar/apartamento/sintra/",
            "Almada":   "comprar/apartamento/almada/",
        },
        is_active = False,
        notes = "Professional agency network. Good for market benchmarking. Agency-only contacts.",
    ),

    "remax": SourceMeta(
        key           = "remax",
        name          = "Remax Portugal",
        base_url      = "https://www.remax.pt",
        scraper_class = None,
        category      = "agency",
        owner_bias    = "agency",
        contact_rate  = 0.85,
        zones         = {
            "Lisboa":   "pt/listing?regionName=Lisboa",
            "Cascais":  "pt/listing?regionName=Cascais",
            "Sintra":   "pt/listing?regionName=Sintra",
        },
        is_active = False,
        notes = "React SPA — requires Playwright. Agency-only. Useful for price benchmarking.",
    ),

    "bpi_expresso": SourceMeta(
        key           = "bpi_expresso",
        name          = "BPI Expresso Imobiliário",
        base_url      = "https://www.expressooimobiliario.pt",
        scraper_class = None,
        category      = "marketplace",
        owner_bias    = "mixed",
        contact_rate  = 0.40,
        zones         = {
            "Lisboa":   "imoveis/venda/?zone=Lisboa",
            "Cascais":  "imoveis/venda/?zone=Cascais",
        },
        is_active = False,
        notes = "BPI bank-affiliated marketplace. Mix of bank repossessions and owner listings.",
    ),

    "century21": SourceMeta(
        key           = "century21",
        name          = "Century 21 Portugal",
        base_url      = "https://www.century21.pt",
        scraper_class = None,
        category      = "agency",
        owner_bias    = "agency",
        contact_rate  = 0.75,
        zones         = {
            "Lisboa":   "comprar/apartamentos/lisboa/",
            "Cascais":  "comprar/apartamentos/cascais/",
        },
        is_active = False,
        notes = "Standard agency. Good volume in Lisbon area. Contact via agency only.",
    ),

    "casa_sapo_arrendar": SourceMeta(
        key           = "casa_sapo_arrendar",
        name          = "Sapo Casa — Arrendamento",
        base_url      = "https://casa.sapo.pt",
        scraper_class = None,
        category      = "portal",
        owner_bias    = "mixed",
        contact_rate  = 0.40,
        zones         = {
            "Lisboa":   "arrendar/apartamentos/lisboa,lisboa,portugal",
            "Cascais":  "arrendar/apartamentos/cascais,lisboa,portugal",
        },
        is_active = False,
        notes = "Future: FRBO detection for rental opportunities.",
    ),
}


def get_active_sources() -> list[SourceMeta]:
    """Return all active (enabled) source definitions."""
    return [s for s in SOURCE_REGISTRY.values() if s.is_active]


def get_source(key: str) -> SourceMeta | None:
    """Look up a source by key. Returns None if not found."""
    return SOURCE_REGISTRY.get(key)


def list_source_keys(active_only: bool = True) -> list[str]:
    """Return list of source keys, optionally filtered to active ones."""
    if active_only:
        return [k for k, s in SOURCE_REGISTRY.items() if s.is_active]
    return list(SOURCE_REGISTRY.keys())


def source_owner_bias(source_key: str) -> str:
    """Return the expected owner_bias for a given source."""
    s = SOURCE_REGISTRY.get(source_key)
    return s.owner_bias if s else "unknown"

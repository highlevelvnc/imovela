"""
Sapo Casa Scraper — https://casa.sapo.pt

One of the largest real-estate portals in Portugal.
Mix of agency listings and direct-owner ads.

Rendering: Server-side (no JS needed for listing pages).
Pagination: ?page=N (1-indexed).
Rate-limiting: handled by BaseScraper (2-6s delays).

Selector strategy: multiple CSS selectors tried in order — Sapo has
changed its markup several times. The first working selector wins.
Debug mode: dumps HTML to /tmp/sapo_debug_<zone>.html when zero cards found.
"""
from __future__ import annotations

import re
from typing import Generator, Iterator

import httpx
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, ScraperResult
from utils.logger import get_logger

log = get_logger(__name__)


class SapoScraper(BaseScraper):
    SOURCE = "sapo"
    BASE_URL = "https://casa.sapo.pt"

    # Canonical zone → Sapo URL path slug (lowercase, no accents)
    # URL pattern: /venda/{category}/{zone_slug}/
    # Lisbon freguesias are handled by the parent Lisboa query; adjacent
    # municipalities (Oeiras, Amadora, Loures, etc.) have dedicated slugs.
    ZONE_SLUGS: dict[str, str] = {
        "Lisboa":   "lisboa",
        "Cascais":  "cascais",
        "Sintra":   "sintra",
        "Almada":   "almada",
        "Seixal":   "seixal",
        "Sesimbra": "sesimbra",
        # Adjacent Grande Lisboa municipalities
        "Oeiras":              "oeiras",
        "Amadora":             "amadora",
        "Loures":              "loures",
        "Odivelas":            "odivelas",
        "Vila-Franca-de-Xira": "vila-franca-de-xira",
        "Mafra":               "mafra",
        # Margem Sul / Setúbal district
        "Barreiro":  "barreiro",
        "Montijo":   "montijo",
        "Palmela":   "palmela",
        "Setubal":   "setubal",
        "Moita":     "moita",
        "Alcochete": "alcochete",
    }

    # Property categories to scrape per zone
    # /venda/{apartamentos|moradias|terrenos}/{zone}/ returns zone-filtered listings
    CATEGORIES = ["apartamentos", "moradias", "terrenos"]

    # Stop after this many pages per zone+category (each page ≈ 28 listings)
    MAX_PAGES = 15

    def __init__(self, max_pages: int = MAX_PAGES):
        super().__init__()
        self.max_pages = max_pages

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        # Lisbon freguesias fall back to the parent Lisboa query — skip to
        # avoid duplicating requests.
        if zone.startswith("Lisboa-"):
            log.debug("[sapo] zone={z} is a Lisboa freguesia — skipping", z=zone)
            return

        slug = self.ZONE_SLUGS.get(zone, zone.lower())
        log.info("Sapo: scraping zone={z} slug={s}", z=zone, s=slug)

        for category in self.CATEGORIES:
            for page in range(1, self.max_pages + 1):
                if page == 1:
                    url = f"{self.BASE_URL}/venda/{category}/{slug}/"
                else:
                    url = f"{self.BASE_URL}/venda/{category}/{slug}/?page={page}"

                resp = self._get(client, url)
                if not resp:
                    log.warning("Sapo: no response for {u}", u=url)
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                cards = self._find_cards(soup)

                if not cards:
                    log.debug("Sapo: zero cards on page {p} for {z}/{cat}", p=page, z=zone, cat=category)
                    # Save debug HTML when no cards found on page 1
                    if page == 1:
                        try:
                            with open(f"/tmp/sapo_debug_{zone}_{category}.html", "w", encoding="utf-8") as f:
                                f.write(resp.text[:80_000])
                        except Exception:
                            pass
                    break

                yielded = 0
                for card in cards:
                    item = self._parse_card(card, zone)
                    if item:
                        yield item
                        yielded += 1

                log.debug("Sapo: {cat} page {p}/{z} → {n} listings", cat=category, p=page, z=zone, n=yielded)

                # Stop if no items parsed from this page (all cards failed to parse)
                if yielded == 0:
                    break

    # ── Selector helpers ─────────────────────────────────────────────────────

    def _find_cards(self, soup: BeautifulSoup) -> list:
        """Try multiple selectors — Sapo has changed its markup several times."""
        selectors = [
            # Validated live 2026-03: homepage + search returns div.property cards
            "div.property",
            # Alternative structures seen historically
            "div.property-list-content article",
            "section.property-list article",
            "ul.property-list li.property-item",
            "[class*='PropertyCard']",
            "[class*='property-card']",
            "article[class*='listing']",
            "div[data-property-id]",
            "li[data-id]",
        ]
        for sel in selectors:
            cards = soup.select(sel)
            # Require at least 2 matches to avoid false positives on page chrome
            if cards and len(cards) >= 1:
                log.debug("Sapo: cards found with selector: {s} ({n})", s=sel, n=len(cards))
                return cards
        return []

    def _parse_card(self, card, zone: str) -> dict | None:
        # ── Title — validated live: div.property-type holds typology+bedrooms ─
        title_el = (
            card.select_one("div.property-type") or
            card.select_one("[class*='property-type']") or
            card.select_one("h2[class*='title']") or
            card.select_one("[class*='Title'] a") or
            card.select_one("a.property-info") or
            card.select_one("h2") or
            card.select_one("h3")
        )
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or len(title) < 3:
            return None

        # ── URL — Sapo uses a tracking redirect; extract listing ID from ?p= ─
        link_el = card.select_one("a.property-info") or card.select_one("a[href]")
        if not link_el:
            return None
        href = link_el.get("href", "")
        # Tracking URL pattern: gespub.casa.sapo.pt/...?p=NNNNNN&...
        # Extract p= and reconstruct canonical URL
        import re as _re
        pid_m = _re.search(r"[?&]p=(\d+)", href)
        if pid_m:
            external_id = pid_m.group(1)
            url = f"{self.BASE_URL}/pt/imovel/{external_id}/"
        else:
            url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
            external_id = self._id_from_url(url) or url[-24:]
            if not url or url == self.BASE_URL:
                return None

        # ── Price — validated: div.property-price-value ───────────────────────
        price_el = (
            card.select_one("div.property-price-value") or
            card.select_one("[class*='property-price-value']") or
            card.select_one("[class*='price']") or
            card.select_one("[class*='Price']")
        )
        price_raw = price_el.get_text(strip=True) if price_el else ""

        # ── Area — not exposed in card view on Sapo; left empty ──────────────
        area_raw = ""

        # ── Typology — extracted from title (property-type text is "Moradia T3")
        typology_raw = title  # normalizer will extract T1/T2/T3 from this

        # ── Location — validated: div.property-location ───────────────────────
        loc_el = (
            card.select_one("div.property-location") or
            card.select_one("[class*='property-location']") or
            card.select_one("[class*='location']") or
            card.select_one("address")
        )
        location_raw = loc_el.get_text(strip=True) if loc_el else zone

        # ── Zone guard — discard listings that don't belong to the target zone ─
        # When Sapo's zone URL is rate-limited or redirects, it can return
        # national results (Porto, Coimbra, etc.) for a Sintra/Cascais query.
        # Normalise the scraped location and reject anything off-zone.
        if location_raw and location_raw != zone:
            from utils.helpers import normalise_zone
            if normalise_zone(location_raw) != zone:
                return None

        return {
            "title":        title,
            "price_raw":    price_raw,
            "area_raw":     area_raw,
            "typology_raw": typology_raw,
            "location_raw": location_raw,
            "agency_name":  "",
            "url":          url,
            "external_id":  external_id,
            "zone_query":   zone,
            "_source":      "sapo",
        }

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        return bool(
            soup.select_one("a[rel='next']") or
            soup.select_one("[class*='pagination'] [class*='next']") or
            soup.select_one("a[aria-label='Próxima']") or
            soup.select_one("a[aria-label='Next']") or
            soup.select_one("[class*='pager'] a[class*='next']") or
            soup.select_one("a[class*='next-page']") or
            soup.select_one("button[class*='next']")
        )

    def _id_from_url(self, url: str) -> str:
        """Extract listing ID from Sapo URL."""
        # Pattern: /detalhe/<slug>-<id>/ or /<id>/
        m = re.search(r"-(\d+)/?$", url)
        if m:
            return m.group(1)
        m = re.search(r"/(\d{5,})/?", url)
        if m:
            return m.group(1)
        # Fallback: last path segment
        m = re.search(r"/([a-z0-9\-]{6,})/?$", url, re.IGNORECASE)
        return m.group(1) if m else ""

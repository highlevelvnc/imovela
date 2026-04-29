"""
Custojusto Scraper — https://www.custojusto.pt/imoveis

Portuguese classified ads portal — high FSBO rate (~70% direct owners).

Rendering: Next.js with __NEXT_DATA__ JSON embedded in HTML.
All listing data (title, price, location, seller, companyAd flag) is
available in the initial HTML via the __NEXT_DATA__ script tag — no
JavaScript execution needed.

Data source:
  Grid page → __NEXT_DATA__.props.pageProps.listItems[]
  Detail page → __NEXT_DATA__.props.pageProps.adData + userData

Key fields:
  companyAd (bool)     → True = professional/agency, False = particular
  locationNames (dict) → {district, county, parish}
  type (str)          → "sell" or "rent"
  price (int)         → numeric price in EUR
  name (str)          → seller display name

Pagination: ?page=N (1-indexed), 40 items per page.
"""
from __future__ import annotations

import json
import re
from typing import Iterator, Optional

import httpx
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from utils.logger import get_logger

log = get_logger(__name__)


class CustojustoScraper(BaseScraper):
    SOURCE = "custojusto"
    BASE_URL = "https://www.custojusto.pt"

    # Canonical zone → Custojusto slug.
    # Adjacent zones added 2026-04 — same /{municipality}/imoveis/ pattern.
    _MUNICIPALITIES: tuple[str, ...] = (
        "lisboa", "cascais", "sintra", "almada", "seixal", "sesimbra",
        "oeiras", "amadora", "loures", "odivelas", "vila-franca-de-xira", "mafra",
        "barreiro", "montijo", "palmela", "setubal", "moita", "alcochete",
    )
    _ZONE_KEYS: dict[str, str] = {
        "Lisboa": "lisboa", "Cascais": "cascais", "Sintra": "sintra",
        "Almada": "almada", "Seixal": "seixal", "Sesimbra": "sesimbra",
        "Oeiras": "oeiras", "Amadora": "amadora", "Loures": "loures",
        "Odivelas": "odivelas", "Vila-Franca-de-Xira": "vila-franca-de-xira",
        "Mafra": "mafra", "Barreiro": "barreiro", "Montijo": "montijo",
        "Palmela": "palmela", "Setubal": "setubal", "Moita": "moita",
        "Alcochete": "alcochete",
    }
    ZONE_SLUGS:        dict[str, str] = {k: f"{v}/imoveis"  for k, v in _ZONE_KEYS.items()}
    ZONE_RENTAL_SLUGS: dict[str, str] = {k: f"{v}/arrendar" for k, v in _ZONE_KEYS.items()}

    SCRAPE_RENTALS: bool = True
    DEFAULT_SLUG = "lisboa/imoveis"
    MAX_PAGES = 15

    def __init__(self, max_pages: int = MAX_PAGES):
        super().__init__()
        self.max_pages = max_pages

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        # Freguesia drill-downs handled only by Imovirtual; skip duplicates.
        if zone.startswith("Lisboa-"):
            log.debug("[custojusto] zone={z} is a Lisboa freguesia — skipping", z=zone)
            return

        # Buy listings
        slug = self.ZONE_SLUGS.get(zone, self.DEFAULT_SLUG)
        log.info("[custojusto] zone={z} BUY slug={s}", z=zone, s=slug)
        yield from self._scrape_slug(client, zone, slug)

        # Rental listings
        if self.SCRAPE_RENTALS:
            rental_slug = self.ZONE_RENTAL_SLUGS.get(zone)
            if rental_slug:
                log.info("[custojusto] zone={z} RENT slug={s}", z=zone, s=rental_slug)
                yield from self._scrape_slug(client, zone, rental_slug)

    def _scrape_slug(self, client: httpx.Client, zone: str, slug: str) -> Iterator[dict]:
        for page in range(1, self.max_pages + 1):
            url = f"{self.BASE_URL}/{slug}"
            if page > 1:
                url += f"?page={page}"

            resp = self._get(client, url)
            if not resp:
                log.warning("[custojusto] no response for {u}", u=url)
                break

            items = self._extract_next_data_items(resp.text)
            if items is None:
                log.warning("[custojusto] no __NEXT_DATA__ on page {p}", p=page)
                break
            if not items:
                log.debug("[custojusto] zero items page {p}/{z}", p=page, z=zone)
                break

            yielded = 0
            for item in items:
                parsed = self._parse_item(item, zone)
                if parsed:
                    yield parsed
                    yielded += 1

            log.info(
                "[custojusto] page {p}/{z} → {n} listings ({t} total in JSON)",
                p=page, z=zone, n=yielded, t=len(items),
            )

            if len(items) < 20:
                break

    # ── __NEXT_DATA__ extraction ─────────────────────────────────────────────

    @staticmethod
    def _extract_next_data_items(html: str) -> Optional[list[dict]]:
        """
        Extract listItems from the __NEXT_DATA__ JSON embedded in the HTML.
        Returns None if the tag is missing, [] if no items found.
        """
        m = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if not m:
            return None
        try:
            data = json.loads(m.group(1))
            return data["props"]["pageProps"].get("listItems", [])
        except (json.JSONDecodeError, KeyError) as e:
            log.debug("[custojusto] __NEXT_DATA__ parse error: {e}", e=e)
            return None

    @staticmethod
    def _extract_detail_data(html: str) -> tuple[dict, dict]:
        """
        Extract adData + userData from a detail page's __NEXT_DATA__.
        Returns (adData, userData) — either may be empty dict on failure.
        """
        m = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if not m:
            return {}, {}
        try:
            data = json.loads(m.group(1))
            pp = data["props"]["pageProps"]
            return pp.get("adData", {}), pp.get("userData", {})
        except (json.JSONDecodeError, KeyError):
            return {}, {}

    # ── Item parsing ─────────────────────────────────────────────────────────

    def _parse_item(self, item: dict, zone: str) -> dict | None:
        """Parse a single listItems[] entry into a raw listing dict."""
        title = (item.get("title") or "").strip()
        if not title or len(title) < 5:
            return None

        # Skip non-real-estate categories
        cat_name = (item.get("categoryName") or "").lower()
        _SKIP_CATS = {"informática", "informatica", "electrónica", "electronica",
                      "veículos", "veiculos", "emprego", "serviços", "servicos",
                      "animais", "desporto", "moda", "lazer", "bebé", "bebe"}
        if any(s in cat_name for s in _SKIP_CATS):
            return None

        # URL
        url_path = item.get("url", "")
        url = url_path if url_path.startswith("http") else f"{self.BASE_URL}{url_path}"

        # External ID
        external_id = str(item.get("listID", ""))

        # Price (already numeric in JSON)
        price_raw = item.get("price")
        price_str = str(price_raw) if price_raw else ""

        # Location — structured dict {district, county, parish}
        loc = item.get("locationNames", {})
        county = loc.get("county", "")
        parish = loc.get("parish", "")
        district = loc.get("district", "")
        location_raw = ", ".join(filter(None, [parish, county, district]))

        # Seller classification — companyAd is the reliable FSBO indicator
        is_company = bool(item.get("companyAd"))
        seller_name = (item.get("name") or "").strip()

        if is_company:
            is_owner = False
            owner_type = "agency"
        else:
            is_owner = True
            owner_type = "fsbo"

        # Listing type: sell vs rent
        listing_type = item.get("type", "sell")

        return {
            "title":          title,
            "price_raw":      price_str,
            "area_raw":       "",
            "location_raw":   location_raw,
            "municipality":   county,
            "parish":         parish,
            "description":    "",
            "contact_name":   seller_name if not is_company else "",
            "contact_phone":  "",
            "is_owner":       is_owner,
            "owner_type":     owner_type,
            "listing_type":   listing_type,
            "category_name":  item.get("categoryName", ""),
            "url":            url,
            "external_id":    external_id,
            "zone_query":     zone,
            "_source":        "custojusto",
        }

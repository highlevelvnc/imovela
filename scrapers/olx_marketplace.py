"""
OLX Portugal Marketplace scraper — active owner / general seller signals.
URL base: https://www.olx.pt/anuncios/q-{zone}/

Purpose
-------
Detect "active owners" — people who are actively selling items on OLX and
may also have property to sell/rent (or know someone who does).  These are
not real estate listings; they are signals that a person is an active OLX
user in a target zone.

What we scrape
--------------
- All non-real-estate listings in a zone (excluding /imoveis/ URLs)
- Seller name, location, price (product_value), item title (product_title)
- Phone via Playwright "Mostrar número" click (same pattern as OLXScraper)

How this differs from OLXScraper
---------------------------------
- OLXScraper targets /imoveis/ — real estate listings only
- This scraper targets /anuncios/q-{zone}/ — general marketplace
- Real estate cross-posts are filtered OUT (URL contains /imoveis/)
- The normalizer maps these to lead_type="active_owner", price=None

Source tag: "olx_marketplace"
Contact source tag: "olx_marketplace" (HTML) / "olx_mkt_playwright" (Playwright)
"""
from __future__ import annotations

import asyncio
import random
import re
from typing import Iterator
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from config.zone_config import get_pw_limit, get_random_user_agent
from scrapers.base import BaseScraper
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.olx.pt"

# Zone → search slug. Adjacent zones added 2026-04; freguesia keys
# resolve to their parent municipality (no freguesia paths on OLX).
ZONE_SLUGS: dict[str, str] = {
    "Lisboa": "lisboa", "Cascais": "cascais", "Sintra": "sintra",
    "Almada": "almada", "Seixal": "seixal", "Sesimbra": "sesimbra",
    "Oeiras": "oeiras", "Amadora": "amadora", "Loures": "loures",
    "Odivelas": "odivelas", "Vila-Franca-de-Xira": "vila-franca-de-xira",
    "Mafra": "mafra", "Barreiro": "barreiro", "Montijo": "montijo",
    "Palmela": "palmela", "Setubal": "setubal", "Moita": "moita",
    "Alcochete": "alcochete",
}

# Skip listings from real estate paths — handled by OLXScraper / ImovirtualScraper
_REAL_ESTATE_PATH_FRAGMENTS = frozenset({
    "/imoveis/",
    "imovirtual.com",
    "idealista.pt",
    "casa.sapo.pt",
})

# Same phone-reveal button selectors as OLXScraper
_PHONE_BTN_SELECTORS = [
    "button[data-testid='show-phone']",
    "a[data-cy='call-ad-button']",
    "button[data-testid='show-phone-button']",
    "button[data-testid='phone-show-number']",
    "a[data-testid='show-phone']",
    "button[class*='show-phone']",
    "a[class*='show-phone']",
]

# Max detail-page fetches per zone run
_MAX_DETAIL_FETCHES = 60

# Sanity cap: marketplace item prices above this are likely property listings
# that slipped through the URL filter — discard.
_PRODUCT_VALUE_CAP = 50_000.0


class OLXMarketplaceScraper(BaseScraper):
    """
    Scrapes OLX Portugal general marketplace listings to detect active owners.

    Args:
        max_pages:    Maximum pages to paginate per zone (default: 3 — shallower
                      than real estate scraper since signal density is lower).
        fetch_phone:  Attempt Playwright phone reveal when no phone in HTML.
    """

    SOURCE = "olx_marketplace"

    def __init__(
        self,
        max_pages: int = 8,
        fetch_phone: bool = True,
    ):
        super().__init__()
        self.max_pages    = max_pages
        self.fetch_phone  = fetch_phone
        self._pw_count    = 0  # reset per zone

    # ── Public interface ──────────────────────────────────────────────────────

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """Yield active-owner signal dicts for the given zone."""
        # Freguesia keys collapse to the parent municipality query — skip.
        if zone.startswith("Lisboa-"):
            log.debug("[olx_marketplace] zone={z} freguesia — skipping", z=zone)
            return
        slug          = ZONE_SLUGS.get(zone, zone.lower())
        detail_count  = 0
        self._pw_count = 0
        pw_limit      = get_pw_limit(zone)
        page          = 1

        while page <= self.max_pages:
            if page == 1:
                url = f"{BASE_URL}/anuncios/q-{slug}/"
            else:
                url = f"{BASE_URL}/anuncios/q-{slug}/?page={page}"

            log.info("[olx_mkt] zone={z} page={p} → {url}", z=zone, p=page, url=url)

            resp = self._get(client, url)
            if not resp:
                log.warning("[olx_mkt] No response zone={z} page={p} — stopping", z=zone, p=page)
                break

            soup  = BeautifulSoup(resp.text, "lxml")
            cards = self._find_cards(soup)

            if not cards:
                log.debug("[olx_mkt] No cards zone={z} page={p} — stopping", z=zone, p=page)
                break

            for card in cards:
                try:
                    item = self._parse_card(card, zone)
                    if not item:
                        continue

                    # Enrich with detail page (seller name, description)
                    if detail_count < _MAX_DETAIL_FETCHES:
                        self._enrich_with_detail(client, item)
                        detail_count += 1

                    # Playwright phone reveal
                    if (
                        self.fetch_phone
                        and not item.get("contact_phone")
                        and self._pw_count < pw_limit
                        and item.get("url")
                    ):
                        phone = self._fetch_phone_playwright(item["url"])
                        if phone:
                            item["contact_phone"] = phone
                            item["contact_source"] = "olx_mkt_playwright"
                            log.debug(
                                "[olx_mkt] Playwright phone found {u}: {p}",
                                u=item["url"], p=phone,
                            )
                        self._pw_count += 1

                    yield item

                except Exception as e:
                    log.debug("[olx_mkt] Card error zone={z}: {e}", z=zone, e=e)

            log.debug(
                "[olx_mkt] zone={z} page={p} → {n} cards processed",
                z=zone, p=page, n=len(cards),
            )

            if not self._has_next_page(soup):
                break
            page += 1

    # ── Card detection ────────────────────────────────────────────────────────

    @staticmethod
    def _find_cards(soup: BeautifulSoup) -> list:
        cards = soup.select("div[data-cy='l-card']")
        if cards:
            return cards
        return soup.select("li[data-cy='l-card']")

    # ── Card parsing ──────────────────────────────────────────────────────────

    def _parse_card(self, card, zone: str) -> dict | None:
        """
        Parse a single marketplace listing card.
        Skips real estate listings (handled by OLXScraper).
        """
        # ── Skip real estate cross-posts ──────────────────────────────────────
        for a_el in card.select("a[href]"):
            href = a_el.get("href", "")
            if any(frag in href for frag in _REAL_ESTATE_PATH_FRAGMENTS):
                return None

        # ── Title ─────────────────────────────────────────────────────────────
        title_el = (
            card.select_one("div[data-cy='ad-card-title'] h4") or
            card.select_one("h4") or
            card.select_one("h6")
        )
        title = title_el.get_text(strip=True) if title_el else None
        if not title or len(title) < 3:
            return None

        # ── URL ───────────────────────────────────────────────────────────────
        url = None
        for a_el in card.select("a[href]"):
            href = a_el.get("href", "")
            if "/d/" in href:
                url = href if href.startswith("http") else urljoin(BASE_URL, href)
                break
        if not url:
            return None

        # Skip any URL that ends up being a real estate path
        if any(frag in url for frag in _REAL_ESTATE_PATH_FRAGMENTS):
            return None

        # ── External ID ───────────────────────────────────────────────────────
        external_id = card.get("id") or self._extract_id_from_url(url)

        # ── Price (product_value) ─────────────────────────────────────────────
        price_el  = card.select_one("p[data-testid='ad-price']")
        price_raw = price_el.get_text(strip=True) if price_el else None
        product_value = self._parse_product_value(price_raw)

        # Discard if price exceeds sanity cap (likely a property listing)
        if product_value and product_value > _PRODUCT_VALUE_CAP:
            return None

        # ── Location ──────────────────────────────────────────────────────────
        loc_el = card.select_one("p[data-testid='location-date']")
        if loc_el:
            loc_text     = loc_el.get_text(strip=True)
            location_raw = loc_text.split(" - ")[0].strip() if " - " in loc_text else loc_text
        else:
            location_raw = zone

        return {
            "external_id":    external_id,
            "url":            url,
            "title":          title,
            "price_raw":      price_raw,
            "product_value":  product_value,
            "location_raw":   location_raw,
            "zone_query":     zone,
            "description":    None,      # filled by _enrich_with_detail
            "contact_name":   None,      # filled by _enrich_with_detail
            "contact_phone":  None,
            "contact_source": "olx_marketplace",
            "is_owner":       True,      # marketplace = private seller
        }

    # ── Detail page ───────────────────────────────────────────────────────────

    def _enrich_with_detail(self, client: httpx.Client, item: dict) -> None:
        """Fetch individual ad page for seller name and full description."""
        try:
            resp = self._get(client, item["url"])
            if not resp:
                return
            soup = BeautifulSoup(resp.text, "html.parser")

            # Description
            desc_el = soup.select_one("[data-testid='ad_description']")
            if desc_el:
                item["description"] = desc_el.get_text(" ", strip=True)[:2000]

            # Seller name
            seller_el = soup.select_one("[data-testid='user-profile-user-name']")
            if seller_el:
                item["contact_name"] = seller_el.get_text(strip=True)[:200]

            # Tel: link (sometimes present directly in detail HTML)
            for link in soup.select("a[href^='tel:']"):
                href  = link.get("href", "")
                phone = href.replace("tel:", "").strip()
                clean = re.sub(r"[\s\-\(\)]", "", phone).lstrip("+")
                if clean.startswith("351"):
                    clean = clean[3:]
                if re.match(r"^[2679]\d{8}$", clean):
                    item["contact_phone"]  = f"+351{clean}"
                    item["contact_source"] = "olx_marketplace"
                    break

        except Exception as e:
            log.debug("[olx_mkt] Detail error {url}: {e}", url=item.get("url", ""), e=e)

    # ── Playwright phone reveal ───────────────────────────────────────────────

    def _fetch_phone_playwright(self, url: str) -> str | None:
        try:
            return asyncio.run(self._async_fetch_phone(url))
        except RuntimeError:
            return None
        except Exception as e:
            log.debug("[olx_mkt] _fetch_phone_playwright error {u}: {e}", u=url, e=e)
            return None

    async def _async_fetch_phone(self, url: str) -> str | None:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            log.debug("[olx_mkt] Playwright not installed — phone reveal unavailable")
            return None

        from config.settings import settings

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=settings.headless_browser,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=get_random_user_agent(),
                locale="pt-PT",
                timezone_id="Europe/Lisbon",
                extra_http_headers={"Accept-Language": "pt-PT,pt;q=0.9"},
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            page = await context.new_page()
            # Block heavy resources to speed up load
            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,ico,css}",
                lambda route: route.abort(),
            )
            await page.route(
                "**googletagmanager**|**doubleclick**|**googlesyndication**|**facebook**",
                lambda route: route.abort(),
            )
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15_000)
                await asyncio.sleep(random.uniform(0.8, 1.6))

                # Locate phone reveal button
                btn = None
                for sel in _PHONE_BTN_SELECTORS:
                    btn = await page.query_selector(sel)
                    if btn:
                        break
                if not btn:
                    for handle in await page.query_selector_all("button, a"):
                        try:
                            text = (await handle.inner_text()).strip().lower()
                            if "mostrar" in text or "número" in text or "telefone" in text:
                                btn = handle
                                break
                        except Exception:
                            continue

                if btn:
                    try:
                        if not await btn.is_visible():
                            return None
                    except Exception:
                        pass
                    await btn.click()
                    try:
                        await page.wait_for_selector(
                            "a[href^='tel:']", timeout=4_000, state="attached"
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(random.uniform(0.4, 0.8))

                # Extract from tel: href
                for link in await page.query_selector_all("a[href^='tel:']"):
                    href  = (await link.get_attribute("href")) or ""
                    phone = href.replace("tel:", "").strip()
                    clean = re.sub(r"[\s\-\(\)]", "", phone).lstrip("+")
                    if clean.startswith("351"):
                        clean = clean[3:]
                    if re.match(r"^[2679]\d{8}$", clean):
                        return f"+351{clean}"

                # Text fallback
                page_text = await page.inner_text("body")
                m = re.search(
                    r"(?<!\d)(?:\+351|00351)?\s*([2679]\d{2}[\s\-]?\d{3}[\s\-]?\d{3})(?!\d)",
                    page_text,
                )
                if m:
                    digits = re.sub(r"[\s\-]", "", m.group(1))
                    if len(digits) == 9:
                        return f"+351{digits}"

            except Exception as e:
                log.debug("[olx_mkt] _async_fetch_phone error {u}: {e}", u=url, e=e)
            finally:
                await browser.close()

        return None

    # ── Pagination ────────────────────────────────────────────────────────────

    @staticmethod
    def _has_next_page(soup: BeautifulSoup) -> bool:
        return bool(
            soup.select_one("a[data-cy='pagination-forward']") or
            soup.select_one("a[rel='next']")
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_id_from_url(url: str) -> str | None:
        match = re.search(r"ID([a-zA-Z0-9]+)\.html", url)
        if match:
            return match.group(1)
        parts = url.rstrip("/").split("/")
        return parts[-1][:100] if parts else None

    @staticmethod
    def _parse_product_value(price_raw: str | None) -> float | None:
        """
        Parse marketplace item price from raw OLX price string.

        Examples:
          "25 €"          → 25.0
          "1 200 €"       → 1200.0
          "Preço livre"   → None
          "Troca / Oferta"→ None
        """
        if not price_raw:
            return None
        # Remove currency symbol and clean up spacing
        cleaned = re.sub(r"[€$£\s]", "", price_raw).replace(",", ".")
        # Remove thousands separators (OLX uses space or dot)
        cleaned = re.sub(r"\.(?=\d{3})", "", cleaned)
        try:
            val = float(cleaned)
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None

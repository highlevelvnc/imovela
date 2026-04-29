"""
Leilões Judiciais — judicial / fiscal property auction scraper.

Source
------
``e-leiloes.pt`` aggregates public auctions for judicial proceedings, fiscal
recoveries, and bankruptcy estates. Properties listed here are typically:

  * 30-50% below market value (auction bids start at debt + costs)
  * Often vacant or with eviction proceedings already complete
  * Dispatched fast — buyers must close in days, not months

For lead-engine purposes these are MOTIVATED-SELLER signals at the lowest
possible price floor. Even one matched deal pays for everything.

Architecture
------------
e-leiloes.pt is a Vue.js single-page app, so JS rendering is mandatory.
We use the same Playwright stealth profile we tuned for Idealista and the
banks. Search URL pattern (verified live 2026-04):

  https://www.e-leiloes.pt/Pesquisa.aspx?CategoriaPesquisa=2&page=N

CategoriaPesquisa=2 is the "Imóveis" filter; values 1, 3, 4 are vehicles,
machinery, miscellaneous (intentionally excluded).

The crawler iterates pages, parses each visible auction card, and emits
one raw listing per property. ``listing_type`` is set to ``auction`` so
downstream scoring can weight these rows higher (urgency + discount).
"""
from __future__ import annotations

import asyncio
import random
import re
from typing import Iterator, Optional
from urllib.parse import urljoin

import httpx

from config.settings import settings
from scrapers.base import BaseScraper, ScraperResult
from utils.logger import get_logger

log = get_logger(__name__)


_STEALTH_JS = r"""
Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});
Object.defineProperty(navigator, 'languages', {get: () => ['pt-PT', 'pt', 'en'], configurable: true});
Object.defineProperty(navigator, 'platform',  {get: () => 'MacIntel', configurable: true});
window.chrome = {runtime: {}, app: {}, csi: () => null, loadTimes: () => null};
"""

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class LeiloesScraper(BaseScraper):
    """e-leiloes.pt judicial/fiscal property auctions."""

    SOURCE   = "leiloes"
    BASE_URL = "https://www.e-leiloes.pt"

    # CategoriaPesquisa=2 → Imóveis only. Verified via the SPA's filter dropdown.
    SEARCH_URL_TPL = "{base}/Pesquisa.aspx?CategoriaPesquisa=2&page={page}"

    # Card grid selectors. The Vue components render with non-namespaced
    # bootstrap classes that drift; multiple fallbacks improve resilience.
    CARD_SELECTORS = [
        "div.card-leilao",
        "div[class*='leilao-item']",
        "article.leilao",
        "div.card",
    ]

    PAGINATION_NEXT_SELECTORS = [
        "a[aria-label='Próxima']",
        "li.pagination-next a",
        "a.proximo",
        "a[rel='next']",
    ]

    MAX_PAGES = 12
    PAGE_DELAY_MIN = 2.5
    PAGE_DELAY_MAX = 5.0

    def run(self, zones: list[str] = None) -> ScraperResult:
        zones = zones or settings.zones
        result = ScraperResult(source=self.SOURCE, batch_id=self.batch_id)
        log.info(
            "[leiloes] Starting national auction scrape "
            "(zones={z} used as a filter hint, downstream)",
            z=zones[:5],
        )

        try:
            for item in asyncio.run(self._scrape_async()):
                item["_source"]    = self.SOURCE
                item["_zone_query"] = "national"
                item["_batch_id"]  = self.batch_id
                item["lead_type"]  = "auction"
                item["owner_type_raw"] = "auction"
                item["is_owner"]   = False
                result.add(item)
        except RuntimeError:
            log.warning("[leiloes] Skipped — async event loop already running")
        except Exception as e:
            result.fail(f"leiloes crashed: {e}")

        result.finish()
        return result

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        # Auctions have no per-zone path — all-in-one async loop covers them.
        if False:
            yield {}
        return

    async def _scrape_async(self) -> list[dict]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            log.warning("[leiloes] Playwright not installed — auction site requires JS rendering")
            return []

        items: list[dict] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=settings.headless_browser,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                ],
            )
            try:
                context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent=_USER_AGENT,
                    locale="pt-PT",
                    timezone_id="Europe/Lisbon",
                    extra_http_headers={"Accept-Language": "pt-PT,pt;q=0.9"},
                )
                await context.add_init_script(_STEALTH_JS)

                for page_n in range(1, self.MAX_PAGES + 1):
                    page = await context.new_page()
                    url = self.SEARCH_URL_TPL.format(base=self.BASE_URL, page=page_n)
                    log.info("[leiloes] page={p} → {u}", p=page_n, u=url)

                    try:
                        await page.goto(url, wait_until="networkidle", timeout=30_000)
                        await asyncio.sleep(random.uniform(2.5, 4.0))
                    except Exception as e:
                        log.warning("[leiloes] page={p} navigation failed: {e}", p=page_n, e=e)
                        await page.close()
                        break

                    cards = await self._find_cards(page)
                    if not cards:
                        log.info("[leiloes] page={p} → 0 cards (selector? end of list?)", p=page_n)
                        await page.close()
                        break

                    for card in cards:
                        try:
                            data = await self._parse_card(card)
                            if data:
                                items.append(data)
                        except Exception as e:
                            log.debug("[leiloes] card parse: {e}", e=e)

                    has_next = await self._has_next(page)
                    await page.close()
                    if not has_next:
                        log.info("[leiloes] page={p} → end of pagination", p=page_n)
                        break

                    await asyncio.sleep(random.uniform(self.PAGE_DELAY_MIN, self.PAGE_DELAY_MAX))
            finally:
                await browser.close()

        log.info("[leiloes] collected {n} auction listings", n=len(items))
        return items

    # ── DOM helpers ───────────────────────────────────────────────────────

    async def _find_cards(self, page) -> list:
        for sel in self.CARD_SELECTORS:
            cards = await page.query_selector_all(sel)
            if cards:
                return cards
        return []

    async def _has_next(self, page) -> bool:
        for sel in self.PAGINATION_NEXT_SELECTORS:
            el = await page.query_selector(sel)
            if el:
                try:
                    if await el.is_enabled() and await el.is_visible():
                        return True
                except Exception:
                    return True
        return False

    async def _parse_card(self, card) -> Optional[dict]:
        async def text_of(sel: str) -> str:
            try:
                el = await card.query_selector(sel)
                return ((await el.inner_text()) or "").strip() if el else ""
            except Exception:
                return ""

        async def attr_of(sel: str, attr: str) -> str:
            try:
                el = await card.query_selector(sel)
                return ((await el.get_attribute(attr)) or "").strip() if el else ""
            except Exception:
                return ""

        # Title — usually the property type + location
        title = (
            await text_of("h3, h4, [class*='titulo']") or
            await text_of("[class*='descricao']")
        )
        if not title or len(title) < 5:
            return None

        # URL
        href = await attr_of("a[href*='/Anuncio'], a[href*='Detalhe'], a[href*='/imovel']", "href")
        if not href:
            href = await attr_of("a[href]", "href")
        if not href:
            return None
        url = urljoin(self.BASE_URL, href)
        external_id = self._extract_id(url)
        if not external_id:
            return None

        # Bid floor — usually rendered as "Valor base: 152 000 €" or similar
        price_raw = (
            await text_of("[class*='valor']") or
            await text_of("[class*='preco']") or
            await text_of("[class*='lance-base']")
        )

        # Auction date / status — keep raw for downstream scoring use
        auction_date_raw = await text_of("[class*='data']")
        auction_status   = await text_of("[class*='estado'], [class*='status']")

        location_raw = await text_of("[class*='localiz'], [class*='zona'], [class*='distrito']")
        area_raw     = await text_of("[class*='area']")
        typology_raw = await text_of("[class*='tipologia'], [class*='tipo']")
        image_url    = await attr_of("img[src]", "src")

        return {
            "external_id":   external_id,
            "url":           url,
            "title":         title[:200],
            "price_raw":     price_raw,
            "area_raw":      area_raw,
            "typology_raw":  typology_raw,
            "rooms_raw":     typology_raw,
            "location_raw":  location_raw,
            "image_url":     image_url,
            "agency_name":   "Leilão Público",
            "owner_type_raw": "auction",
            "is_owner":      False,
            "description":   None,
            "contact_name":  None,
            "contact_phone": None,
            "listing_type":  "auction",
            # Auction-specific raw fields — Normalizer can promote these later
            "auction_date_raw":   auction_date_raw,
            "auction_status_raw": auction_status,
        }

    @staticmethod
    def _extract_id(url: str) -> Optional[str]:
        # /Anuncio.aspx?id=12345 or /imovel/12345 patterns
        for pat in (r"id=(\d+)", r"/(\d{4,})/?", r"Numero=(\d+)"):
            m = re.search(pat, url)
            if m:
                return m.group(1)[:40]
        parts = [p for p in url.rstrip("/").split("/") if p]
        return parts[-1][:40] if parts else None

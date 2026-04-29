"""
Bank-owned real-estate scrapers — CGD, Millennium, Novo Banco, Santander.

Why this is a goldmine
----------------------
Banks list properties they recovered through default proceedings ("REOs" in
US parlance, "imóveis em recuperação" in PT). These typically:

  * Trade 15-30% below open-market comparable price
  * Are already legally clean (bank performed conveyance during recovery)
  * Have motivated sellers (banks must liquidate within accounting deadlines)

For Imovela this is a high-margin lead vector that nobody else is mining
systematically — the portals are siloed (each bank runs its own), and the
listings rarely cross-post to OLX/Imovirtual.

Architecture
------------
All four sites are JS-rendered single-page apps protected by Incapsula or
DataDome. Plain httpx returns shells without listing data, so we use a
shared Playwright base (`BankBaseScraper`) with the stealth profile we
already validated on Idealista.

Each per-bank subclass declares:

  * SOURCE          : registry slug
  * BASE_URL        : portal root
  * SEARCH_PATH     : URL pattern with ``{page}`` placeholder
  * MAX_PAGES       : safety cap
  * CARD_SELECTOR   : CSS for a listing tile on the search grid
  * FIELD_SELECTORS : per-field map (title, price, url, area, typology, location)
  * TYPE            : "bank_reo" — used by Normalizer + Scorer

Per-bank selectors are isolated so when a portal redesigns we patch only
the offending subclass without touching the others.

Status
------
Selectors are seeded from live spot-checks of the four portals in 2026-04.
They WILL drift — verify each subclass against the live HTML before
production runs and adjust the FIELD_SELECTORS map accordingly. The
shared base + stealth profile do all the heavy lifting.
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


# ── Stealth profile (re-uses Idealista patches) ─────────────────────────────

_BANK_STEALTH_JS = r"""
Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});
Object.defineProperty(navigator, 'languages', {get: () => ['pt-PT', 'pt', 'en'], configurable: true});
Object.defineProperty(navigator, 'platform',  {get: () => 'MacIntel', configurable: true});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8, configurable: true});
Object.defineProperty(navigator, 'deviceMemory',        {get: () => 8, configurable: true});
window.chrome = {runtime: {}, app: {}, csi: () => null, loadTimes: () => null};
const _origQuery = window.navigator.permissions ? window.navigator.permissions.query : null;
if (_origQuery) {
    window.navigator.permissions.query = (params) => (
        params.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : _origQuery(params)
    );
}
"""

_DEFAULT_VIEWPORT = {"width": 1366, "height": 768}
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ── Shared base ──────────────────────────────────────────────────────────────

class BankBaseScraper(BaseScraper):
    """
    Common Playwright pipeline for bank-owned real-estate portals.

    Subclasses override the SEARCH_PATH / CARD_SELECTOR / FIELD_SELECTORS
    class-level constants. Everything else (browser setup, stealth, paging,
    field extraction) is inherited unchanged.

    Banks publish properties NATIONAL — there is no per-zone URL slot.
    The scraper iterates ``settings.zones`` only to short-circuit when the
    user passes a custom zone list, but every zone returns the same global
    set, deduplicated downstream by external_id.
    """

    SOURCE:        str = "bank_unknown"
    BASE_URL:      str = ""
    SEARCH_PATH:   str = "/{page}"            # interpolated with current page
    MAX_PAGES:     int = 8
    PER_PAGE_HINT: int = 18                   # display estimate, not enforced
    LISTING_TYPE:  str = "venda"              # banks rarely list rentals
    CARD_SELECTOR:    str = "article, div.card"
    PAGINATION_NEXT_SELECTOR: str = "a[rel='next'], a.next-page"

    # Per-portal field selectors. Override in subclasses.
    FIELD_SELECTORS: dict[str, str] = {
        "title":     "h2, h3, [class*='title']",
        "price":     "[class*='price']",
        "area":      "[class*='area'], [class*='m2']",
        "typology":  "[class*='typology'], [class*='tipologia']",
        "location":  "[class*='location'], [class*='locality']",
        "url":       "a[href]",
        "image":     "img[src]",
    }

    # Inter-page sleep (seconds) to look human and stay below WAF radar.
    PAGE_DELAY_MIN = 3.0
    PAGE_DELAY_MAX = 7.0

    # ── Public hook ────────────────────────────────────────────────────────

    def run(self, zones: list[str] = None) -> ScraperResult:
        """
        Run a single national pass. Bank portals don't filter by zone, so we
        ignore the zones argument other than as a logging hint.
        """
        zones = zones or settings.zones
        result = ScraperResult(source=self.SOURCE, batch_id=self.batch_id)
        log.info(
            "[{src}] Starting national bank-REO scrape "
            "(zones argument {z} used only for logging)",
            src=self.SOURCE, z=zones[:5],
        )

        try:
            for item in asyncio.run(self._scrape_async()):
                item["_source"]    = self.SOURCE
                item["_zone_query"] = "national"
                item["_batch_id"]  = self.batch_id
                item["lead_type"]  = "bank_reo"
                item["owner_type_raw"] = "bank"
                item["is_owner"]   = False
                result.add(item)
        except RuntimeError:
            # Already inside an event loop (e.g. notebook) — graceful exit
            log.warning(
                "[{src}] Skipped — async event loop already running",
                src=self.SOURCE,
            )
        except Exception as e:
            result.fail(f"{self.SOURCE} crashed: {e}")

        result.finish()
        return result

    # ── Required by BaseScraper.run() default — unused on bank scrapers ────

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """Bank scrapers have no per-zone path; bypass via overridden run()."""
        if False:
            yield {}
        return

    # ── Async core ─────────────────────────────────────────────────────────

    async def _scrape_async(self) -> list[dict]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            log.warning(
                "[{src}] Playwright not installed — bank portal needs JS rendering",
                src=self.SOURCE,
            )
            return []

        from config.zone_config import get_random_user_agent

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
                    viewport=_DEFAULT_VIEWPORT,
                    user_agent=get_random_user_agent() or _USER_AGENT,
                    locale="pt-PT",
                    timezone_id="Europe/Lisbon",
                    extra_http_headers={"Accept-Language": "pt-PT,pt;q=0.9"},
                )
                await context.add_init_script(_BANK_STEALTH_JS)

                for page_n in range(1, self.MAX_PAGES + 1):
                    page = await context.new_page()
                    url = self.BASE_URL + self.SEARCH_PATH.format(page=page_n)
                    log.info("[{src}] page={p} → {u}",
                             src=self.SOURCE, p=page_n, u=url)
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=25_000)
                        await asyncio.sleep(random.uniform(2.0, 3.5))
                    except Exception as e:
                        log.warning("[{src}] page={p} navigation failed: {e}",
                                    src=self.SOURCE, p=page_n, e=e)
                        await page.close()
                        break

                    cards = await page.query_selector_all(self.CARD_SELECTOR)
                    if not cards:
                        log.info("[{src}] page={p} → 0 cards (selector? WAF? end of list?)",
                                 src=self.SOURCE, p=page_n)
                        await page.close()
                        break

                    for card in cards:
                        try:
                            data = await self._parse_card(card)
                            if data:
                                items.append(data)
                        except Exception as e:
                            log.debug("[{src}] card parse: {e}", src=self.SOURCE, e=e)

                    # Bail out when pagination control is absent
                    has_next = await page.query_selector(self.PAGINATION_NEXT_SELECTOR)
                    await page.close()
                    if not has_next:
                        log.info("[{src}] page={p} → no next-page control, stopping",
                                 src=self.SOURCE, p=page_n)
                        break

                    await asyncio.sleep(random.uniform(self.PAGE_DELAY_MIN, self.PAGE_DELAY_MAX))

            finally:
                await browser.close()

        log.info("[{src}] collected {n} listings total", src=self.SOURCE, n=len(items))
        return items

    # ── Field extraction ───────────────────────────────────────────────────

    async def _parse_card(self, card) -> Optional[dict]:
        async def text_of(sel: str) -> str:
            el = await card.query_selector(sel)
            if not el:
                return ""
            try:
                return (await el.inner_text() or "").strip()
            except Exception:
                return ""

        async def attr_of(sel: str, attr: str) -> str:
            el = await card.query_selector(sel)
            if not el:
                return ""
            try:
                return (await el.get_attribute(attr) or "").strip()
            except Exception:
                return ""

        title    = await text_of(self.FIELD_SELECTORS["title"])
        if not title or len(title) < 5:
            return None

        href     = await attr_of(self.FIELD_SELECTORS["url"], "href")
        url      = urljoin(self.BASE_URL, href) if href else ""
        external_id = self._extract_id(url) if url else None
        if not external_id:
            return None

        return {
            "external_id":   external_id,
            "url":           url,
            "title":         title,
            "price_raw":     await text_of(self.FIELD_SELECTORS["price"]),
            "area_raw":      await text_of(self.FIELD_SELECTORS["area"]),
            "typology_raw":  await text_of(self.FIELD_SELECTORS["typology"]),
            "rooms_raw":     await text_of(self.FIELD_SELECTORS["typology"]),
            "location_raw":  await text_of(self.FIELD_SELECTORS["location"]),
            "image_url":     await attr_of(self.FIELD_SELECTORS.get("image", ""), "src"),
            "agency_name":   self.bank_display_name,
            "owner_type_raw": "bank",
            "is_owner":      False,
            "description":   None,
            "contact_name":  self.bank_display_name,
            "contact_phone": None,
            "listing_type":  self.LISTING_TYPE,
        }

    # ── Helpers ────────────────────────────────────────────────────────────

    @property
    def bank_display_name(self) -> str:
        """Human-friendly bank name shown as agency/contact_name."""
        return self.SOURCE.replace("_", " ").title()

    @staticmethod
    def _extract_id(url: str) -> Optional[str]:
        # Most portals end with a numeric ID or alphanumeric slug
        m = re.search(r"(?:/imovel|/property|/detalhe|/p)/(\d+|[a-zA-Z0-9_-]+)/?", url)
        if m:
            return m.group(1)[:40]
        m = re.search(r"id[=/](\d+)", url)
        if m:
            return m.group(1)[:40]
        # Fallback: last path token
        parts = [p for p in url.rstrip("/").split("/") if p]
        return parts[-1][:40] if parts else None


# ── Subclasses — one per bank ────────────────────────────────────────────────

class CGDImoveisScraper(BankBaseScraper):
    """Caixa Geral de Depósitos / imobiliariocaixa.pt"""
    SOURCE   = "cgd_imoveis"
    BASE_URL = "https://www.imobiliariocaixa.pt"
    # Listing grid pages — verified via live spot-check 2026-04. Caixa uses
    # Angular; the search route lives at /pesquisa-de-imoveis with ?page=N.
    SEARCH_PATH = "/pesquisa-de-imoveis?page={page}"
    MAX_PAGES   = 12
    CARD_SELECTOR = "article.imovel-card, div.imovel-card, [class*='imovel-card']"
    PAGINATION_NEXT_SELECTOR = "a[aria-label='Próxima'], a.proximo, li.next a"
    FIELD_SELECTORS = {
        "title":    "[class*='titulo'], h3, h2",
        "price":    "[class*='preco'], [class*='price']",
        "area":     "[class*='area-util'], [class*='area']",
        "typology": "[class*='tipologia'], [class*='typology']",
        "location": "[class*='localizacao'], [class*='locality']",
        "url":      "a[href*='/imovel/']",
        "image":    "img[src]",
    }


class MillenniumImoveisScraper(BankBaseScraper):
    """Millennium BCP / imoveis.millenniumbcp.pt"""
    SOURCE   = "millennium_imoveis"
    BASE_URL = "https://imoveis.millenniumbcp.pt"
    SEARCH_PATH = "/imoveis-em-promocao/{page}"
    MAX_PAGES   = 10
    CARD_SELECTOR = "div.product-card, article.imovel, [class*='card-imovel']"
    PAGINATION_NEXT_SELECTOR = "a.next, li.next a, a[rel='next']"
    FIELD_SELECTORS = {
        "title":    "[class*='card-title'], h3, h2",
        "price":    "[class*='preco-valor'], [class*='price']",
        "area":     "[class*='area']",
        "typology": "[class*='tipologia']",
        "location": "[class*='localizacao'], [class*='localidade']",
        "url":      "a[href*='/imovel/'], a[href*='/detalhe']",
        "image":    "img[src]",
    }


class NovobancoImoveisScraper(BankBaseScraper):
    """Novo Banco / imoveis.novobanco.pt"""
    SOURCE   = "novobanco_imoveis"
    BASE_URL = "https://imoveis.novobanco.pt"
    SEARCH_PATH = "/imoveis-em-promocao?page={page}"
    MAX_PAGES   = 10
    CARD_SELECTOR = "article.realestate-card, div.imovel, [class*='card']"
    PAGINATION_NEXT_SELECTOR = "a[rel='next'], li.pagination__next a"
    FIELD_SELECTORS = {
        "title":    "[class*='title'], h3, h2",
        "price":    "[class*='price'], [class*='preco']",
        "area":     "[class*='area']",
        "typology": "[class*='tipologia'], [class*='rooms']",
        "location": "[class*='locality'], [class*='localizacao']",
        "url":      "a[href*='/imovel/']",
        "image":    "img[src]",
    }


class SantanderImoveisScraper(BankBaseScraper):
    """Santander Totta / imoveis.santander.pt — verified live 2026-04."""
    SOURCE   = "santander_imoveis"
    BASE_URL = "https://imoveis.santander.pt"
    SEARCH_PATH = "/imoveis-promo/{page}/"
    MAX_PAGES   = 12
    # Live HTML uses ASP.NET .NET MVC with bootstrap card layout
    CARD_SELECTOR = "div.card-imovel, div.imovel-card, article[class*='card']"
    PAGINATION_NEXT_SELECTOR = "a[aria-label='Next'], li.page-item.next a, a.proximo"
    FIELD_SELECTORS = {
        "title":    "h5, h4, [class*='title']",
        "price":    "[class*='preco'], [class*='valor']",
        "area":     "[class*='area']",
        "typology": "[class*='tipologia']",
        "location": "[class*='localidade'], [class*='zona']",
        "url":      "a[href*='/imovel/'], a[href*='/detalhe']",
        "image":    "img[src]",
    }

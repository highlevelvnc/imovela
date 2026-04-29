"""
Standvirtual Portugal scraper — high-value vehicle sellers as property owner signals.
URL base: https://www.standvirtual.com/carros/

Purpose
-------
Detect "active owners" — affluent individuals selling vehicles above €25,000
on Standvirtual (the largest auto marketplace in Portugal, OLX Group).
Selling a high-value vehicle is a strong signal of property ownership.

Strategy
--------
Standvirtual serves a React SPA behind CloudFlare / DataDome-style anti-bot.
Direct httpx requests return 403.  Full Playwright is required for every page.

What we scrape
--------------
- Listings with price >= €25,000 (URL price filter)
- Private sellers only — professional stands/dealers are filtered out
- Seller name, phone (Playwright reveal), location, car model (product_title),
  price (product_value)

Source tag: "standvirtual"
Contact source: "standvirtual" (HTML tel:) / "standvirtual_playwright" (button click)

Note: Standvirtual is part of OLX Group. Phone reveal mechanism is similar to OLX PT.
Selectors may need live validation — run with LOG_LEVEL=DEBUG to dump HTML on failure.
"""
from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Iterator

import httpx
from bs4 import BeautifulSoup

from config.zone_config import get_pw_limit, get_random_user_agent
from scrapers.base import BaseScraper
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.standvirtual.com"

# Minimum price filter — only capture sellers of high-value vehicles
_MIN_PRICE = 25_000

# Sanity cap — above this is likely a commercial fleet or data error
_PRODUCT_VALUE_CAP = 500_000.0

# Zone → URL slug (Standvirtual uses district/city in URL path).
# Adjacent zones added 2026-04 — expands car-owner signal coverage.
ZONE_SLUGS: dict[str, str] = {
    "Lisboa": "lisboa", "Cascais": "cascais", "Sintra": "sintra",
    "Almada": "almada", "Seixal": "seixal", "Sesimbra": "sesimbra",
    "Oeiras": "oeiras", "Amadora": "amadora", "Loures": "loures",
    "Odivelas": "odivelas", "Vila-Franca-de-Xira": "vila-franca-de-xira",
    "Mafra": "mafra", "Barreiro": "barreiro", "Montijo": "montijo",
    "Palmela": "palmela", "Setubal": "setubal", "Moita": "moita",
    "Alcochete": "alcochete",
}

# Keywords indicating professional dealer/stand — exclude from lead generation
_DEALER_KEYWORDS = frozenset({
    "stand", "automóveis", "automoveis", "auto ", "motors", "motor ",
    "comércio", "comercio", "lda", "lda.", "unipessoal", "s.a.",
    "concessionário", "concessionario", "car dealer", "automotiva",
    "garagem", "oficina", "importação", "importacao",
})

# Phone reveal button selectors — OLX Group shared patterns + Standvirtual specific
_PHONE_BTN_SELECTORS = [
    "button[data-testid='show-phone']",
    "a[data-testid='show-phone']",
    "button[data-testid='show-phone-button']",
    "a[data-cy='call-ad-button']",
    "button[data-testid='phone-show-number']",
    "button[class*='show-phone']",
    "a[class*='show-phone']",
    # Standvirtual-specific patterns (may differ from OLX)
    "button[class*='phone']",
    "a[class*='phone']",
]

# Max detail-page fetches per zone
_MAX_DETAIL_FETCHES = 60

# Stealth init script — minimal webdriver masking
_STEALTH_JS = (
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});"
)


class StandvirtualScraper(BaseScraper):
    """
    Scrapes Standvirtual Portugal for high-value vehicle sellers.
    Private sellers of vehicles >= €25k are property owner signals.

    Requires Playwright: run `playwright install chromium` once.
    """

    SOURCE = "standvirtual"

    def __init__(self, max_pages: int = 10, fetch_phone: bool = True):
        super().__init__()
        self.max_pages   = max_pages
        self.fetch_phone = fetch_phone
        self._pw_count   = 0

    # ── Public interface ──────────────────────────────────────────────────────

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """Yield active-owner signal dicts from Standvirtual for the given zone."""
        if zone.startswith("Lisboa-"):
            log.debug("[standvirtual] zone={z} freguesia — skipping", z=zone)
            return
        slug = ZONE_SLUGS.get(zone, zone.lower())
        self._pw_count = 0
        pw_limit = get_pw_limit(zone)

        # Standvirtual requires Playwright for listing pages (403 on httpx)
        log.info("[standvirtual] zone={z} starting Playwright scrape", z=zone)
        try:
            items = asyncio.run(self._async_scrape_zone(zone, slug, pw_limit))
            for item in items:
                yield item
        except ImportError:
            log.error("[standvirtual] Playwright not installed. Run: playwright install chromium")
        except Exception as e:
            log.error("[standvirtual] Scrape failed zone={z}: {e}", z=zone, e=e)

    async def _async_scrape_zone(self, zone: str, slug: str, pw_limit: int) -> list[dict]:
        """Playwright-based listing scrape with phone reveal."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError("playwright is not installed")

        from config.settings import settings

        items: list[dict] = []
        detail_count = 0

        vw = random.choice([1366, 1440, 1280, 1536])
        vh = random.choice([768, 800, 720, 900])

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=settings.headless_browser,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    f"--window-size={vw},{vh}",
                ],
            )
            context = await browser.new_context(
                viewport={"width": vw, "height": vh},
                user_agent=get_random_user_agent(),
                locale="pt-PT",
                timezone_id="Europe/Lisbon",
                extra_http_headers={
                    "Accept-Language": "pt-PT,pt;q=0.9,en-GB;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            await context.add_init_script(_STEALTH_JS)
            page = await context.new_page()

            # Block heavy resources to speed up load
            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,ico}",
                lambda route: route.abort(),
            )
            await page.route(
                "**googletagmanager**|**doubleclick**|**googlesyndication**|**facebook**|**sentry**",
                lambda route: route.abort(),
            )

            page_num = 1
            while page_num <= self.max_pages:
                # Standvirtual URL with price filter
                url = (
                    f"{BASE_URL}/carros/{slug}/"
                    f"?search%5Bfilter_float_price%3Afrom%5D={_MIN_PRICE}"
                )
                if page_num > 1:
                    url += f"&page={page_num}"

                log.info("[standvirtual] zone={z} page={p} → {url}", z=zone, p=page_num, url=url)

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(random.uniform(2.0, 4.0) if page_num == 1 else random.uniform(1.2, 2.8))

                    # Handle cookie consent
                    await self._handle_cookie_consent(page)

                    html = await page.content()

                    # Anti-bot detection
                    if self._is_blocked(html):
                        log.warning("[standvirtual] Blocked zone={z} page={p}", z=zone, p=page_num)
                        break

                    # Debug dump on first page
                    if page_num == 1:
                        self._debug_dump(html, zone)

                    page_items = self._parse_html(html, zone)

                    if not page_items:
                        log.debug("[standvirtual] No items page={p} — stopping", p=page_num)
                        if page_num == 1:
                            self._debug_dump(html, zone, force=True)
                        break

                    # Enrich each listing
                    for item in page_items:
                        if not item:
                            continue

                        # Phone reveal via Playwright on detail page
                        if (
                            self.fetch_phone
                            and not item.get("contact_phone")
                            and self._pw_count < pw_limit
                            and item.get("url")
                        ):
                            phone = await self._reveal_phone(page, item["url"])
                            if phone:
                                item["contact_phone"] = phone
                                item["contact_source"] = "standvirtual_playwright"
                                log.debug(
                                    "[standvirtual] Phone found {u}: {p}",
                                    u=item["url"], p=phone,
                                )
                            self._pw_count += 1
                            await asyncio.sleep(random.uniform(1.0, 2.5))

                        items.append(item)

                    # Check for next page
                    has_next = await page.query_selector(
                        "a[data-testid='pagination-step-forwards'], "
                        "a[rel='next'], "
                        "li.pagination-item--next a"
                    )
                    if not has_next:
                        break

                    page_num += 1

                except Exception as e:
                    log.warning("[standvirtual] Page error zone={z} page={p}: {e}", z=zone, p=page_num, e=e)
                    break

            await browser.close()

        log.info("[standvirtual] zone={z} → {n} items, {pw} phone reveals",
                 z=zone, n=len(items), pw=self._pw_count)
        return items

    # ── HTML parsing ──────────────────────────────────────────────────────────

    def _parse_html(self, html: str, zone: str) -> list[dict]:
        """Parse listing cards from Standvirtual HTML."""
        soup = BeautifulSoup(html, "lxml")
        items = []

        # Try multiple selector strategies (OLX Group varies selectors between verticals)
        cards = (
            soup.select("article[data-testid='listing-ad']") or
            soup.select("article[data-id]") or
            soup.select("div[data-testid='listing-ad']") or
            soup.select("article.ooa-1t80gpj") or  # Standvirtual class pattern
            soup.select("article.e1b25f6f18") or    # Alternative class
            soup.select("main article") or
            soup.select("[data-testid='search-results'] article")
        )

        if not cards:
            log.debug("[standvirtual] No cards found — selectors may need update")
            return []

        log.debug("[standvirtual] Found {n} cards", n=len(cards))

        for card in cards:
            try:
                item = self._parse_card(card, zone)
                if item:
                    items.append(item)
            except Exception as e:
                log.debug("[standvirtual] Card parse error: {e}", e=e)

        return items

    def _parse_card(self, card, zone: str) -> dict | None:
        """Extract data from a single Standvirtual listing card."""

        # ── Title (car model) ─────────────────────────────────────────────────
        title_el = (
            card.select_one("h1") or
            card.select_one("h2") or
            card.select_one("[data-testid='ad-title']") or
            card.select_one("a[title]")
        )
        title = None
        if title_el:
            title = title_el.get("title") or title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # ── URL ───────────────────────────────────────────────────────────────
        url = None
        for a_el in card.select("a[href]"):
            href = a_el.get("href", "")
            if "/anuncio/" in href or "/oferta/" in href or href.startswith(BASE_URL):
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                break
        # Fallback: first anchor with standvirtual.com
        if not url:
            for a_el in card.select("a[href]"):
                href = a_el.get("href", "")
                if "standvirtual.com" in href:
                    url = href
                    break
        if not url:
            return None

        # ── External ID ───────────────────────────────────────────────────────
        external_id = card.get("data-id") or card.get("id") or self._extract_id(url)

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = (
            card.select_one("[data-testid='ad-price']") or
            card.select_one("span.ooa-1bmnxg7") or   # Standvirtual price class
            card.select_one("h3") or                    # Price sometimes in h3
            card.select_one("[class*='price']")
        )
        price_raw = price_el.get_text(strip=True) if price_el else None
        product_value = self._parse_price(price_raw)

        # Skip if below minimum or above cap
        if product_value and product_value < _MIN_PRICE:
            return None
        if product_value and product_value > _PRODUCT_VALUE_CAP:
            return None

        # ── Seller type / Dealer filter ───────────────────────────────────────
        seller_el = (
            card.select_one("[data-testid='seller-type']") or
            card.select_one("[class*='seller']") or
            card.select_one("[class*='dealer']") or
            card.select_one("[data-testid='ad-owner-type']")
        )
        seller_badge = seller_el.get_text(strip=True).lower() if seller_el else ""

        # Exclude stands/dealers
        if any(kw in seller_badge for kw in ("stand", "profissional", "empresa", "dealer")):
            return None

        # Also check seller name against dealer keywords
        seller_name_el = card.select_one("[data-testid='seller-name']") or card.select_one("[class*='seller-name']")
        seller_name = seller_name_el.get_text(strip=True) if seller_name_el else None
        if seller_name and self._is_dealer(seller_name):
            return None

        # ── Location ──────────────────────────────────────────────────────────
        loc_el = (
            card.select_one("[data-testid='location']") or
            card.select_one("[data-testid='location-date']") or
            card.select_one("[class*='location']") or
            card.select_one("p[class*='city']")
        )
        location_raw = loc_el.get_text(strip=True) if loc_el else zone

        return {
            "external_id":    external_id,
            "url":            url,
            "title":          title,
            "price_raw":      price_raw,
            "product_value":  product_value,
            "location_raw":   location_raw,
            "zone_query":     zone,
            "description":    None,
            "contact_name":   seller_name,
            "contact_phone":  None,
            "contact_source": "standvirtual",
            "is_owner":       True,   # only private sellers pass dealer filter
        }

    # ── Phone reveal ──────────────────────────────────────────────────────────

    async def _reveal_phone(self, page, url: str) -> str | None:
        """Navigate to detail page and click phone reveal button."""
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(random.uniform(0.8, 1.6))

            # Try button selectors
            btn = None
            for sel in _PHONE_BTN_SELECTORS:
                btn = await page.query_selector(sel)
                if btn:
                    break

            # Text fallback
            if not btn:
                for handle in await page.query_selector_all("button, a"):
                    try:
                        text = (await handle.inner_text()).strip().lower()
                        if "mostrar" in text or "número" in text or "telefone" in text or "ligar" in text:
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
                    await page.wait_for_selector("a[href^='tel:']", timeout=4_000, state="attached")
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(0.4, 0.8))

            # Extract from tel: href
            for link in await page.query_selector_all("a[href^='tel:']"):
                href = (await link.get_attribute("href")) or ""
                phone = href.replace("tel:", "").strip()
                clean = re.sub(r"[\s\-\(\)]", "", phone).lstrip("+")
                if clean.startswith("351"):
                    clean = clean[3:]
                if re.match(r"^[2679]\d{8}$", clean):
                    return f"+351{clean}"

            # Text fallback — scan visible text for PT phone
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
            log.debug("[standvirtual] Phone reveal error {u}: {e}", u=url, e=e)

        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _handle_cookie_consent(self, page) -> None:
        """Dismiss cookie consent modal if present."""
        try:
            for sel in [
                "#onetrust-accept-btn-handler",
                "button[id*='accept']",
                "button[data-testid='cookie-accept']",
                "#didomi-notice-agree-button",
            ]:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await asyncio.sleep(0.5)
                    log.debug("[standvirtual] Cookie consent dismissed")
                    return
        except Exception:
            pass

    def _is_blocked(self, html: str) -> bool:
        """Return True if the response is a bot detection page."""
        if not html or len(html) < 500:
            return True
        sample = html[:3_000].lower()
        signals = (
            "datadome", "captcha", "bot detection", "access denied",
            "verifying you are human", "checking your browser",
            "cloudflare", "please enable cookies",
        )
        return any(s in sample for s in signals)

    @staticmethod
    def _is_dealer(name: str) -> bool:
        """Check if seller name matches dealer/stand keywords."""
        lower = name.lower()
        return any(kw in lower for kw in _DEALER_KEYWORDS)

    @staticmethod
    def _extract_id(url: str) -> str | None:
        """Extract listing ID from URL."""
        match = re.search(r"/(\d{6,})", url)
        if match:
            return match.group(1)
        parts = url.rstrip("/").split("/")
        return parts[-1][:100] if parts else None

    @staticmethod
    def _parse_price(price_raw: str | None) -> float | None:
        """Parse price from Standvirtual format (e.g. '32 500 EUR', '€45.000')."""
        if not price_raw:
            return None
        cleaned = re.sub(r"[€$£\s]", "", price_raw).replace("EUR", "").strip()
        # Handle both dot and space as thousands separators
        # "32.500" or "32 500" → 32500
        if "," in cleaned and "." in cleaned:
            # "32.500,00" format
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "." in cleaned:
            # Could be "32.500" (thousands) or "32.5" (decimal)
            parts = cleaned.split(".")
            if len(parts[-1]) == 3:
                cleaned = cleaned.replace(".", "")
            # else it's a decimal
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            val = float(cleaned)
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None

    def _debug_dump(self, html: str, zone: str, force: bool = False) -> None:
        """Save raw HTML for selector debugging."""
        if not force and os.environ.get("LOG_LEVEL", "").upper() != "DEBUG":
            return
        path = f"/tmp/standvirtual_debug_{zone.lower()}.html"
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            log.info("[standvirtual] HTML dump saved → {path}", path=path)
        except Exception:
            pass

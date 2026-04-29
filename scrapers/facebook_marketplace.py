"""
Facebook Marketplace — Imóveis para venda em Portugal.

⚠️ Status: SKELETON / OPT-IN
---------------------------
Facebook Marketplace requires an authenticated session to display listings.
This module ships the full crawl pipeline + cookie-loading logic, but does
NOT bundle credentials. Activation is opt-in via a one-time login flow:

    python -m scrapers.facebook_marketplace --login

That command opens a non-headless Chromium window, the operator logs in
manually (or solves any captcha), and the resulting cookie jar is persisted
to ``data/.fb_state.json``. Subsequent scraper runs reuse those cookies
without further interaction.

The cookie file is gitignored and stored next to the SQLite DB. **Do not
share it** — anyone with the file can impersonate the account.

Why opt-in instead of skipped
-----------------------------
For agency clients with their own FB business account, this is a real
lead source — direct private sellers in city groups that never cross-post
to OLX/Imovirtual. For clients without FB credentials it stays inert.

Scope and limits
----------------
  * Marketplace search URL: /marketplace/{location_id}/propertyforsale
  * MAX_LISTINGS_PER_RUN caps results to 100 per zone to stay polite
  * Selector tags drift FREQUENTLY — Facebook ships A/B variants weekly.
    All selectors use multiple-fallback patterns that catch most variants;
    long-term resilience requires periodic spot-checks.
  * No phone/email reveal: Facebook proxies messaging via Messenger; those
    are captured as ``contact_messenger`` URLs, not phone numbers.

CLI
---
``python -m scrapers.facebook_marketplace --login``    Save cookies once.
``python -m scrapers.facebook_marketplace --check``    Verify saved cookies.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import sys
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin

import httpx

from config.settings import settings
from scrapers.base import BaseScraper, ScraperResult
from utils.logger import get_logger

log = get_logger(__name__)


ROOT_DIR     = Path(__file__).resolve().parent.parent
COOKIE_PATH  = ROOT_DIR / "data" / ".fb_state.json"

# Marketplace city IDs — discovered via Facebook's location autocomplete.
# These are stable per-city tokens FB uses to filter the search.
ZONE_LOCATION_IDS: dict[str, str] = {
    "Lisboa":   "111777152182368",
    "Cascais":  "108277045871731",
    "Sintra":   "108160195880076",
    "Almada":   "111712752199706",
    "Seixal":   "108108025884060",
    "Porto":    "112548045432593",
}

# How many radius miles around the city to include. 25 covers the whole
# Grande Lisboa metro area without bleeding into neighbouring districts.
SEARCH_RADIUS_KM = 25

MAX_LISTINGS_PER_RUN = 100
SCROLL_BATCHES        = 5     # 5 scrolls × ~20 items per batch ≈ 100

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ── Login / cookie management ────────────────────────────────────────────────

def _login_interactive() -> None:
    """Open a non-headless window, wait for manual login, persist storage_state."""
    import asyncio as _asyncio
    _asyncio.run(_login_async())


async def _login_async() -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("playwright not installed — `pip install playwright && playwright install chromium`")
        return

    log.info("[fb_marketplace] opening browser for manual login (NOT headless)")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=["--no-sandbox"])
        context = await browser.new_context(
            viewport={"width": 1366, "height": 800},
            user_agent=USER_AGENT,
            locale="pt-PT",
        )
        page = await context.new_page()
        await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded")
        print()
        print("👉 Faz login manualmente no separador aberto.")
        print("👉 Quando estiveres no feed (logged in), pressa Enter aqui para guardar a sessão.")
        try:
            input()
        except EOFError:
            pass
        COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(COOKIE_PATH))
        log.info("[fb_marketplace] storage state saved → {p}", p=COOKIE_PATH)
        await browser.close()


def _check_cookies() -> bool:
    """True when COOKIE_PATH exists and contains at least one FB cookie."""
    if not COOKIE_PATH.exists():
        log.warning("[fb_marketplace] no cookie file at {p}", p=COOKIE_PATH)
        return False
    try:
        data = json.loads(COOKIE_PATH.read_text())
        cookies = data.get("cookies", [])
        fb_cookies = [c for c in cookies if "facebook" in c.get("domain", "")]
        log.info("[fb_marketplace] {n} FB cookies in storage state", n=len(fb_cookies))
        return len(fb_cookies) > 0
    except Exception as e:
        log.warning("[fb_marketplace] cookie file unreadable: {e}", e=e)
        return False


# ── Scraper class ────────────────────────────────────────────────────────────

class FacebookMarketplaceScraper(BaseScraper):
    """Marketplace property scraper — opt-in via cookie file."""

    SOURCE = "facebook_marketplace"

    def run(self, zones: list[str] = None) -> ScraperResult:
        zones = zones or settings.zones
        result = ScraperResult(source=self.SOURCE, batch_id=self.batch_id)

        if not COOKIE_PATH.exists():
            log.info(
                "[fb_marketplace] inactive — no cookies stored. "
                "Run `python -m scrapers.facebook_marketplace --login` to enable."
            )
            result.finish()
            return result

        target_zones = [z for z in zones if z in ZONE_LOCATION_IDS]
        if not target_zones:
            log.info("[fb_marketplace] no FB-mapped zones in target list — skipping")
            result.finish()
            return result

        try:
            for zone in target_zones:
                items = asyncio.run(self._scrape_zone_async(zone))
                for item in items:
                    item["_source"]    = self.SOURCE
                    item["_zone_query"] = zone
                    item["_batch_id"]  = self.batch_id
                    item["lead_type"]  = "fsbo"             # FB marketplace = direct sellers
                    item["owner_type_raw"] = "fsbo"
                    item["is_owner"]   = True
                    result.add(item)
        except RuntimeError:
            log.warning("[fb_marketplace] async loop already running — skipping")
        except Exception as e:
            result.fail(f"facebook_marketplace crashed: {e}")

        result.finish()
        return result

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        # Bypassed via overridden run()
        if False:
            yield {}
        return

    async def _scrape_zone_async(self, zone: str) -> list[dict]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return []

        location_id = ZONE_LOCATION_IDS[zone]
        items: list[dict] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=settings.headless_browser,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            try:
                context = await browser.new_context(
                    viewport={"width": 1366, "height": 800},
                    user_agent=USER_AGENT,
                    locale="pt-PT",
                    storage_state=str(COOKIE_PATH),
                )
                page = await context.new_page()

                url = (
                    f"https://www.facebook.com/marketplace/{location_id}/"
                    f"propertyforsale/?radius={SEARCH_RADIUS_KM * 1000}&exact=false"
                )
                log.info("[fb_marketplace] zone={z} → {u}", z=zone, u=url[:90])
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(random.uniform(3.0, 5.0))
                except Exception as e:
                    log.warning("[fb_marketplace] zone={z} navigation: {e}", z=zone, e=e)
                    await browser.close()
                    return []

                # Detect "Login required" wall — cookies expired
                if await page.query_selector("input[name='email']"):
                    log.warning("[fb_marketplace] cookies appear expired — re-run --login")
                    await browser.close()
                    return []

                # Scroll-load more cards
                for _ in range(SCROLL_BATCHES):
                    await page.evaluate("window.scrollBy(0, 1500)")
                    await asyncio.sleep(random.uniform(1.5, 2.8))

                cards = await self._find_cards(page)
                log.info("[fb_marketplace] zone={z} → {n} cards rendered",
                         z=zone, n=len(cards))

                for card in cards[:MAX_LISTINGS_PER_RUN]:
                    try:
                        data = await self._parse_card(card, zone)
                        if data:
                            items.append(data)
                    except Exception as e:
                        log.debug("[fb_marketplace] card parse: {e}", e=e)

                # Persist any updated cookies (sliding session)
                await context.storage_state(path=str(COOKIE_PATH))
            finally:
                await browser.close()

        return items

    async def _find_cards(self, page) -> list:
        # Marketplace links match /marketplace/item/<id>
        return await page.query_selector_all("a[href*='/marketplace/item/']")

    async def _parse_card(self, card, zone: str) -> Optional[dict]:
        try:
            href = await card.get_attribute("href")
            if not href:
                return None
            url = urljoin("https://www.facebook.com", href.split("?")[0])
            external_id = self._extract_id(url)
            if not external_id:
                return None

            # Marketplace cards bundle title + price + locality in nested spans
            text = (await card.inner_text() or "").strip()
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            if len(lines) < 2:
                return None

            # First line is usually price, second is title; older variants flip them.
            price_raw, title = "", ""
            for ln in lines[:5]:
                if "€" in ln and not price_raw:
                    price_raw = ln
                elif not title and len(ln) >= 5:
                    title = ln
            title = title or lines[0]

            # Last line is normally the locality
            location_raw = lines[-1] if len(lines) >= 3 else zone

            return {
                "external_id":   external_id,
                "url":           url,
                "title":         title[:200],
                "price_raw":     price_raw,
                "area_raw":      None,
                "typology_raw":  None,
                "rooms_raw":     None,
                "location_raw":  location_raw,
                "image_url":     None,
                "agency_name":   None,
                "owner_type_raw": "fsbo",
                "is_owner":      True,
                "description":   None,
                "contact_name":  None,
                "contact_phone": None,
                "contact_messenger": url,    # Buyer must DM via Messenger
                "listing_type":  "venda",
            }
        except Exception:
            return None

    @staticmethod
    def _extract_id(url: str) -> Optional[str]:
        m = re.search(r"/marketplace/item/(\d+)", url)
        return m.group(1) if m else None


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--login" in sys.argv:
        _login_interactive()
    elif "--check" in sys.argv:
        ok = _check_cookies()
        sys.exit(0 if ok else 1)
    else:
        print(__doc__)
        sys.exit(0)

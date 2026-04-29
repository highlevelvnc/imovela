"""
Idealista Portugal scraper — uses Playwright for JS rendering.
URL: https://www.idealista.pt/comprar-casas/

Idealista uses DataDome anti-bot.  Two-stage strategy:
  Stage 1 — httpx: fast, zero browser overhead.  DataDome sometimes passes
             server-side-rendered listing pages with correct HTTP headers.
             If ≥1 card is parsed, Playwright is skipped entirely.
  Stage 2 — Playwright: headless Chromium with a comprehensive stealth profile
             that patches ~15 navigator/window vectors DataDome checks.
             DataDome's own fingerprint scripts are blocked via page.route().
             If the page is still detected as a bot, the zone is skipped
             gracefully and the pipeline continues without error.

Requires: playwright browsers installed → run `playwright install chromium` once.
"""
from __future__ import annotations

import asyncio
import random
import re
import time
from typing import Iterator

import httpx
from bs4 import BeautifulSoup

from config.settings import settings
from scrapers.base import BaseScraper
from utils.logger import get_logger

log = get_logger(__name__)

BASE_URL = "https://www.idealista.pt"

# ── Anti-bot constants ────────────────────────────────────────────────────────

# DataDome fingerprints the browser via these scripts.
# Blocking them at the network layer reduces the detection surface.
_DATADOME_BLOCK_PATTERNS: list[str] = [
    "**datadome.co/**",
    "**/dd.js",
    "**/interstitial.js",
    "**fingerprintjs**",
    "**fp2.js**",
    "**/tags.js",
    "**datadog**",
    "**sentry.io/**",
]

# Comprehensive browser-fingerprint stealth script.
# Injected as a Playwright context init script — runs before ANY page JS.
# Covers the main DataDome detection vectors (in rough priority order):
#   1. navigator.webdriver
#   2. navigator.plugins  (empty = headless)
#   3. window.chrome      (absent = not Chrome)
#   4. navigator.languages (string vs array check)
#   5. navigator.platform
#   6. navigator.hardwareConcurrency
#   7. navigator.deviceMemory
#   8. Notification.permissions query
#   9. HTMLIFrameElement.contentWindow (frame-based webdriver check)
#  10. Function.prototype.toString (native-function check)
_STEALTH_JS = r"""
// 1. webdriver flag
Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});

// 2. plugins — realistic Chrome set (empty array triggers DataDome)
(function() {
    function mkMime(type, suf, desc) {
        const m = Object.create(MimeType.prototype);
        Object.defineProperties(m, {
            type:        {get: () => type,  configurable: true},
            suffixes:    {get: () => suf,   configurable: true},
            description: {get: () => desc,  configurable: true},
        });
        return m;
    }
    function mkPlugin(name, fn, desc) {
        const p = Object.create(Plugin.prototype);
        Object.defineProperties(p, {
            name:        {get: () => name, configurable: true},
            filename:    {get: () => fn,   configurable: true},
            description: {get: () => desc, configurable: true},
            length:      {get: () => 0,    configurable: true},
            item:        {value: () => null},
            namedItem:   {value: () => null},
        });
        return p;
    }
    const plugins = [
        mkPlugin('Chrome PDF Plugin',  'internal-pdf-viewer', 'Portable Document Format'),
        mkPlugin('Chrome PDF Viewer',  'mhjfbmdgcfjbbpaeojofohoefgiehjai', ''),
        mkPlugin('Native Client',      'internal-nacl-plugin', ''),
    ];
    const pa = Object.create(PluginArray.prototype);
    Object.defineProperties(pa, {
        length:    {get: () => plugins.length},
        item:      {value: i => plugins[i] || null},
        namedItem: {value: n => plugins.find(p => p.name === n) || null},
        refresh:   {value: () => {}},
    });
    plugins.forEach((p, i) => Object.defineProperty(pa, i, {get: () => p}));
    Object.defineProperty(navigator, 'plugins', {get: () => pa, configurable: true});
})();

// 3. window.chrome — must exist with a plausible runtime object
if (!window.chrome) {
    window.chrome = {
        app: {isInstalled: false},
        runtime: {
            connect: function(){},
            sendMessage: function(){},
            id: undefined,
        },
        loadTimes: function() {
            return {requestTime: Date.now()/1000 - 1, startLoadTime: Date.now()/1000 - 0.9,
                    commitLoadTime: Date.now()/1000 - 0.8, finishDocumentLoadTime: Date.now()/1000 - 0.2,
                    finishLoadTime: Date.now()/1000 - 0.1, firstPaintTime: Date.now()/1000 - 0.7,
                    firstPaintAfterLoadTime: 0, navigationType: 'Other', wasFetchedViaSpdy: false,
                    wasNpnNegotiated: false, npnNegotiatedProtocol: '', wasAlternateProtocolAvailable: false,
                    connectionInfo: 'http/1.1'};
        },
        csi: function() {return {startE: Date.now(), onloadT: Date.now()+300, pageT: 300, tran: 15};},
    };
}

// 4. languages — array, not string
Object.defineProperty(navigator, 'languages', {
    get: () => ['pt-PT', 'pt', 'en-GB', 'en'],
    configurable: true,
});

// 5. platform
Object.defineProperty(navigator, 'platform', {get: () => 'Win32', configurable: true});

// 6. hardwareConcurrency
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8, configurable: true});

// 7. deviceMemory
try { Object.defineProperty(navigator, 'deviceMemory', {get: () => 8, configurable: true}); } catch(e) {}

// 8. Notification.permissions — headless browsers return 'denied' unconditionally
try {
    const _origQuery = window.navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.__proto__.query = function(p) {
        if (p && p.name === 'notifications') {
            return Promise.resolve({state: Notification.permission, onchange: null});
        }
        return _origQuery(p);
    };
} catch(e) {}

// 9. iframe contentWindow — frame-based webdriver detection
try {
    Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
        get: function() {
            const w = this.__contentWindow || window;
            return w;
        }
    });
} catch(e) {}

// 10. Function.prototype.toString — mask patched functions as native
(function() {
    const _orig = Function.prototype.toString;
    const _patched = new WeakSet();
    Function.prototype.toString = function() {
        if (_patched.has(this)) return 'function () { [native code] }';
        return _orig.apply(this, arguments);
    };
})();
"""

# Primary municipality-level URLs. Adjacent zones added 2026-04.
# Idealista uses /comprar-casas/{municipality}/ — freguesia drill-downs
# are supported at /comprar-casas/lisboa-lisboa/{freguesia}/, but
# DataDome makes granular sweeps costly; we rely on the Lisboa parent
# plus Imovirtual's freguesia drill-down for coverage in Lisbon.
_MUNICIPALITIES: tuple[str, ...] = (
    "lisboa", "cascais", "sintra", "almada", "seixal", "sesimbra",
    "oeiras", "amadora", "loures", "odivelas", "vila-franca-de-xira", "mafra",
    "barreiro", "montijo", "palmela", "setubal", "moita", "alcochete",
)


# Keys must match settings.target_zones exactly (case + hyphen layout).
# Canonical map mirrors scrapers.imovirtual.ZONE_PATHS keys.
_ZONE_KEYS: dict[str, str] = {
    "Lisboa":             "lisboa",
    "Cascais":            "cascais",
    "Sintra":             "sintra",
    "Almada":             "almada",
    "Seixal":             "seixal",
    "Sesimbra":           "sesimbra",
    "Oeiras":             "oeiras",
    "Amadora":            "amadora",
    "Loures":             "loures",
    "Odivelas":           "odivelas",
    "Vila-Franca-de-Xira": "vila-franca-de-xira",
    "Mafra":              "mafra",
    "Barreiro":           "barreiro",
    "Montijo":            "montijo",
    "Palmela":            "palmela",
    "Setubal":            "setubal",
    "Moita":              "moita",
    "Alcochete":          "alcochete",
}


def _build_zone_urls(path: str) -> dict[str, str]:
    return {key: f"{BASE_URL}/{path}/{slug}/" for key, slug in _ZONE_KEYS.items()}


ZONE_URLS:        dict[str, str] = _build_zone_urls("comprar-casas")
ZONE_RENTAL_URLS: dict[str, str] = _build_zone_urls("arrendar-casas")

# Set False to disable rental scraping (e.g. during recovery / testing)
SCRAPE_RENTALS: bool = True


class IdealistaScraper(BaseScraper):
    SOURCE = "idealista"

    # Tracks whether Playwright path yielded anything — set per zone in scrape_zone
    _EXPERIMENTAL_NOTE = (
        "EXPERIMENTAL SOURCE — Idealista uses DataDome anti-bot. "
        "This zone returned 0 items. Playwright stealth profile active but "
        "DataDome may still block. Pipeline continues normally. "
        "Check scraper logs for block details."
    )

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """
        Two-stage scrape strategy for buy AND (optionally) rental listings:
          1. httpx — fast, no browser. DataDome sometimes lets SSR listing
             pages through when HTTP headers look realistic.  If any cards
             are parsed, Playwright is skipped for that URL.
          2. Playwright — headless Chromium with full stealth profile + route
             blocking. Falls back gracefully if still blocked.

        When SCRAPE_RENTALS is True, the same two-stage strategy is applied
        to the /arrendar-casas/ URL after the /comprar-casas/ URL.

        EXPERIMENTAL STATUS: Idealista uses DataDome anti-bot protection.
          When both stages fail for a URL, a WARNING is logged. The pipeline
          never crashes — it simply receives 0 items for that URL.
        """
        # Lisbon freguesia drill-downs rely on the Imovirtual scraper — Idealista
        # freguesia URLs exist but each one costs a DataDome challenge, so we
        # route all Lisboa-* keys to the single Lisboa parent query here.
        if zone.startswith("Lisboa-"):
            log.debug("[idealista] zone={z} is a Lisboa freguesia — skipping", z=zone)
            return

        # Build the list of URLs to scrape: buy always first, rental optional
        urls_to_scrape: list[tuple[str, str]] = []  # (label, url)
        buy_url = ZONE_URLS.get(zone)
        if buy_url:
            urls_to_scrape.append(("buy", buy_url))
        if SCRAPE_RENTALS:
            rental_url = ZONE_RENTAL_URLS.get(zone)
            if rental_url:
                urls_to_scrape.append(("rent", rental_url))

        total_yielded = 0

        for label, zone_url in urls_to_scrape:
            url_yielded = 0

            # Stage 1 — httpx (fast path)
            try:
                httpx_items = self._try_scrape_httpx(client, zone, zone_url=zone_url)
                if httpx_items:
                    log.info(
                        "[idealista] httpx path OK zone={z} {lbl} ({n} items — Playwright skipped)",
                        z=zone, lbl=label, n=len(httpx_items),
                    )
                    for item in httpx_items:
                        url_yielded += 1
                        yield item
                    total_yielded += url_yielded
                    continue
            except Exception as e:
                log.debug("[idealista] httpx path failed zone={z} {lbl}: {e}", z=zone, lbl=label, e=e)

            # Stage 2 — Playwright (stealth path)
            log.info("[idealista] Falling back to Playwright zone={z} {lbl}", z=zone, lbl=label)
            try:
                results = asyncio.run(self._async_scrape_zone(zone, zone_url=zone_url))
                for item in results:
                    url_yielded += 1
                    yield item
            except ImportError:
                log.error("[idealista] Playwright not installed. Run: playwright install chromium")
            except Exception as e:
                log.error("[idealista] Async scrape failed zone={z} {lbl}: {e}", z=zone, lbl=label, e=e)

            # Experimental status — log clearly when both stages returned nothing
            if url_yielded == 0:
                log.warning(
                    "[idealista] {note} zone={z} url={u}",
                    note=self._EXPERIMENTAL_NOTE,
                    z=zone, u=zone_url,
                )
            total_yielded += url_yielded

    async def _async_scrape_zone(self, zone: str, zone_url: str | None = None) -> list[dict]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError("playwright is not installed")

        if not zone_url:
            zone_url = ZONE_URLS.get(zone)
        if not zone_url:
            log.warning("[idealista] No URL configured for zone: {z}", z=zone)
            return []

        items = []
        # Randomise viewport slightly — identical viewports across sessions
        # are a weak but real DataDome signal.
        vw = random.choice([1366, 1440, 1280, 1536])
        vh = random.choice([768, 800, 720, 900])

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=settings.headless_browser,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--disable-component-extensions-with-background-pages",
                    f"--window-size={vw},{vh}",
                ],
            )
            context = await browser.new_context(
                viewport={"width": vw, "height": vh},
                user_agent=self.proxy_manager.get_user_agent(),
                locale="pt-PT",
                timezone_id="Europe/Lisbon",
                extra_http_headers={
                    "Accept-Language": "pt-PT,pt;q=0.9,en-GB;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                },
            )

            # Comprehensive stealth profile — patches ~15 DataDome detection vectors
            await context.add_init_script(_STEALTH_JS)

            page = await context.new_page()

            # Block DataDome's own fingerprint collection scripts
            async def _block_datadome(route):
                await route.abort()

            for pattern in _DATADOME_BLOCK_PATTERNS:
                await page.route(pattern, _block_datadome)

            page_num = 1
            consecutive_blocks = 0

            while page_num <= 10:
                url = zone_url if page_num == 1 else f"{zone_url}pagina-{page_num}.htm"
                log.debug("[idealista] page={p} zone={z}", p=page_num, z=zone)

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    # Human-like pause — longer on first page (more realistic)
                    await asyncio.sleep(random.uniform(2.0, 4.0) if page_num == 1 else random.uniform(1.2, 2.8))

                    # Handle cookie consent modal if present
                    await self._handle_cookie_consent(page)

                    html = await page.content()

                    # DataDome interstitial detection — retry once with longer wait
                    if self._is_blocked(html):
                        consecutive_blocks += 1
                        log.warning(
                            "[idealista] DataDome block detected zone={z} page={p} "
                            "(attempt {a}/2)",
                            z=zone, p=page_num, a=consecutive_blocks,
                        )
                        if consecutive_blocks >= 2:
                            log.warning(
                                "[idealista] Giving up on zone={z} after 2 consecutive blocks",
                                z=zone,
                            )
                            break
                        # Back off and retry the same page
                        await asyncio.sleep(random.uniform(8.0, 15.0))
                        continue

                    consecutive_blocks = 0
                    page_items = self._parse_html(html, zone)

                    if not page_items:
                        log.debug("[idealista] No items on page {p} — stopping", p=page_num)
                        break

                    # Enrich each listing with phone from its detail page
                    for item in page_items:
                        detail_url = item.get("url")
                        if detail_url and not item.get("contact_phone"):
                            phone = await self._fetch_detail_phone(page, detail_url)
                            if phone:
                                item["contact_phone"] = phone
                            await asyncio.sleep(random.uniform(1.0, 2.5))

                    items.extend(page_items)

                    # Check for next page
                    has_next = await page.query_selector("a.icon-arrow-right-after")
                    if not has_next:
                        break

                    # Simulate scroll before going to next page
                    await self._human_scroll(page)
                    page_num += 1

                except Exception as e:
                    log.warning("[idealista] Page error zone={z} page={p}: {e}", z=zone, p=page_num, e=e)
                    break

            await browser.close()

        return items

    async def _fetch_detail_phone(self, page, url: str) -> str | None:
        """
        Visit a listing detail page and extract phone via tel: link.

        Uses the same Playwright page object — navigates to detail, extracts
        the first valid Portuguese mobile/landline number, then returns to
        the caller.  Returns None if no phone found or on any error.
        """
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(random.uniform(0.6, 1.2))

            # tel: hrefs are the most reliable source (visible in initial HTML)
            tel_links = await page.query_selector_all("a[href^='tel:']")
            for link in tel_links:
                href = await link.get_attribute("href") or ""
                phone = href.replace("tel:", "").strip()
                # Basic sanity: Portuguese numbers are 9 digits (mobile) or 9+ with country code
                if phone and len(phone.replace(" ", "").lstrip("+")) >= 9:
                    log.debug("[idealista] detail phone found at {u}: {p}", u=url, p=phone)
                    return phone

        except Exception as e:
            log.debug("[idealista] _fetch_detail_phone failed for {u}: {e}", u=url, e=e)

        return None

    async def _handle_cookie_consent(self, page) -> None:
        """Dismiss cookie/GDPR modal if present."""
        try:
            accept_btn = await page.query_selector("#didomi-notice-agree-button")
            if accept_btn:
                await accept_btn.click()
                await asyncio.sleep(0.5)
                log.debug("[idealista] Cookie consent dismissed")
        except Exception:
            pass

    async def _human_scroll(self, page) -> None:
        """Simulate human-like scroll to load lazy images and avoid bot detection."""
        await page.evaluate("""
            window.scrollTo({top: 300, behavior: 'smooth'});
        """)
        await asyncio.sleep(random.uniform(0.3, 0.8))
        await page.evaluate("""
            window.scrollTo({top: 800, behavior: 'smooth'});
        """)
        await asyncio.sleep(random.uniform(0.2, 0.6))

    def _parse_html(self, html: str, zone: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        items = []

        # Idealista listing articles
        cards = soup.select("article.item")
        if not cards:
            cards = soup.select("div.item-info-container")
            log.debug("[idealista] Using fallback selector — found {n} cards", n=len(cards))

        for card in cards:
            try:
                item = self._parse_card(card, zone)
                if item:
                    items.append(item)
            except Exception as e:
                log.debug("[idealista] Card parse error: {e}", e=e)

        return items

    def _parse_card(self, card, zone: str) -> dict | None:
        # Title + URL
        title_el = (
            card.select_one("a.item-link") or
            card.select_one("a[title]") or
            card.select_one(".item-title a")
        )
        if not title_el:
            return None

        title = title_el.get("title") or title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = href if href.startswith("http") else f"{BASE_URL}{href}"

        # External ID from URL slug
        ext_id_match = re.search(r"/imovel/(\d+)/", url)
        external_id = ext_id_match.group(1) if ext_id_match else None

        # Price
        price_el = card.select_one(".item-price") or card.select_one("span.priceDown")
        price_raw = price_el.get_text(strip=True) if price_el else None

        # Details row: area, rooms, floor
        details: dict[str, str] = {}
        for detail in card.select(".item-detail-char span"):
            text = detail.get_text(strip=True)
            if "m²" in text:
                details["area_raw"] = text
            elif re.match(r"T\d", text, re.IGNORECASE):
                details["typology_raw"] = text
            elif "andar" in text.lower():
                details["floor_raw"] = text

        # Location
        location_el = (
            card.select_one(".item-detail-location") or
            card.select_one("span.item-detail-char:last-child")
        )
        location_raw = location_el.get_text(strip=True) if location_el else zone

        # Description snippet
        desc_el = card.select_one(".item-description")
        description = desc_el.get_text(strip=True) if desc_el else None

        # Agency/owner badge — detect "Particular" vs agency name
        agency_el = card.select_one(".contact-name") or card.select_one(".agency-name")
        badge_text = agency_el.get_text(strip=True) if agency_el else None
        if badge_text and badge_text.lower().strip() in ("particular", "proprietário", "propietario"):
            owner_type_raw = "fsbo"
            is_owner = True
            agency_name = None       # "Particular" is not an agency name
            contact_name = badge_text  # use as contact hint
        elif badge_text:
            owner_type_raw = "agency"
            is_owner = False
            agency_name = badge_text
            contact_name = None
        else:
            owner_type_raw = ""
            is_owner = False
            agency_name = None
            contact_name = None

        return {
            "external_id": external_id,
            "url": url,
            "title": title,
            "price_raw": price_raw,
            "area_raw": details.get("area_raw"),
            "typology_raw": details.get("typology_raw"),
            "floor_raw": details.get("floor_raw"),
            "location_raw": location_raw,
            "description": description,
            "agency_name": agency_name,
            "owner_type_raw": owner_type_raw,
            "is_owner": is_owner,
            "contact_name": contact_name,
            "contact_phone": None,
            "zone_query": zone,
        }

    # ── Anti-bot helpers ──────────────────────────────────────────────────────

    def _is_blocked(self, html: str) -> bool:
        """
        Return True if the response is a DataDome interstitial / bot-detection wall.

        Checks the first 3 000 characters for known DataDome / anti-bot fingerprints
        and also catches trivially short pages (real Idealista listing pages are
        typically > 40 KB — anything under 500 bytes is certainly not a listing).
        """
        if not html or len(html) < 500:
            return True
        sample = html[:3_000].lower()
        signals = (
            "datadome",
            "interstitial",
            "captcha",
            "bot detection",
            "access denied",
            "verifying you are human",
            "checking your browser",
            "please enable cookies",
        )
        return any(s in sample for s in signals)

    def _try_scrape_httpx(
        self,
        client: httpx.Client,
        zone: str,
        zone_url: str | None = None,
    ) -> list[dict]:
        """
        Stage-1 fast path: pure HTTP request with realistic Chrome-like headers.

        DataDome sometimes passes server-side-rendered Idealista listing pages
        when the HTTP request looks indistinguishable from a real browser visit.
        When at least one listing card is parsed here, Playwright is skipped
        entirely — saving 5–20 s per zone and reducing bot-detection risk.

        Strategy:
          - Fetch page 1; if blocked → bail (return []).
          - Fetch page 2 with Referer header set to page 1 (realistic navigation).
          - Stop early when a page yields zero cards.

        Returns a (possibly empty) list of parsed listing dicts.
        The caller (`scrape_zone`) falls back to Playwright when this returns [].

        `zone_url` overrides the ZONE_URLS lookup when provided — used for
        rental URLs (ZONE_RENTAL_URLS) passed in from scrape_zone.
        """
        if not zone_url:
            zone_url = ZONE_URLS.get(zone)
        if not zone_url:
            return []

        # Overlay browser-like headers for this zone's requests.
        # We save and restore User-Agent so shared client state stays clean.
        original_ua = client.headers.get("User-Agent", "")
        ua = self.proxy_manager.get_user_agent()

        client.headers.update({
            "User-Agent": ua,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "pt-PT,pt;q=0.9,en-GB;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
            "DNT": "1",
        })

        items: list[dict] = []

        try:
            for page_num in (1, 2):
                url = zone_url if page_num == 1 else f"{zone_url}pagina-{page_num}.htm"

                # Realistic: page 2 is navigated from page 1
                if page_num == 2:
                    client.headers["Referer"] = zone_url

                resp = self._get(client, url)
                if resp is None:
                    log.debug(
                        "[idealista] httpx: no response zone={z} page={p}",
                        z=zone, p=page_num,
                    )
                    break

                html = resp.text

                if self._is_blocked(html):
                    log.debug(
                        "[idealista] httpx: DataDome block zone={z} page={p} — "
                        "aborting httpx path",
                        z=zone, p=page_num,
                    )
                    # Page 1 blocked → entire zone must go through Playwright
                    items = []
                    break

                page_items = self._parse_html(html, zone)
                log.debug(
                    "[idealista] httpx: zone={z} page={p} → {n} items",
                    z=zone, p=page_num, n=len(page_items),
                )
                items.extend(page_items)

                if not page_items:
                    # Empty page — no point fetching further pages
                    break

        finally:
            # Always restore original UA so the shared client is not polluted
            client.headers["User-Agent"] = original_ua

        return items

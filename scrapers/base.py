"""
Abstract base scraper. Every source scraper inherits from this.
Provides: HTTP client setup, retry logic, anti-block integration, raw listing storage.

Also exports PlaywrightPhoneRevealer — reusable batch phone-reveal session.
Opens one browser per batch instead of one per URL (3-5x faster for multi-item zones).
"""
from __future__ import annotations

import asyncio
import random
import re
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import settings
from scrapers.anti_block.proxy_manager import ProxyManager
from scrapers.anti_block.rate_limiter import RateLimiter
from utils.logger import get_logger
from utils.phone import (
    best_phone,
    extract_phone_from_tel_href,
    extract_phone_from_text,
    extract_whatsapp,
    validate_pt_phone,
)

log = get_logger(__name__)


# ── Playwright batch phone revealer ───────────────────────────────────────────

class PlaywrightPhoneRevealer:
    """
    Reusable Playwright session for revealing phone numbers across a batch of URLs.

    Instead of launching a new browser for every listing (3-5s startup overhead each),
    this class opens ONE browser per batch, reuses the same context for all pages,
    and closes the browser when done.

    Usage (per zone):
        revealer = PlaywrightPhoneRevealer(
            phone_btn_selectors=_PHONE_BTN_SELECTORS,
            consent_selectors=_CONSENT_SELECTORS,
            headless=settings.headless_browser,
        )
        phones = revealer.reveal_batch(urls)   # {url: "+351XXXXXXXXX"}

    Thread safety: not thread-safe. One instance per zone-run is recommended.
    """

    def __init__(
        self,
        phone_btn_selectors: list[str],
        consent_selectors: list[str],
        headless: bool = True,
        inter_fetch_delay_min: float = 0.8,
        inter_fetch_delay_max: float = 1.8,
    ):
        self._phone_btn_selectors   = phone_btn_selectors
        self._consent_selectors     = consent_selectors
        self._headless              = headless
        self._delay_min             = inter_fetch_delay_min
        self._delay_max             = inter_fetch_delay_max

    def reveal_batch(self, urls: list[str]) -> dict[str, str]:
        """
        Reveal phones for a list of listing URLs in one browser session.

        Returns a dict mapping each successfully-revealed URL to its
        canonical phone string ("+351XXXXXXXXX").
        URLs where reveal failed are simply absent from the returned dict.
        """
        if not urls:
            return {}
        try:
            return asyncio.run(self._reveal_batch_async(urls))
        except RuntimeError:
            # asyncio.run() cannot be called when a loop is already running
            return self._reveal_batch_sequential_fallback(urls)
        except Exception as e:
            log.debug("[PlaywrightPhoneRevealer] batch error: {e}", e=e)
            return {}

    def _reveal_batch_sequential_fallback(self, urls: list[str]) -> dict[str, str]:
        """Fallback for environments with an existing event loop (e.g. Jupyter)."""
        results = {}
        for url in urls:
            try:
                loop = asyncio.get_event_loop()
                phone = loop.run_until_complete(self._reveal_single_session(url))
                if phone:
                    results[url] = phone
            except Exception as e:
                log.debug("[PlaywrightPhoneRevealer] fallback error {u}: {e}", u=url[-60:], e=e)
        return results

    async def _reveal_batch_async(self, urls: list[str]) -> dict[str, str]:
        """Core async implementation — one browser, one context, N pages."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            log.debug("[PlaywrightPhoneRevealer] Playwright not installed — skipping")
            return {}

        from config.zone_config import get_random_user_agent

        results: dict[str, str] = {}

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self._headless,
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

            for i, url in enumerate(urls):
                page = await context.new_page()
                # Block heavy non-essential resources — cuts load time by ~75%
                await page.route(
                    "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,ico,css}",
                    lambda route: route.abort(),
                )
                await page.route(
                    "**googletagmanager**|**doubleclick**|**googlesyndication**|**facebook**",
                    lambda route: route.abort(),
                )
                try:
                    phone = await self._reveal_one(page, url)
                    if phone:
                        results[url] = phone
                        log.debug(
                            "[PlaywrightPhoneRevealer] {i}/{n} OK: {p} ← {u}",
                            i=i + 1, n=len(urls), p=phone, u=url[-60:],
                        )
                    else:
                        log.debug(
                            "[PlaywrightPhoneRevealer] {i}/{n} no phone: {u}",
                            i=i + 1, n=len(urls), u=url[-60:],
                        )
                except Exception as e:
                    log.debug(
                        "[PlaywrightPhoneRevealer] error {i}/{n} {u}: {e}",
                        i=i + 1, n=len(urls), u=url[-60:], e=e,
                    )
                finally:
                    await page.close()

                # Inter-fetch delay (skip after last URL)
                if i < len(urls) - 1:
                    await asyncio.sleep(random.uniform(self._delay_min, self._delay_max))

            await browser.close()

        return results

    async def _reveal_single_session(self, url: str) -> str | None:
        """Open a fresh browser for a single URL (fallback path)."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return None

        from config.zone_config import get_random_user_agent

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self._headless,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=get_random_user_agent(),
                locale="pt-PT",
            )
            page = await context.new_page()
            try:
                return await self._reveal_one(page, url)
            finally:
                await browser.close()

    async def _reveal_one(self, page, url: str) -> str | None:
        """
        Navigate to a listing page and extract the best available phone number.

        Strategy (best source wins, real numbers prioritised over relay):
          1. Load page + dismiss consent banners
          2. PRE-CLICK aggressive discovery via ``utils.phone_discovery``:
             WhatsApp deep links, microdata, data-* attrs, hidden inputs,
             JSON-LD, JSON key/value pairs in inline scripts, ``<meta>`` tags,
             description text. Skips the reveal click entirely when a
             non-relay number is already discoverable.
          3. Otherwise click the reveal button
          4. POST-CLICK discovery — same surfaces re-checked. Real numbers
             often surface in newly-injected ``data-phone`` attrs.
          5. Returns ``best_phone`` of all candidates (mobile > landline >
             relay). Relay (6XX) is only returned when nothing better exists.

        Returns a canonical "+351XXXXXXXXX" or None.
        """
        from utils.phone_discovery import discover_phones

        await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(random.uniform(0.8, 1.5))

        # Dismiss consent modal (blocks button clicks when visible)
        for sel in self._consent_selectors:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible():
                    await btn.click(timeout=2_000)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                pass
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)

        # ── Phase A: aggressive pre-click discovery ───────────────────────
        # Sweeps every static surface we know of — WhatsApp deep links,
        # microdata, data-* attrs, hidden inputs, JSON-LD, inline ``<script>``
        # stores, ``<meta>`` tags, description — BEFORE clicking the reveal
        # button. If a real (non-relay) mobile/landline is anywhere in the
        # page payload, we skip the click entirely and avoid the masking
        # 6XX number altogether.
        try:
            html_pre = await page.content()
        except Exception:
            html_pre = ""

        non_relay_pre = discover_phones(html_pre, allow_relay=False)
        if non_relay_pre:
            best = best_phone(non_relay_pre)
            if best and best.valid and best.phone_type in ("mobile", "landline"):
                return best.canonical

        # Keep the legacy DOM-walk results too — covers fast paths the
        # full-page HTML scan might miss (cross-frame iframes, async
        # content that already settled into the live DOM).
        legacy_pre = await self._scan_page_phones(page)

        # ── Phase B: click the reveal button ───────────────────────────────
        phone_btn = None
        for sel in self._phone_btn_selectors:
            phone_btn = await page.query_selector(sel)
            if phone_btn:
                break

        # Text-match fallback
        if not phone_btn:
            for handle in await page.query_selector_all("button, a"):
                try:
                    text = (await handle.inner_text()).strip().lower()
                    if "mostrar" in text or "n\u00famero" in text or "telefone" in text:
                        phone_btn = handle
                        break
                except Exception:
                    continue

        if phone_btn:
            # force=True bypasses visibility/overlay checks — handles hidden/obscured buttons.
            try:
                await phone_btn.click(force=True)
                try:
                    await page.wait_for_selector(
                        "a[href^='tel:']", timeout=4_000, state="attached"
                    )
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(0.3, 0.7))
            except Exception as e:
                log.debug("[PhoneRevealer] button click failed: {e}", e=e)

        # ── Phase C: post-click discovery ─────────────────────────────────
        try:
            html_post = await page.content()
        except Exception:
            html_post = html_pre

        # Full discovery sweep on the post-click HTML. Prefer non-relay
        # candidates — when nothing better is found, fall back to the relay
        # number so the lead at least carries some contact channel.
        non_relay_post = discover_phones(html_post, allow_relay=False)
        if non_relay_post:
            best = best_phone(non_relay_post)
            if best and best.valid:
                return best.canonical

        legacy_post = await self._scan_page_phones(page, include_body_text=True)
        all_candidates = (
            non_relay_pre + legacy_pre +
            discover_phones(html_post, allow_relay=True) +
            legacy_post
        )
        best = best_phone(all_candidates)
        return best.canonical if best and best.valid else None

    async def _scan_page_phones(self, page, include_body_text: bool = False) -> list[str]:
        """
        Collect every plausible phone-number source from the page DOM.

        Order matters — `best_phone()` uses source order to break ties, so
        trusted sources are listed first:

          1. WhatsApp links (wa.me, api.whatsapp.com) — always real mobile
          2. tel: hrefs                                — direct seller contact
          3. Visible description / body text           — sellers sometimes
                                                         embed their direct
                                                         number when the
                                                         portal allows it

        `include_body_text=True` adds a full-body text sweep at the end;
        only enable post-click to avoid noisy regex false positives.
        """
        candidates: list[str] = []

        # 1. WhatsApp links
        try:
            for a in await page.query_selector_all(
                "a[href*='wa.me'], a[href*='whatsapp.com']"
            ):
                href = (await a.get_attribute("href")) or ""
                wa = extract_whatsapp(href)
                if wa:
                    candidates.append(wa)
        except Exception:
            pass

        # 2. tel: hrefs
        try:
            for a in await page.query_selector_all("a[href^='tel:']"):
                href = (await a.get_attribute("href")) or ""
                phone = extract_phone_from_tel_href(href)
                if phone:
                    candidates.append(phone)
        except Exception:
            pass

        # 3. Description-scope text scan (safer than full-body)
        for sel in (
            "[data-testid='ad_description']",
            "[data-cy='advert-description']",
            "[class*='description']",
            "section[class*='Description']",
        ):
            try:
                el = await page.query_selector(sel)
                if not el:
                    continue
                text = await el.inner_text()
                wa = extract_whatsapp(text)
                if wa:
                    candidates.append(wa)
                phone = extract_phone_from_text(text)
                if phone:
                    candidates.append(phone)
            except Exception:
                continue

        # 4. Full-body text (optional, noisier)
        if include_body_text:
            try:
                body = await page.inner_text("body")
                wa = extract_whatsapp(body)
                if wa:
                    candidates.append(wa)
                phone = extract_phone_from_text(body)
                if phone:
                    candidates.append(phone)
            except Exception:
                pass

        return candidates


class ScraperResult:
    """Container returned by each scraper run."""
    def __init__(self, source: str, batch_id: str):
        self.source = source
        self.batch_id = batch_id
        self.started_at = datetime.utcnow()
        self.items: list[dict] = []
        self.errors: list[str] = []
        self.finished_at: Optional[datetime] = None

    def add(self, item: dict) -> None:
        self.items.append(item)

    def fail(self, msg: str) -> None:
        self.errors.append(msg)
        log.error("[{src}] {msg}", src=self.source, msg=msg)

    def finish(self) -> None:
        self.finished_at = datetime.utcnow()
        elapsed = (self.finished_at - self.started_at).total_seconds()
        log.info(
            "[{src}] Done — {n} items, {e} errors, {t:.1f}s",
            src=self.source, n=len(self.items), e=len(self.errors), t=elapsed,
        )

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class BaseScraper(ABC):
    """
    Base class for all source scrapers.
    Subclasses implement `scrape_zone()` which yields raw listing dicts.
    """

    SOURCE: str = "unknown"

    # Ratio at which a pagination page is considered "all-seen" — if this
    # many listings on the current page are already in `known_external_ids`,
    # deeper pages almost certainly add no new rows and pagination stops.
    # Tuned to 0.85 so page-level noise doesn't prematurely cut long-tail pages.
    DELTA_STOP_RATIO: float = 0.85

    def __init__(self):
        self.proxy_manager = ProxyManager()
        self.rate_limiter = RateLimiter(
            min_delay=settings.scrape_delay_min,
            max_delay=settings.scrape_delay_max,
            source=self.SOURCE,
        )
        self.batch_id = str(uuid.uuid4())
        # Populated by the pipeline runner before run() — set of external_ids
        # this source has already captured in previous batches. Scrapers that
        # support delta-crawl check overlap against this set to stop paginating
        # once a page is mostly-seen.
        self.known_external_ids: set[str] = set()

    # ── Delta crawling helpers ────────────────────────────────────────────────

    def _page_is_mostly_seen(self, page_items: list[dict]) -> bool:
        """
        Return True when ≥DELTA_STOP_RATIO of this page's external_ids already
        exist in `self.known_external_ids`. Scrapers call this after parsing a
        page to decide whether to break pagination early.

        Returns False when:
          - known_external_ids is empty (first-ever run for this source)
          - page_items is empty
          - overlap ratio is below the threshold
        """
        if not self.known_external_ids or not page_items:
            return False
        with_id = [it for it in page_items if it.get("external_id")]
        if not with_id:
            return False
        overlap = sum(1 for it in with_id if it["external_id"] in self.known_external_ids)
        ratio = overlap / len(with_id)
        if ratio >= self.DELTA_STOP_RATIO:
            log.info(
                "[{src}] delta-stop — {o}/{n} ({r:.0%}) already seen",
                src=self.SOURCE, o=overlap, n=len(with_id), r=ratio,
            )
            return True
        return False

    # ── HTTP client ───────────────────────────────────────────────────────────

    def _build_client(self, follow_redirects: bool = True) -> httpx.Client:
        """Build a fresh httpx client with anti-block headers and optional proxy."""
        kwargs = self.proxy_manager.get_httpx_kwargs()
        return httpx.Client(
            timeout=settings.request_timeout,
            follow_redirects=follow_redirects,
            **kwargs,
        )

    def _get(self, client: httpx.Client, url: str, params: dict = None) -> Optional[httpx.Response]:
        """
        Perform a GET with rate limiting and retry on failure.
        Returns None if all retries exhausted.
        """
        self.rate_limiter.wait()
        for attempt in range(settings.max_retries):
            try:
                resp = client.get(url, params=params)
                if resp.status_code == 200:
                    self.rate_limiter.record_success()
                    return resp
                if resp.status_code == 429:
                    log.warning("[{src}] 429 Too Many Requests — backoff", src=self.SOURCE)
                    self.rate_limiter.backoff(attempt)
                    continue
                if resp.status_code in (403, 401):
                    log.warning("[{src}] {code} on {url}", src=self.SOURCE, code=resp.status_code, url=url)
                    self.rate_limiter.backoff(attempt)
                    # Rotate user-agent for next attempt
                    client.headers["User-Agent"] = self.proxy_manager.get_user_agent()
                    continue
                log.warning("[{src}] HTTP {code} for {url}", src=self.SOURCE, code=resp.status_code, url=url)
                return None
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                log.warning("[{src}] Request error attempt {a}: {e}", src=self.SOURCE, a=attempt + 1, e=e)
                self.rate_limiter.backoff(attempt)
        return None

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self, zones: list[str] = None) -> ScraperResult:
        """
        Execute the scraper for all target zones.
        Returns a ScraperResult with all raw listing dicts collected.
        """
        zones = zones or settings.zones
        result = ScraperResult(source=self.SOURCE, batch_id=self.batch_id)
        log.info("[{src}] Starting scrape — zones: {zones}", src=self.SOURCE, zones=zones)

        with self._build_client() as client:
            for zone in zones:
                try:
                    count_before = len(result.items)
                    for item in self.scrape_zone(client, zone):
                        item["_source"] = self.SOURCE
                        item["_zone_query"] = zone
                        item["_batch_id"] = self.batch_id
                        result.add(item)
                    found = len(result.items) - count_before
                    log.info("[{src}] Zone '{z}' → {n} listings", src=self.SOURCE, z=zone, n=found)
                except Exception as exc:
                    result.fail(f"Zone '{zone}' failed: {exc}")

        result.finish()
        return result

    @abstractmethod
    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """
        Yield raw listing dicts for the given zone.
        Each dict must contain at minimum: 'url', 'title', 'price_raw'.
        """
        ...

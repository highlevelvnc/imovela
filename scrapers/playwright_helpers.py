"""
Shared Playwright primitives for stealth-required scrapers.

Five scrapers in this codebase use Playwright with a stealth profile —
Idealista, the four bank portals, the Leilões scraper, and Facebook
Marketplace. Until now each duplicated the same browser-launch boilerplate
and stealth init script. That copy/paste was already drifting subtly
between sites (e.g. one had ``hardwareConcurrency`` patches, others
didn't), making bug-for-bug parity impossible.

This module centralises:

  * ``stealth_browser_context()`` — async context manager yielding a
    fully-patched ``BrowserContext``. Caller just opens pages.
  * ``block_heavy_resources(page)`` — abort image/font/CSS/tracking
    requests so listing pages render in ~3-4s instead of 15-30s.
  * ``dismiss_consent_modals(page)`` — sweep the most common cookie
    banner selectors (OneTrust, Didomi, Custom-built).

Designed as drop-in: ``async with stealth_browser_context(headless=...)
as ctx:`` replaces every existing ``async_playwright()`` boilerplate.

Also re-uses ``cookie_jar`` to optionally persist storage_state across
runs — see the ``persist_state_for`` parameter.
"""
from __future__ import annotations

import asyncio
import contextlib
import random
from pathlib import Path
from typing import AsyncIterator, Optional

from utils.logger import get_logger

log = get_logger(__name__)


# Comprehensive stealth init — superset of every per-site patch we had,
# all known DataDome / Incapsula / Cloudflare fingerprint vectors covered.
_STEALTH_INIT_JS = r"""
// 1. webdriver flag
Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});

// 2. realistic language list
Object.defineProperty(navigator, 'languages', {
    get: () => ['pt-PT', 'pt', 'en-GB', 'en'],
    configurable: true,
});

// 3. platform
Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel', configurable: true});

// 4. hardware
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8, configurable: true});
Object.defineProperty(navigator, 'deviceMemory',        {get: () => 8, configurable: true});

// 5. window.chrome — present on real Chrome, absent on headless
window.chrome = {
    runtime: {},
    app:     {InstallState: {}, RunningState: {}},
    csi:     () => null,
    loadTimes: () => null,
};

// 6. permissions.query — return real-looking notification permission
const _origQuery = window.navigator.permissions ? window.navigator.permissions.query : null;
if (_origQuery) {
    window.navigator.permissions.query = (params) => (
        params && params.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : _origQuery(params)
    );
}

// 7. plugins length — empty array is the strongest signal of headless
//    Spoofing length=3 with realistic mime types matches default Chrome.
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const fakePlugin = (name, desc, filename) => {
            const p = Object.create(Plugin.prototype);
            Object.defineProperties(p, {
                name:        {get: () => name},
                description: {get: () => desc},
                filename:    {get: () => filename},
                length:      {get: () => 1},
            });
            return p;
        };
        return [
            fakePlugin('PDF Viewer',    'Portable Document Format', 'internal-pdf-viewer'),
            fakePlugin('Chrome PDF Viewer', '', 'mhjfbmdgcfjbbpaeojofohoefgiehjai'),
            fakePlugin('WebKit built-in PDF', '', 'internal-pdf-viewer'),
        ];
    },
    configurable: true,
});

// 8. iframe.contentWindow — DataDome checks if iframes leak headless flag
const origDescriptor = Object.getOwnPropertyDescriptor(
    HTMLIFrameElement.prototype, 'contentWindow'
);
if (origDescriptor && origDescriptor.get) {
    Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
        get: function () {
            const w = origDescriptor.get.call(this);
            try { delete w.navigator.webdriver; } catch (_) {}
            return w;
        },
    });
}

// 9. Function.prototype.toString — must return [native code] for spoofed fns
const origToString = Function.prototype.toString;
Function.prototype.toString = function () {
    if (this === window.navigator.permissions.query) {
        return 'function query() { [native code] }';
    }
    return origToString.apply(this, arguments);
};
"""


# Resource patterns aborted by ``block_heavy_resources`` — cuts ~75% of
# bytes loaded with zero impact on listing data extraction.
_HEAVY_PATTERNS: tuple[str, ...] = (
    "**/*.{png,jpg,jpeg,gif,webp,svg,ico,bmp}",
    "**/*.{woff,woff2,ttf,otf,eot}",
    "**/*.{css,less,scss}",
    "**googletagmanager**",
    "**doubleclick**",
    "**googlesyndication**",
    "**facebook.net**",
    "**connect.facebook.net**",
    "**hotjar.com**",
    "**segment.io**",
    "**amplitude.com**",
    "**newrelic.com**",
    "**sentry.io**",
)


# Common consent-banner selectors — sweep order doesn't matter, the first
# one visible gets clicked. Add new banner variants here as we encounter them.
_CONSENT_SELECTORS: tuple[str, ...] = (
    "#onetrust-accept-btn-handler",
    "button#onetrust-accept-btn-handler",
    "button.onetrust-close-btn-handler",
    "#accept-recommended-btn-handler",
    "button[title='Aceitar Todos os Cookies']",
    "button[title='Accept All Cookies']",
    ".ot-pc-refuse-all-handler",
    "button[id*='accept'][id*='cookie']",
    "button[class*='accept'][class*='cookie']",
    "button[id*='didomi-notice-agree']",
    ".didomi-continue-without-agreeing",
    "#truste-consent-button",
)


# ── Public helpers ───────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def stealth_browser_context(
    *,
    headless:           bool          = True,
    user_agent:         Optional[str] = None,
    viewport:           Optional[dict] = None,
    locale:             str           = "pt-PT",
    timezone_id:        str           = "Europe/Lisbon",
    persist_state_for:  Optional[str] = None,
) -> AsyncIterator:
    """
    Async context manager — yields a fully-stealth-patched BrowserContext.

    ``persist_state_for`` (if set) loads a saved storage_state for that
    source slug at entry, and saves it back at exit. Cookies survive
    across runs, dramatically improving anti-bot success rates on second
    and subsequent crawls.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("Playwright not installed — install with `pip install playwright && playwright install chromium`")
        yield None
        return

    from config.zone_config import get_random_user_agent

    ua = user_agent or get_random_user_agent()
    vp = viewport or {"width": 1366, "height": 768}

    # Optional: load saved storage_state from cookie jar
    storage_state_path = None
    if persist_state_for:
        try:
            from scrapers.anti_block.cookie_jar import load_into_playwright_state
            storage_state_path = load_into_playwright_state(persist_state_for)
            if storage_state_path:
                log.debug("[playwright] loaded saved state for {s}", s=persist_state_for)
        except Exception as e:
            log.debug("[playwright] could not load state: {e}", e=e)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        try:
            context_kwargs = {
                "viewport":     vp,
                "user_agent":   ua,
                "locale":       locale,
                "timezone_id":  timezone_id,
                "extra_http_headers": {"Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8"},
            }
            if storage_state_path:
                context_kwargs["storage_state"] = storage_state_path

            context = await browser.new_context(**context_kwargs)
            await context.add_init_script(_STEALTH_INIT_JS)

            try:
                yield context
            finally:
                # Persist state on exit so the next run inherits cookies
                if persist_state_for:
                    try:
                        from scrapers.anti_block.cookie_jar import save_from_playwright_context
                        await save_from_playwright_context(context, persist_state_for)
                    except Exception as e:
                        log.debug("[playwright] state save failed: {e}", e=e)
                await context.close()
        finally:
            await browser.close()


async def block_heavy_resources(page) -> None:
    """Register network rules that abort image/font/CSS/tracking requests."""
    for pattern in _HEAVY_PATTERNS:
        try:
            await page.route(pattern, lambda route: route.abort())
        except Exception:
            pass


async def dismiss_consent_modals(page, timeout_ms: int = 1500) -> bool:
    """
    Click the first visible consent-banner button. Returns True if any
    button was clicked, False otherwise. Idempotent — safe to call
    repeatedly during a session.
    """
    for sel in _CONSENT_SELECTORS:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click(timeout=timeout_ms)
                await asyncio.sleep(0.3)
                return True
        except Exception:
            continue

    # Final fallback: ESC key dismisses many lightweight modals
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.15)
    except Exception:
        pass
    return False


async def human_pause(min_s: float = 0.6, max_s: float = 1.6) -> None:
    """Random delay that mimics a human reading the page."""
    await asyncio.sleep(random.uniform(min_s, max_s))

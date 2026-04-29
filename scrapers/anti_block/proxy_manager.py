"""
Proxy rotation manager.
Phase 1: Works with optional proxy list from .env.
Phase 2: Plug in Brightdata / Oxylabs residential proxy pool.
"""
from __future__ import annotations

import itertools
import random
from typing import Optional

from config.settings import settings
from utils.logger import get_logger

log = get_logger(__name__)


# ─── User-Agent pool ─────────────────────────────────────────────────────────
# 40 real user-agents covering Chrome/Firefox on Windows/Mac/Linux
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Consistent Accept headers matching the user-agent type
CHROME_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en-GB;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


class ProxyManager:
    """
    Manages a rotating pool of HTTP proxies and user-agents.

    Phase 1: Uses proxies from .env PROXY_LIST (can be empty — falls back to direct).
    Phase 2: Replace with residential proxy API (Brightdata, Oxylabs, etc.)
    """

    def __init__(self):
        self._proxies = settings.proxies
        self._proxy_cycle = itertools.cycle(self._proxies) if self._proxies else None
        self._ua_pool = USER_AGENTS.copy()
        random.shuffle(self._ua_pool)
        self._ua_cycle = itertools.cycle(self._ua_pool)
        self._blocked_proxies: set[str] = set()

        if self._proxies:
            log.info("ProxyManager initialised with {n} proxies", n=len(self._proxies))
        else:
            log.info("ProxyManager running without proxies (direct connection)")

    def get_proxy(self) -> Optional[str]:
        """Return the next available proxy URL, or None for direct connection."""
        if not settings.use_proxies or not self._proxy_cycle:
            return None
        # Skip blocked proxies
        for _ in range(len(self._proxies)):
            proxy = next(self._proxy_cycle)
            if proxy not in self._blocked_proxies:
                return proxy
        log.warning("All proxies are blocked — falling back to direct connection")
        return None

    def get_user_agent(self) -> str:
        """Return the next user-agent from the pool."""
        return next(self._ua_cycle)

    def get_headers(self, extra: dict = None) -> dict:
        """Return a complete headers dict with a fresh user-agent."""
        headers = {**CHROME_HEADERS, "User-Agent": self.get_user_agent()}
        if extra:
            headers.update(extra)
        return headers

    def mark_blocked(self, proxy: str) -> None:
        """Mark a proxy as blocked so it's skipped temporarily."""
        if proxy:
            self._blocked_proxies.add(proxy)
            log.warning("Proxy marked as blocked: {proxy}", proxy=proxy)

    def get_httpx_kwargs(self) -> dict:
        """Return kwargs dict ready to unpack into httpx.Client(...)."""
        proxy = self.get_proxy()
        kwargs: dict = {"headers": self.get_headers()}
        if proxy:
            kwargs["proxy"] = proxy
        return kwargs

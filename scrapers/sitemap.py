"""
Sitemap crawler — discovery via robots.txt → sitemap.xml chain.

Real-estate portals publish gzipped XML sitemaps that enumerate every live
listing URL. Scraping these is dramatically cheaper than paginating the
result pages because:

  * One HTTP request per ~20-50k URLs (vs one per ~25 listings on grid)
  * No JS rendering, no card-parser fragility
  * Plays nicely with anti-block — sitemaps are explicitly meant to be
    fetched by bots and respond fast and uniformly
  * Uncovers listings that fall off the end of paginated results
    (everything past the ~1000-item cap on portal queries)

Usage
-----
This module is consumed by the existing Imovirtual / Sapo scrapers via
``self.discover_urls_from_sitemap()`` — each scraper appends the URL set
to the ones discovered through standard pagination, and the natural
external_id dedup at the storage layer absorbs any overlap.

The crawler is conservative about scope:
  * Only URLs matching a per-source path filter are kept (e.g. anuncios/)
  * Optional zone filter: keeps URLs whose path contains a known zone slug
  * Hard cap of MAX_URLS_PER_SOURCE per call to bound memory use

Public API
----------
SitemapCrawler(source_name)
    .discover(robots_url=..., path_filters=..., zone_slugs=...) -> list[str]

The returned list is shuffled before paginated fetching so that any
delta-crawl stop logic on the consumer side encounters new URLs first.
"""
from __future__ import annotations

import gzip
import io
import random
import re
from typing import Iterable, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import httpx

from utils.logger import get_logger

log = get_logger(__name__)


# ── Constants ────────────────────────────────────────────────────────────────

MAX_URLS_PER_SOURCE  = 30_000   # safety cap per discover() call
SITEMAP_TIMEOUT      = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Sitemap XML namespace — every reasonably-implemented sitemap uses this.
_SM_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


# ── Crawler ──────────────────────────────────────────────────────────────────

class SitemapCrawler:
    """
    Discover listing URLs published in a portal's sitemap chain.

    Resolution order:
      1. Fetch robots.txt; collect every ``Sitemap:`` directive
      2. For each sitemap, follow recursively (sitemap index → child sitemaps)
      3. Collect every <loc> entry that passes the per-source filter
      4. Optionally filter by zone slug
    """

    def __init__(self, source_name: str = "sitemap"):
        self.source = source_name
        self._seen: set[str] = set()
        self._client: Optional[httpx.Client] = None

    def discover(
        self,
        robots_url: str,
        path_filters: Iterable[str],
        zone_slugs: Optional[Iterable[str]] = None,
        max_urls: int = MAX_URLS_PER_SOURCE,
    ) -> list[str]:
        """
        Return discovered listing URLs filtered by path + optional zones.

        ``path_filters`` is an iterable of path substrings — a URL is kept
        when ANY filter substring appears in its path component. Pass
        e.g. ``("/pt/anuncio/",)`` for Imovirtual or ``("/venda/",)`` for Sapo.

        ``zone_slugs`` further narrows the set: only URLs whose path
        contains at least one of the slugs are returned. ``None`` (default)
        keeps everything.

        Hard cap of ``max_urls`` to bound memory.
        """
        self._seen.clear()
        path_filters = tuple(path_filters)
        zone_slugs = tuple(s.lower() for s in zone_slugs) if zone_slugs else None

        with httpx.Client(
            timeout=SITEMAP_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "text/xml,application/xml,*/*"},
        ) as client:
            self._client = client

            # 1. robots.txt → list of sitemap URLs
            sitemap_urls = self._read_sitemaps_from_robots(robots_url)
            if not sitemap_urls:
                log.warning(
                    "[sitemap:{src}] robots.txt yielded no Sitemap directive — falling back to /sitemap.xml",
                    src=self.source,
                )
                sitemap_urls = [urljoin(robots_url, "/sitemap.xml")]

            results: list[str] = []
            for sm_url in sitemap_urls:
                if len(results) >= max_urls:
                    break
                self._crawl(sm_url, path_filters, zone_slugs, results, max_urls)

        # Shuffle so delta-crawl stop logic encounters new URLs first
        random.shuffle(results)
        log.info(
            "[sitemap:{src}] discovered {n} URLs (filters={f}, zones={z})",
            src=self.source, n=len(results),
            f=list(path_filters), z=list(zone_slugs) if zone_slugs else "*",
        )
        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _read_sitemaps_from_robots(self, robots_url: str) -> list[str]:
        try:
            r = self._client.get(robots_url)
            if r.status_code != 200:
                return []
        except (httpx.HTTPError, httpx.NetworkError) as e:
            log.debug("[sitemap:{src}] robots.txt error: {e}", src=self.source, e=e)
            return []

        out: list[str] = []
        for line in r.text.splitlines():
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                url = line.split(":", 1)[1].strip()
                if url and url not in out:
                    out.append(url)
        return out

    def _crawl(
        self,
        sitemap_url: str,
        path_filters: tuple[str, ...],
        zone_slugs: Optional[tuple[str, ...]],
        out: list[str],
        max_urls: int,
        depth: int = 0,
    ) -> None:
        if depth > 4:                  # safety: pathological recursion
            return
        if sitemap_url in self._seen:
            return
        self._seen.add(sitemap_url)

        body = self._fetch_xml(sitemap_url)
        if not body:
            return

        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            log.debug("[sitemap:{src}] parse error {u}: {e}", src=self.source, u=sitemap_url, e=e)
            return

        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            # Index file — recurse into each child <sitemap><loc>
            for sm in root.findall(f"{_SM_NS}sitemap"):
                loc = sm.find(f"{_SM_NS}loc")
                if loc is None or not loc.text:
                    continue
                if len(out) >= max_urls:
                    return
                self._crawl(loc.text.strip(), path_filters, zone_slugs, out, max_urls, depth + 1)

        elif tag.endswith("urlset"):
            # Leaf file — collect every <url><loc>
            kept = 0
            for url_el in root.findall(f"{_SM_NS}url"):
                if len(out) >= max_urls:
                    break
                loc = url_el.find(f"{_SM_NS}loc")
                if loc is None or not loc.text:
                    continue
                url = loc.text.strip()
                if not any(f in url for f in path_filters):
                    continue
                if zone_slugs and not any(z in url.lower() for z in zone_slugs):
                    continue
                out.append(url)
                kept += 1
            log.debug(
                "[sitemap:{src}] {u} → {n} URLs kept",
                src=self.source, u=sitemap_url[-60:], n=kept,
            )

    def _fetch_xml(self, url: str) -> Optional[bytes]:
        """Download a sitemap, transparently un-gzipping when appropriate."""
        try:
            r = self._client.get(url)
            if r.status_code != 200:
                log.debug(
                    "[sitemap:{src}] HTTP {c} for {u}",
                    src=self.source, c=r.status_code, u=url,
                )
                return None
        except (httpx.HTTPError, httpx.NetworkError) as e:
            log.debug("[sitemap:{src}] fetch error {u}: {e}", src=self.source, u=url, e=e)
            return None

        body = r.content
        if url.endswith(".gz") or body[:2] == b"\x1f\x8b":
            try:
                body = gzip.GzipFile(fileobj=io.BytesIO(body)).read()
            except OSError as e:
                log.debug("[sitemap:{src}] gunzip failed {u}: {e}", src=self.source, u=url, e=e)
                return None
        return body

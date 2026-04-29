"""
Tests for scrapers (unit tests using mocked HTTP — no real network calls).
Run with: pytest tests/test_scrapers.py -v
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import MagicMock, patch
from scrapers.anti_block.rate_limiter import RateLimiter
from scrapers.anti_block.proxy_manager import ProxyManager
from scrapers.olx import OLXScraper
from scrapers.imovirtual import ImovirtualScraper


class TestRateLimiter:

    def test_initialises(self):
        rl = RateLimiter(min_delay=0.01, max_delay=0.02, source="test")
        assert rl.min_delay == 0.01

    def test_wait_enforces_delay(self):
        import time
        rl = RateLimiter(min_delay=0.05, max_delay=0.05, source="test")
        start = time.monotonic()
        rl.wait()
        rl.wait()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.04  # at least one delay cycle

    def test_backoff_increases(self):
        """Backoff should not raise and should sleep proportionally."""
        rl = RateLimiter(min_delay=0.01, max_delay=0.01, source="test")
        # Just verify it doesn't throw
        with patch("time.sleep") as mock_sleep:
            rl.backoff(0)
            rl.backoff(1)
            calls = [c.args[0] for c in mock_sleep.call_args_list]
            assert calls[1] >= calls[0]  # second backoff >= first


class TestProxyManager:

    def test_no_proxies_returns_none(self):
        with patch("config.settings.settings") as mock_settings:
            mock_settings.use_proxies = False
            mock_settings.proxies = []
            pm = ProxyManager()
            assert pm.get_proxy() is None

    def test_user_agent_returned(self):
        pm = ProxyManager()
        ua = pm.get_user_agent()
        assert "Mozilla" in ua

    def test_headers_include_user_agent(self):
        pm = ProxyManager()
        headers = pm.get_headers()
        assert "User-Agent" in headers
        assert "Mozilla" in headers["User-Agent"]

    def test_different_user_agents_rotated(self):
        pm = ProxyManager()
        uas = {pm.get_user_agent() for _ in range(20)}
        assert len(uas) > 1  # Should have cycled through multiple


class TestOLXScraper:

    SAMPLE_HTML = """
    <html><body>
    <div data-cy="l-card">
        <h6 data-cy="ad-title">T2 Lisboa Alvalade 78m²</h6>
        <a data-cy="listing-ad-title" href="/d/t2-lisboa-IDabc123.html">T2 Lisboa</a>
        <p data-testid="ad-price">195.000 €</p>
        <p data-testid="location-date">Lisboa, Alvalade - hoje</p>
    </div>
    <div data-cy="l-card">
        <h6 data-cy="ad-title">T3 Cascais com garagem</h6>
        <a data-cy="listing-ad-title" href="/d/t3-cascais-IDdef456.html">T3 Cascais</a>
        <p data-testid="ad-price">380.000 €</p>
        <p data-testid="location-date">Cascais - ontem</p>
    </div>
    </body></html>
    """

    def test_parse_listing_page_finds_cards(self):
        from bs4 import BeautifulSoup
        scraper = OLXScraper()
        soup = BeautifulSoup(self.SAMPLE_HTML, "lxml")
        items = scraper._parse_listing_page(soup, "Lisboa")
        assert len(items) == 2

    def test_parsed_item_has_required_fields(self):
        from bs4 import BeautifulSoup
        scraper = OLXScraper()
        soup = BeautifulSoup(self.SAMPLE_HTML, "lxml")
        items = scraper._parse_listing_page(soup, "Lisboa")
        item = items[0]
        assert "url" in item
        assert "title" in item
        assert "price_raw" in item
        assert item["title"] == "T2 Lisboa Alvalade 78m²"
        assert item["price_raw"] == "195.000 €"

    def test_external_id_extracted_from_url(self):
        from bs4 import BeautifulSoup
        scraper = OLXScraper()
        soup = BeautifulSoup(self.SAMPLE_HTML, "lxml")
        items = scraper._parse_listing_page(soup, "Lisboa")
        assert items[0]["external_id"] == "abc123"

    def test_area_extracted_from_title(self):
        scraper = OLXScraper()
        area = scraper._extract_area_from_title("T2 Lisboa Alvalade 78m²")
        assert area == "78m²"

    def test_area_not_in_title_returns_none(self):
        scraper = OLXScraper()
        area = scraper._extract_area_from_title("T2 Lisboa bom apartamento")
        assert area is None


class TestImovirtualScraper:

    SAMPLE_HTML = """
    <html><body>
    <article data-cy="listing-item" data-id="101">
        <a data-cy="listing-item-title" href="/imovel/t2-lisboa-ID101.html">T2 Lisboa Campolide</a>
        <strong data-cy="listing-item-price">220 000 €</strong>
        <li aria-label="Área">85 m²</li>
        <p data-cy="listing-item-address">Campolide, Lisboa</p>
    </article>
    </body></html>
    """

    def test_parse_returns_item(self):
        from bs4 import BeautifulSoup
        scraper = ImovirtualScraper()
        soup = BeautifulSoup(self.SAMPLE_HTML, "lxml")
        items = scraper._parse_listing_page(soup, "Lisboa")
        assert len(items) == 1
        assert items[0]["title"] == "T2 Lisboa Campolide"
        assert items[0]["price_raw"] == "220 000 €"
        assert items[0]["area_raw"] == "85 m²"

    def test_external_id_set(self):
        from bs4 import BeautifulSoup
        scraper = ImovirtualScraper()
        soup = BeautifulSoup(self.SAMPLE_HTML, "lxml")
        items = scraper._parse_listing_page(soup, "Lisboa")
        assert items[0]["external_id"] == "101"

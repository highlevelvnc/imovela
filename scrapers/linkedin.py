"""
LinkedIn scraper — EXPERIMENTAL property owner signal detection.
URL: https://www.linkedin.com/search/results/people/

⚠️  EXPERIMENTAL STATUS: LinkedIn blocks scraping aggressively.
    This scraper requires the user to log in manually first.
    It works with LOW volumes only (20-30 profiles per session).
    For scale, use LinkedIn Sales Navigator API.

Purpose
-------
Find potential property owners by searching for LinkedIn profiles with
keywords like "proprietário", "investidor imobiliário", "senhorio" in
target zones (Lisboa, Almada, Seixal, Sesimbra, Cascais, Sintra).

Strategy
--------
1. User logs into LinkedIn in a regular Chrome browser
2. Scraper launches Playwright with persistent browser context (reuses cookies)
3. Searches for relevant keywords per zone
4. Extracts: name, headline, location, profile URL
5. Visits profile pages to extract contact info (phone/email from /contact-info/)
6. Very conservative rate limiting to avoid account suspension

Source tag: "linkedin"
"""
from __future__ import annotations

import asyncio
import os
import random
import re
from pathlib import Path
from typing import Iterator

import httpx

from config.zone_config import get_random_user_agent
from scrapers.base import BaseScraper
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.linkedin.com"

# Keywords to search for property owners / investors / landlords
SEARCH_KEYWORDS: list[str] = [
    "proprietário imóvel",
    "investidor imobiliário",
    "senhorio",
    "dono imóvel",
    "investimento imobiliário",
]

# LinkedIn geo URN IDs for Portuguese locations
# These are approximate — LinkedIn geo IDs need to be validated
ZONE_GEO_URNS: dict[str, str] = {
    "Lisboa":   "105723847",   # Greater Lisbon Area
    "Cascais":  "105723847",   # Uses Lisbon area (Cascais is within)
    "Sintra":   "105723847",   # Uses Lisbon area
    "Almada":   "105723847",   # Uses Lisbon area (Setúbal district)
    "Seixal":   "105723847",   # Uses Lisbon area
    "Sesimbra": "105723847",   # Uses Lisbon area
}

# Conservative limits to avoid LinkedIn account suspension
_MAX_PROFILES_PER_SESSION = 25    # Total profiles to visit per run
_MAX_SEARCH_PAGES = 2             # Max search result pages per keyword
_DELAY_BETWEEN_PROFILES = (4, 8)  # Seconds between profile visits
_DELAY_BETWEEN_PAGES = (6, 12)    # Seconds between search pages

# Path to persistent browser profile for LinkedIn cookies
_BROWSER_PROFILE_DIR = Path.home() / ".claude" / "linkedin_browser_profile"


class LinkedInScraper(BaseScraper):
    """
    EXPERIMENTAL LinkedIn scraper for property owner signals.

    Requires:
    1. Playwright installed: `playwright install chromium`
    2. Manual LinkedIn login: Run `python -m scrapers.linkedin --login` first
       to open a browser window where you can log in. Cookies are saved.
    3. Very conservative rate limiting (25 profiles max per session)

    ⚠️  Using this scraper excessively WILL result in LinkedIn account
        restrictions. Use sparingly and monitor for warnings.
    """

    SOURCE = "linkedin"

    def __init__(self):
        super().__init__()
        self._profile_count = 0

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """Yield LinkedIn profile data for potential property owners in zone."""
        geo_urn = ZONE_GEO_URNS.get(zone)
        if not geo_urn:
            log.warning("[linkedin] No geo URN for zone: {z}", z=zone)
            return

        log.info("[linkedin] zone={z} starting experimental scrape", z=zone)
        try:
            items = asyncio.run(self._async_scrape_zone(zone, geo_urn))
            for item in items:
                yield item
        except ImportError:
            log.error("[linkedin] Playwright not installed. Run: playwright install chromium")
        except Exception as e:
            log.error("[linkedin] Scrape failed zone={z}: {e}", z=zone, e=e)

    async def _async_scrape_zone(self, zone: str, geo_urn: str) -> list[dict]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError("playwright is not installed")

        from config.settings import settings

        items: list[dict] = []

        # Ensure browser profile directory exists
        _BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

        async with async_playwright() as pw:
            # Use persistent context to preserve LinkedIn login cookies
            context = await pw.chromium.launch_persistent_context(
                str(_BROWSER_PROFILE_DIR),
                headless=settings.headless_browser,
                viewport={"width": 1366, "height": 768},
                user_agent=get_random_user_agent(),
                locale="pt-PT",
                timezone_id="Europe/Lisbon",
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            page = context.pages[0] if context.pages else await context.new_page()

            # Check if logged in
            if not await self._is_logged_in(page):
                log.error(
                    "[linkedin] Not logged in. Run with --login flag first to authenticate: "
                    "python -m scrapers.linkedin --login"
                )
                await context.close()
                return []

            # Search for each keyword (rotate through, stop when budget hit)
            for keyword in SEARCH_KEYWORDS:
                if self._profile_count >= _MAX_PROFILES_PER_SESSION:
                    log.info("[linkedin] Profile budget exhausted ({n})", n=self._profile_count)
                    break

                search_items = await self._search_keyword(page, keyword, zone, geo_urn)
                items.extend(search_items)

                # Rest between keyword searches
                await asyncio.sleep(random.uniform(*_DELAY_BETWEEN_PAGES))

            await context.close()

        log.info("[linkedin] zone={z} → {n} profiles extracted", z=zone, n=len(items))
        return items

    async def _is_logged_in(self, page) -> bool:
        """Check if LinkedIn session is active."""
        try:
            await page.goto(f"{BASE_URL}/feed/", wait_until="domcontentloaded", timeout=15_000)
            await asyncio.sleep(2)
            # If redirected to login page, we're not logged in
            if "/login" in page.url or "/authwall" in page.url:
                return False
            # Check for feed content (logged-in indicator)
            feed = await page.query_selector("[data-test-id='feed-container'], .feed-shared-update-v2")
            return feed is not None
        except Exception:
            return False

    async def _search_keyword(self, page, keyword: str, zone: str, geo_urn: str) -> list[dict]:
        """Search LinkedIn People for a keyword in a zone."""
        items = []

        for page_num in range(1, _MAX_SEARCH_PAGES + 1):
            if self._profile_count >= _MAX_PROFILES_PER_SESSION:
                break

            search_url = (
                f"{BASE_URL}/search/results/people/"
                f"?keywords={keyword.replace(' ', '%20')}"
                f"&geoUrn=%5B%22{geo_urn}%22%5D"
                f"&origin=FACETED_SEARCH"
            )
            if page_num > 1:
                search_url += f"&page={page_num}"

            log.debug("[linkedin] Searching: {kw} page={p}", kw=keyword, p=page_num)

            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
                await asyncio.sleep(random.uniform(2.0, 4.0))

                # Check for rate limit or block
                if "/checkpoint/" in page.url or "restricted" in page.url.lower():
                    log.warning("[linkedin] Rate limited or blocked — stopping")
                    break

                # Parse search results
                result_cards = await page.query_selector_all(
                    "li.reusable-search__result-container, "
                    "div.entity-result, "
                    "li[class*='search-result']"
                )

                if not result_cards:
                    log.debug("[linkedin] No results for '{kw}' page={p}", kw=keyword, p=page_num)
                    break

                for card in result_cards:
                    if self._profile_count >= _MAX_PROFILES_PER_SESSION:
                        break

                    try:
                        item = await self._parse_search_card(card, zone, keyword)
                        if item:
                            # Visit profile for contact info
                            contact = await self._extract_contact_info(page, item["url"])
                            if contact:
                                item.update(contact)
                            items.append(item)
                            self._profile_count += 1

                            # Conservative delay between profile visits
                            await asyncio.sleep(random.uniform(*_DELAY_BETWEEN_PROFILES))

                    except Exception as e:
                        log.debug("[linkedin] Card parse error: {e}", e=e)

            except Exception as e:
                log.warning("[linkedin] Search error kw='{kw}' page={p}: {e}", kw=keyword, p=page_num, e=e)
                break

            # Delay between search pages
            await asyncio.sleep(random.uniform(*_DELAY_BETWEEN_PAGES))

        return items

    async def _parse_search_card(self, card, zone: str, keyword: str) -> dict | None:
        """Parse a LinkedIn search result card."""
        try:
            # Name
            name_el = await card.query_selector(
                "span.entity-result__title-text a span[aria-hidden='true'], "
                "span.actor-name, "
                "a[class*='app-aware-link'] span[dir='ltr']"
            )
            name = (await name_el.inner_text()).strip() if name_el else None
            if not name or len(name) < 2:
                return None

            # Profile URL
            link_el = await card.query_selector(
                "a.app-aware-link[href*='/in/'], "
                "a[href*='/in/']"
            )
            href = (await link_el.get_attribute("href")) if link_el else None
            if not href or "/in/" not in href:
                return None
            # Clean URL — remove query params
            profile_url = href.split("?")[0]
            if not profile_url.startswith("http"):
                profile_url = f"{BASE_URL}{profile_url}"

            # Headline
            headline_el = await card.query_selector(
                "div.entity-result__primary-subtitle, "
                "p[class*='subline-level-1'], "
                "div[class*='search-result__info'] p"
            )
            headline = (await headline_el.inner_text()).strip() if headline_el else None

            # Location
            loc_el = await card.query_selector(
                "div.entity-result__secondary-subtitle, "
                "p[class*='subline-level-2']"
            )
            location = (await loc_el.inner_text()).strip() if loc_el else zone

            return {
                "external_id":    profile_url.rstrip("/").split("/")[-1],
                "url":            profile_url,
                "title":          f"{name} — {headline}" if headline else name,
                "author_name":    name,
                "text":           headline or "",
                "location":       location,
                "zone_query":     zone,
                "contact_name":   name,
                "contact_phone":  None,
                "contact_email":  None,
                "contact_source": None,
                "source":         "linkedin",
                "search_keyword": keyword,
            }

        except Exception as e:
            log.debug("[linkedin] _parse_search_card error: {e}", e=e)
            return None

    async def _extract_contact_info(self, page, profile_url: str) -> dict | None:
        """Visit a LinkedIn profile and extract contact information."""
        try:
            contact_url = f"{profile_url.rstrip('/')}/overlay/contact-info/"
            await page.goto(contact_url, wait_until="domcontentloaded", timeout=15_000)
            await asyncio.sleep(random.uniform(1.5, 3.0))

            result = {}

            # Phone numbers
            phone_section = await page.query_selector("section.ci-phone, [class*='ci-phone']")
            if phone_section:
                phone_els = await phone_section.query_selector_all("span.t-14, span[class*='t-14']")
                for el in phone_els:
                    text = (await el.inner_text()).strip()
                    clean = re.sub(r"[\s\-\(\)]", "", text).lstrip("+")
                    if clean.startswith("351"):
                        clean = clean[3:]
                    if re.match(r"^[2679]\d{8}$", clean):
                        result["contact_phone"] = f"+351{clean}"
                        result["contact_source"] = "linkedin"
                        break

            # Email
            email_section = await page.query_selector("section.ci-email, [class*='ci-email']")
            if email_section:
                email_els = await email_section.query_selector_all("a[href^='mailto:']")
                for el in email_els:
                    href = (await el.get_attribute("href")) or ""
                    email = href.replace("mailto:", "").strip()
                    if "@" in email:
                        result["contact_email"] = email
                        if not result.get("contact_source"):
                            result["contact_source"] = "linkedin"
                        break

            # Birthday (if visible)
            birthday_section = await page.query_selector("section.ci-birthday, [class*='ci-birthday']")
            if birthday_section:
                bday_el = await birthday_section.query_selector("span.t-14, span[class*='t-14']")
                if bday_el:
                    result["birthday"] = (await bday_el.inner_text()).strip()

            return result if result else None

        except Exception as e:
            log.debug("[linkedin] Contact info extraction error {u}: {e}", u=profile_url, e=e)
            return None


# ── CLI entry point for manual login ──────────────────────────────────────────

def _login_interactive():
    """Open a browser window for manual LinkedIn login."""
    import asyncio

    async def _do_login():
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            print("Playwright not installed. Run: playwright install chromium")
            return

        _BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

        print("\n" + "=" * 60)
        print("LinkedIn Login — Browser Window")
        print("=" * 60)
        print("1. A browser window will open.")
        print("2. Log into your LinkedIn account.")
        print("3. Once logged in, close the browser window.")
        print("4. Your session cookies will be saved for the scraper.")
        print("=" * 60 + "\n")

        async with async_playwright() as pw:
            context = await pw.chromium.launch_persistent_context(
                str(_BROWSER_PROFILE_DIR),
                headless=False,  # Must be visible for manual login
                viewport={"width": 1366, "height": 768},
                locale="pt-PT",
                timezone_id="Europe/Lisbon",
            )
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")

            print("Waiting for login... Close the browser when done.")

            # Wait until the user closes the browser
            try:
                await page.wait_for_event("close", timeout=300_000)  # 5 min timeout
            except Exception:
                pass

            await context.close()
            print("\nSession saved. You can now run the LinkedIn scraper.")

    asyncio.run(_do_login())


if __name__ == "__main__":
    import sys
    if "--login" in sys.argv:
        _login_interactive()
    else:
        print("Usage: python -m scrapers.linkedin --login")
        print("  Opens a browser for manual LinkedIn login.")
        print("  Cookies are saved for the scraper to reuse.")

"""
OLX Portugal scraper — real estate listings.
URL base: https://www.olx.pt/imoveis/

Architecture:
- Listing grid: server-side rendered HTML → httpx + BeautifulSoup
- Zone URLs: /imoveis/q-{zone}/ with ?page=N for pagination
- Detail page: fetched per OLX-native listing to get structured data
- Rate limiting: inherited from BaseScraper (2–6s between requests)

Selectors (validated against live OLX PT HTML, March 2026):
  Card grid  — div[data-cy='l-card']
  Title      — div[data-cy='ad-card-title'] h4
  Price      — p[data-testid='ad-price']
  Location   — p[data-testid='location-date']
  Card ID    — card.get('id')  attribute on the l-card div
  Area       — span[data-nx-name='P5']  (in card grid)
  Pagination — a[data-cy='pagination-forward']

  Detail page:
  Description — [data-testid='ad_description']  (underscore, not dash)
  Parameters  — [data-testid='ad-parameters-container'] p  (label: value pairs)
  Seller name — [data-testid='user-profile-user-name']

Zone URL mapping:
  Lisboa   → /imoveis/q-lisboa/
  Cascais  → /imoveis/q-cascais/
  Sintra   → /imoveis/q-sintra/
  Almada   → /imoveis/q-almada/
  Seixal   → /imoveis/q-seixal/
  Sesimbra → /imoveis/q-sesimbra/

Note: OLX PT aggregates listings from partner portals (imovirtual.com, etc.).
These cross-posts are skipped to avoid duplication with other scrapers.
Debug: set LOG_LEVEL=DEBUG to dump raw HTML to /tmp/olx_debug_{zone}.html
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import re
from typing import Iterator
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from config.settings import settings
from config.zone_config import get_pw_limit, get_random_user_agent
from scrapers.base import BaseScraper, PlaywrightPhoneRevealer
from utils.logger import get_logger
from utils.phone import (
    best_phone,
    extract_phone_from_tel_href,
    extract_phone_from_text,
    extract_whatsapp,
    validate_pt_phone,
)
from utils.phone_discovery import discover_phones, discover_whatsapp
from utils.email_extractor import extract_first_email

log = get_logger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.olx.pt"

# Zone → URL slug
#
# OLX uses a municipality-level slug for its /imoveis/q-{slug}/ search.
# Freguesia-level queries fall back to the parent municipality; adding them
# here would duplicate work, so only municipalities are enumerated.
ZONE_SLUGS: dict[str, str] = {
    # ── Primary targets ───────────────────────────────────────────────────
    "Lisboa":   "lisboa",
    "Cascais":  "cascais",
    "Sintra":   "sintra",
    "Almada":   "almada",
    "Seixal":   "seixal",
    "Sesimbra": "sesimbra",

    # ── Lisbon freguesias — route to municipality query ───────────────────
    "Lisboa-Alvalade":              "lisboa",
    "Lisboa-Areeiro":               "lisboa",
    "Lisboa-Arroios":               "lisboa",
    "Lisboa-Avenidas-Novas":        "lisboa",
    "Lisboa-Beato":                 "lisboa",
    "Lisboa-Belem":                 "lisboa",
    "Lisboa-Benfica":               "lisboa",
    "Lisboa-Campo-de-Ourique":      "lisboa",
    "Lisboa-Campolide":             "lisboa",
    "Lisboa-Carnide":               "lisboa",
    "Lisboa-Estrela":               "lisboa",
    "Lisboa-Lumiar":                "lisboa",
    "Lisboa-Marvila":               "lisboa",
    "Lisboa-Misericordia":          "lisboa",
    "Lisboa-Olivais":               "lisboa",
    "Lisboa-Parque-das-Nacoes":     "lisboa",
    "Lisboa-Penha-de-Franca":       "lisboa",
    "Lisboa-Santa-Clara":           "lisboa",
    "Lisboa-Santa-Maria-Maior":     "lisboa",
    "Lisboa-Santo-Antonio":         "lisboa",
    "Lisboa-Sao-Domingos-de-Benfica": "lisboa",
    "Lisboa-Sao-Vicente":           "lisboa",

    # ── Grande Lisboa adjacents ───────────────────────────────────────────
    "Oeiras":              "oeiras",
    "Amadora":             "amadora",
    "Loures":              "loures",
    "Odivelas":            "odivelas",
    "Vila-Franca-de-Xira": "vila-franca-de-xira",
    "Mafra":               "mafra",

    # ── Margem Sul / Setúbal ──────────────────────────────────────────────
    "Barreiro":  "barreiro",
    "Montijo":   "montijo",
    "Palmela":   "palmela",
    "Setubal":   "setubal",
    "Moita":     "moita",
    "Alcochete": "alcochete",
}

# Set False to disable rental scraping (e.g. during recovery / testing)
SCRAPE_RENTALS: bool = True

# When True, the rental ("arrendamento") path is scraped FIRST and its
# Playwright phone-reveal budget is consumed before the buy path. The
# client's FRBO list comes exclusively from OLX rentals — prioritising
# that path means rental leads dominate the Playwright reveals when
# the budget is tight.
FRBO_FIRST: bool = True

# External portal domains to skip (handled by dedicated scrapers)
EXTERNAL_DOMAINS = frozenset({
    "imovirtual.com",
    "idealista.pt",
    "casa.sapo.pt",
    "remax.pt",
    "era.pt",
    "kw.pt",
    "kwportugal.pt",
})

# Max detail-page fetches per zone run (avoids excessive requests)
MAX_DETAIL_FETCHES_PER_ZONE = 80

# Max Playwright phone-reveal fetches per zone run.
# Each fetch takes ~3–5s (page load + click + wait).
# 30 fetches ≈ 1.5–2.5 extra minutes per zone.
MAX_PLAYWRIGHT_PHONE_FETCHES = 30

# OLX PT "Mostrar número" button — selectors tried in priority order.
# OLX changes selectors periodically; multiple attempts make this robust.
_PHONE_BTN_SELECTORS = [
    "button[data-testid='show-phone']",          # confirmed OLX PT 2026-03 (live validation)
    "a[data-cy='call-ad-button']",               # alternative OLX PT selector
    "button[data-testid='show-phone-button']",
    "button[data-testid='phone-show-number']",
    "a[data-testid='show-phone']",
    "button[class*='show-phone']",
    "a[class*='show-phone']",
]

# OneTrust / generic cookie consent modal selectors.
# Must be dismissed before clicking the phone button (intercepts pointer events).
_CONSENT_SELECTORS = [
    "#onetrust-accept-btn-handler",
    "button#onetrust-accept-btn-handler",
    "button.onetrust-close-btn-handler",
    "#accept-recommended-btn-handler",
    "button[title='Aceitar Todos os Cookies']",
    "button[title='Accept All Cookies']",
    ".ot-pc-refuse-all-handler",
    "button[id*='accept'][id*='cookie']",
    "button[class*='accept'][class*='cookie']",
]


class OLXScraper(BaseScraper):
    """
    Scrapes OLX Portugal /imoveis/ listings.

    Args:
        max_pages:     Maximum pages to paginate per zone (default: 5).
        fetch_details: Whether to fetch each ad's detail page for richer data.
                       Doubles request count but provides:
                       description, typology, area, condition, seller info.
    """

    SOURCE = "olx"

    def __init__(self, max_pages: int = 15, fetch_details: bool = True, fetch_phone: bool = True):
        """
        Args:
            max_pages:    Max pages to paginate per zone.
            fetch_details: Fetch each ad's detail page (description, params, seller).
            fetch_phone:  After the httpx detail fetch, attempt Playwright phone reveal
                          for ads still missing a phone number. Requires playwright to
                          be installed (`playwright install chromium`). Silently skipped
                          when Playwright is not available.
        """
        super().__init__()
        self.max_pages     = max_pages
        self.fetch_details = fetch_details
        self.fetch_phone   = fetch_phone
        self._pw_phone_count = 0   # resets each zone via scrape_zone

    # ── Public interface ──────────────────────────────────────────────────────

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """
        Yield raw listing dicts for the given zone.

        Scrapes buy listings first, then (if SCRAPE_RENTALS is True) rental
        listings. Both passes share the Playwright phone budget to prevent
        doubling overhead per zone.

        Lisbon freguesia zones (e.g. "Lisboa-Alvalade") are covered by the
        parent Lisboa query and skipped here — OLX has no freguesia-level
        URL paths, so drilling down would only duplicate requests.
        """
        if zone.startswith("Lisboa-"):
            log.debug(
                "[olx] zone={z} is a Lisboa freguesia — skipping (already covered by Lisboa)",
                z=zone,
            )
            return

        slug     = ZONE_SLUGS.get(zone, zone.lower())
        pw_limit = get_pw_limit(zone)
        self._pw_phone_count = 0   # reset per zone — shared across buy + rent
        self._detail_count   = 0   # shared across buy + rent

        buy_base    = f"{BASE_URL}/imoveis/q-{slug}"
        rental_base = f"{BASE_URL}/imoveis/q-arrendamento-{slug}"

        if FRBO_FIRST and SCRAPE_RENTALS:
            # ── Rental first (client's FRBO list source) ──────────────────
            log.debug("[olx] zone={z} FRBO-first — scraping rentals first", z=zone)
            yield from self._scrape_listing_path(client, zone, rental_base, "rent", pw_limit)
            yield from self._scrape_listing_path(client, zone, buy_base,    "buy",  pw_limit)
        else:
            # Legacy ordering — buy then rent
            yield from self._scrape_listing_path(client, zone, buy_base, "buy", pw_limit)
            if SCRAPE_RENTALS:
                yield from self._scrape_listing_path(client, zone, rental_base, "rent", pw_limit)

    def _scrape_listing_path(
        self,
        client: httpx.Client,
        zone: str,
        url_base: str,
        label: str,
        pw_limit: int,
    ) -> Iterator[dict]:
        """Core pagination loop for a single listing path (buy or rent)."""
        page = 1

        while page <= self.max_pages:
            if page == 1:
                url = f"{url_base}/"
            else:
                url = f"{url_base}/?page={page}"

            log.info("[olx] zone={z} {lbl} page={p} → {url}", z=zone, lbl=label, p=page, url=url)

            resp = self._get(client, url)
            if not resp:
                log.warning("[olx] No response zone={z} {lbl} page={p} — stopping", z=zone, lbl=label, p=page)
                break

            if page == 1:
                self._debug_dump(resp.text, f"{zone}_{label}")

            soup  = BeautifulSoup(resp.text, "lxml")
            cards = self._find_cards(soup)

            if not cards:
                log.warning(
                    "[olx] Zero cards zone={z} {lbl} page={p} — possible HTML change",
                    z=zone, lbl=label, p=page,
                )
                self._debug_dump(resp.text, f"{zone}_{label}", force=True)
                break

            # Parse cards first so delta-crawl can inspect the full page
            parsed_cards: list[dict] = []
            for card in cards:
                try:
                    item = self._parse_card(card, zone)
                    if item:
                        parsed_cards.append(item)
                except Exception as e:
                    log.debug("[olx] Card parse error zone={z} {lbl}: {e}", z=zone, lbl=label, e=e)

            # Delta-crawl check — stop paginating when page is mostly-seen
            if self._page_is_mostly_seen(parsed_cards):
                log.info(
                    "[olx] zone={z} {lbl} page={p} delta-stop — pagination halted",
                    z=zone, lbl=label, p=page,
                )
                break

            page_items: list[dict] = list(parsed_cards)

            # ── Parallel detail fetch (the big speedup) ──────────────────
            # Each card normally needs a follow-up GET for description /
            # phone / params / seller. Doing those serially with the 2-6s
            # rate-limit between them blew page time to 1-2 minutes. Batch
            # them here with ``parallel_fetch`` (concurrency=4) so a 24-card
            # page finishes in ~10-12s without changing per-host RPS.
            if self.fetch_details:
                self._enrich_batch_async(parsed_cards)

            # ── Batch Playwright reveal — only items still without a REAL phone ──
            # "Real" = mobile or landline. Relay (66X) counts as missing because
            # the batch pass may surface the seller's direct number via click reveal.
            if self.fetch_phone and self._pw_phone_count < pw_limit:
                remaining = pw_limit - self._pw_phone_count
                pending = [
                    it for it in page_items
                    if self._needs_playwright(it) and it.get("url")
                ][:remaining]

                if pending:
                    log.info(
                        "[olx] zone={z} {lbl} page={p} Playwright batch — {n} URLs",
                        z=zone, lbl=label, p=page, n=len(pending),
                    )
                    revealer = PlaywrightPhoneRevealer(
                        phone_btn_selectors=_PHONE_BTN_SELECTORS,
                        consent_selectors=_CONSENT_SELECTORS,
                        headless=settings.headless_browser,
                    )
                    reveals = revealer.reveal_batch([it["url"] for it in pending])

                    for it in pending:
                        revealed = reveals.get(it["url"])
                        if revealed:
                            # Compare against current phone — only overwrite if the
                            # new candidate is better (mobile > landline > relay).
                            current = it.get("contact_phone")
                            best = best_phone([current, revealed]) if current else validate_pt_phone(revealed)
                            if best and best.valid:
                                if best.canonical != current:
                                    it["contact_phone"] = best.canonical
                                    it["contact_source"] = "olx_playwright"
                                    it["phone_type"] = best.phone_type
                                    it["contact_confidence"] = best.confidence

                    self._pw_phone_count += len(pending)

            yield from page_items

            log.debug("[olx] zone={z} {lbl} page={p} → {n} cards", z=zone, lbl=label, p=page, n=len(cards))

            if not self._has_next_page(soup):
                log.debug("[olx] No next page — stopping zone={z} {lbl}", z=zone, lbl=label)
                break

            page += 1

    # ── Concurrent detail fetcher ────────────────────────────────────────────

    def _enrich_batch_async(self, items: list[dict]) -> None:
        """
        Fetch every card's detail page in parallel and run _enrich_with_detail
        equivalent against the cached HTML. Skips items already in the delta
        cache (we already have them) and respects the per-zone budget.
        """
        from utils.async_fetcher import parallel_fetch

        # Build the work set: only cards we don't already know AND still
        # under the per-zone detail budget.
        targets: list[dict] = []
        for item in items:
            if item.get("external_id") in self.known_external_ids:
                continue
            if not item.get("url"):
                continue
            if self._detail_count >= MAX_DETAIL_FETCHES_PER_ZONE:
                break
            targets.append(item)
            self._detail_count += 1

        if not targets:
            return

        # URL → item lookup so the on_response callback can find its row
        index = {it["url"]: it for it in targets}

        def _on_response(url: str, body: str) -> None:
            item = index.get(url)
            if not item:
                return
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(body, "html.parser")
                self._populate_detail(soup, body, item)
            except Exception as e:
                log.debug("[olx] async detail parse {u}: {e}", u=url[-60:], e=e)

        headers = {
            "User-Agent":      get_random_user_agent(),
            "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        log.info("[olx] async detail fetch — {n} URLs in flight", n=len(targets))
        parallel_fetch(
            urls=list(index.keys()),
            headers=headers,
            concurrency=4,
            timeout=20.0,
            on_response=_on_response,
        )

    def _populate_detail(self, soup, page_html: str, item: dict) -> None:
        """
        Per-card body of the original ``_enrich_with_detail``, refactored
        so the parallel fetcher and the legacy serial path share the same
        extraction logic.
        """
        from bs4 import BeautifulSoup
        # ── Description
        desc_el = soup.select_one("[data-testid='ad_description']")
        if desc_el:
            item["description"] = desc_el.get_text(" ", strip=True)[:4000]

        # ── Params
        params_el = soup.select_one("[data-testid='ad-parameters-container']")
        if params_el:
            seller_type_param = None
            for p in params_el.select("p"):
                text = p.get_text(strip=True)
                if not text:
                    continue
                if text.startswith("Tipologia:"):
                    val = text.split(":", 1)[1].strip()
                    if val and not item.get("typology_raw"):
                        item["typology_raw"] = val
                elif re.match(r"^Área", text, re.IGNORECASE):
                    val = text.split(":", 1)[-1].strip()
                    if val and not item.get("area_raw"):
                        item["area_raw"] = val
                elif text.startswith("Condição:"):
                    item["condition_raw"] = text.split(":", 1)[1].strip()
                elif text.startswith("Nº divisões:"):
                    item["rooms_raw"] = text.split(":", 1)[1].strip()
                elif text.startswith("Casas de Banho:"):
                    item["bathrooms_raw"] = text.split(":", 1)[1].strip()
                elif text.startswith("Mobilado:"):
                    item["furnished_raw"] = text.split(":", 1)[1].strip()
                elif text.startswith("Certificado Energético:"):
                    item["energy_cert"] = text.split(":", 1)[1].strip()
                elif text.lower() in ("particular", "empresa", "agência", "agencia",
                                      "profissional"):
                    seller_type_param = text
            if seller_type_param:
                item["is_owner"] = seller_type_param.lower() == "particular"

        # ── Trader title
        trader_el = soup.select_one("[data-testid='trader-title']")
        if trader_el:
            trader_text = trader_el.get_text(strip=True).lower()
            if trader_text == "utilizador":
                item["owner_type_raw"] = "fsbo"
                item["is_owner"] = True
            elif trader_text in ("empresa", "profissional"):
                item["owner_type_raw"] = "agency"
                item["is_owner"] = False

        # ── Seller name
        seller_el = soup.select_one("[data-testid='user-profile-user-name']")
        if seller_el:
            item["contact_name"] = seller_el.get_text(strip=True)[:200]

        # ── Seller profile URL
        seller_link = (
            soup.select_one("a[href*='/perfil/']") or
            soup.select_one("a[href*='/d/perfil/']")
        )
        if seller_link:
            href = seller_link.get("href", "")
            if href:
                profile_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                item["seller_profile_url"] = profile_url

        # ── Member since
        ms_el = soup.select_one("[data-testid='member-since']")
        if ms_el:
            item["member_since_raw"] = ms_el.get_text(strip=True)[:80]

        # ── JSON-LD
        self._parse_json_ld(soup, item)

        # ── Phone discovery (real-first, relay-last)
        self._extract_phone_static(soup, page_html, item)

        # ── Email
        if not item.get("contact_email"):
            email = extract_first_email(page_html)
            if email:
                item["contact_email"] = email

    # ── Playwright triage ────────────────────────────────────────────────────

    @staticmethod
    def _needs_playwright(item: dict) -> bool:
        """
        Return True when Playwright reveal is worth invoking for this item.

        Triggers a reveal when:
          - No phone was captured from HTML at all
          - The captured phone is a relay (66X) — a direct number might
            still be exposed after clicking the reveal button (rare but
            possible when the seller has WhatsApp configured)
        """
        phone = item.get("contact_phone")
        if not phone:
            return True
        result = validate_pt_phone(phone)
        return result.valid and result.phone_type == "relay"

    # ── Card detection ────────────────────────────────────────────────────────

    @staticmethod
    def _find_cards(soup: BeautifulSoup) -> list:
        """Locate listing cards using stable data-cy attributes."""
        cards = soup.select("div[data-cy='l-card']")
        if cards:
            return cards
        # Fallback for alternate card tag
        return soup.select("li[data-cy='l-card']")

    # ── Card parsing ──────────────────────────────────────────────────────────

    def _parse_card(self, card, zone: str) -> dict | None:
        """
        Extract structured fields from a single listing card.
        Returns None for:
        - Cards with no title
        - Cross-posts from external portals (imovirtual, idealista, etc.)
        """
        # ── Skip external portal cross-posts ──────────────────────────────────
        for a_el in card.select("a[href]"):
            href = a_el.get("href", "")
            if any(domain in href for domain in EXTERNAL_DOMAINS):
                return None   # handled by dedicated scrapers

        # ── Title ─────────────────────────────────────────────────────────────
        title_el = (
            card.select_one("div[data-cy='ad-card-title'] h4") or
            card.select_one("div[data-testid='ad-card-title'] h4") or
            card.select_one("h4") or
            card.select_one("h6")
        )
        title = title_el.get_text(strip=True) if title_el else None
        if not title or len(title) < 5:
            return None

        # ── URL ───────────────────────────────────────────────────────────────
        # First anchor with an OLX listing path (/d/anuncio/)
        url = None
        for a_el in card.select("a[href]"):
            href = a_el.get("href", "")
            if "/d/" in href:
                url = href if href.startswith("http") else urljoin(BASE_URL, href)
                break

        if not url:
            return None

        # ── External ID ───────────────────────────────────────────────────────
        # OLX puts the ad ID directly as the id= attribute on the l-card div
        external_id = card.get("id") or self._extract_id_from_url(url)

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = card.select_one("p[data-testid='ad-price']")
        price_raw = price_el.get_text(strip=True) if price_el else None

        # ── Location / Date ───────────────────────────────────────────────────
        loc_el = card.select_one("p[data-testid='location-date']")
        if loc_el:
            loc_text = loc_el.get_text(strip=True)
            # "Mina De Água - Para o topo a 14 de março" → keep part before " - "
            location_raw = loc_text.split(" - ")[0].strip() if " - " in loc_text else loc_text
        else:
            location_raw = zone

        # ── Area (from card-level parameter span) ─────────────────────────────
        area_el = card.select_one("span[data-nx-name='P5']")
        area_raw = None
        if area_el:
            area_text = area_el.get_text(strip=True)
            if "m²" in area_text or "m2" in area_text.lower():
                area_raw = area_text

        # ── Typology hint from title ───────────────────────────────────────────
        typology_raw = self._extract_typology_hint(title)

        return {
            "external_id":   external_id,
            "url":           url,
            "title":         title,
            "price_raw":     price_raw,
            "location_raw":  location_raw,
            "zone_query":    zone,
            "area_raw":      area_raw,
            "typology_raw":  typology_raw,
            "description":   None,       # populated by _enrich_with_detail
            "contact_name":  None,       # populated by _enrich_with_detail
            "contact_phone": None,
            "is_owner":      None,       # populated by _enrich_with_detail
            "condition_raw": None,       # populated by _enrich_with_detail
        }

    # ── Detail page ───────────────────────────────────────────────────────────

    def _enrich_with_detail(self, client: httpx.Client, item: dict) -> None:
        """
        Fetch the individual OLX ad page and extract:
        - Full description (up to 4000 chars)
        - Structured parameters: all known OLX property params
        - Seller name, type (Particular/Empresa), and account tier
        - owner_type_raw from trader-title (more reliable than text heuristics)
        - Structured data from JSON-LD (price, city, ad ID)
        - Best phone from multiple static-HTML sources (tel: href, WhatsApp
          links, description text). Avoids wasting Playwright budget when
          the seller's direct number is already visible without clicking.
        """
        try:
            resp = self._get(client, item["url"])
            if not resp:
                return

            soup = BeautifulSoup(resp.text, "html.parser")
            page_html = resp.text

            # ── Description ───────────────────────────────────────────────────
            # OLX uses ad_description with UNDERSCORE (not dash).
            # Increased limit to 4000 to avoid cutting contact info in long ads.
            desc_el = soup.select_one("[data-testid='ad_description']")
            if desc_el:
                item["description"] = desc_el.get_text(" ", strip=True)[:4000]

            # ── Property parameters ───────────────────────────────────────────
            # Each <p> inside the container is "Label: Value" or a standalone value.
            # Known OLX PT params (validated live 2026-03):
            #   Tipologia: T1/T2/T3/T4/...
            #   Área útil: 85 m² | Área bruta: 120 m²
            #   Condição: Novo / Renovado / Usado / Para recuperar
            #   Nº divisões: 3
            #   Casas de Banho: 2
            #   Mobilado: Sim / Não
            #   Certificado Energético: A+ / B / C / ...
            #   Particular (standalone) | Empresa (standalone)
            params_el = soup.select_one("[data-testid='ad-parameters-container']")
            if params_el:
                seller_type_param = None
                for p in params_el.select("p"):
                    text = p.get_text(strip=True)
                    if not text:
                        continue

                    if text.startswith("Tipologia:"):
                        val = text.split(":", 1)[1].strip()
                        if val and not item.get("typology_raw"):
                            item["typology_raw"] = val

                    elif re.match(r"^Área", text, re.IGNORECASE):
                        val = text.split(":", 1)[-1].strip()
                        if val and not item.get("area_raw"):
                            item["area_raw"] = val

                    elif text.startswith("Condição:"):
                        item["condition_raw"] = text.split(":", 1)[1].strip()

                    elif text.startswith("Nº divisões:"):
                        item["rooms_raw"] = text.split(":", 1)[1].strip()

                    elif text.startswith("Casas de Banho:"):
                        item["bathrooms_raw"] = text.split(":", 1)[1].strip()

                    elif text.startswith("Mobilado:"):
                        item["furnished_raw"] = text.split(":", 1)[1].strip()

                    elif text.startswith("Certificado Energético:"):
                        item["energy_cert"] = text.split(":", 1)[1].strip()

                    elif text.lower() in ("particular", "empresa", "agência", "agencia",
                                          "profissional"):
                        seller_type_param = text

                if seller_type_param:
                    item["is_owner"] = seller_type_param.lower() == "particular"

            # ── Trader title — most reliable seller-type signal ───────────────
            # data-testid='trader-title' contains:
            #   "Utilizador"   → private individual → fsbo
            #   "Empresa"      → registered business → agency
            #   "Profissional" → professional seller  → agency
            trader_el = soup.select_one("[data-testid='trader-title']")
            if trader_el:
                trader_text = trader_el.get_text(strip=True).lower()
                if trader_text == "utilizador":
                    item["owner_type_raw"] = "fsbo"
                    item["is_owner"] = True
                elif trader_text in ("empresa", "profissional"):
                    item["owner_type_raw"] = "agency"
                    item["is_owner"] = False

            # ── Seller name ───────────────────────────────────────────────────
            seller_el = soup.select_one("[data-testid='user-profile-user-name']")
            if seller_el:
                item["contact_name"] = seller_el.get_text(strip=True)[:200]

            # ── Seller profile URL (key for the seller-profile sweep) ─────────
            # OLX puts the profile path on the wrapper anchor: /perfil/<slug>/
            # Capturing it now means the seller-profile enricher can group
            # listings by seller without re-parsing the detail page later.
            seller_link = (
                soup.select_one("a[href*='/perfil/']") or
                soup.select_one("a[href*='/d/perfil/']")
            )
            if seller_link:
                href = seller_link.get("href", "")
                if href:
                    profile_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                    item["seller_profile_url"] = profile_url

            # ── Member since (trust signal) ────────────────────────────────────
            ms_el = soup.select_one("[data-testid='member-since']")
            if ms_el:
                item["member_since_raw"] = ms_el.get_text(strip=True)[:80]

            # ── JSON-LD structured data ────────────────────────────────────────
            # Provides: numeric price, precise city, ad SKU. Runs after HTML
            # parsing so HTML-derived values take priority where both exist.
            self._parse_json_ld(soup, item)

            # ── Multi-source phone extraction (static HTML) ────────────────────
            # Gathers candidates from the detail page without invoking Playwright,
            # then picks the highest-priority one (mobile > landline > relay).
            self._extract_phone_static(soup, page_html, item)

            # ── Email extraction (static HTML) ─────────────────────────────────
            # mailto: hrefs → JSON-LD → description → obfuscation patterns.
            # No extra requests — runs on the HTML already in memory.
            if not item.get("contact_email"):
                email = extract_first_email(page_html)
                if email:
                    item["contact_email"] = email

        except Exception as e:
            log.debug("[olx] Detail page error url={url}: {e}", url=item.get("url", ""), e=e)

    def _extract_phone_static(self, soup: BeautifulSoup, page_html: str, item: dict) -> None:
        """
        Aggressive multi-source phone discovery — runs ``discover_phones``
        which sweeps WhatsApp deep links, microdata, data-* attrs, hidden
        inputs, JSON-LD, JSON key/value pairs, inline ``<script>`` stores,
        ``<meta>`` tags, and finally the description text.

        Real seller numbers leak in JS-injected ``data-phone`` attributes
        and SSR seller payloads on OLX more often than on the visible HTML.
        Catching them here means we skip the masking-relay (6XX) entirely
        whenever a direct mobile is reachable.

        Writes to ``item``:
          contact_phone, contact_source, phone_type, contact_confidence,
          contact_whatsapp
        """
        # Capture WhatsApp first (always direct, never relay)
        for wa in discover_whatsapp(page_html, soup=soup):
            if not item.get("contact_whatsapp"):
                item["contact_whatsapp"] = wa
                break

        desc = item.get("description") or ""

        # Try real-only first — if anything but a 6XX is around, take it
        non_relay = discover_phones(
            page_html, soup=soup, description=desc, allow_relay=False,
        )
        if non_relay:
            picked = best_phone(non_relay)
            if picked and picked.valid:
                self._maybe_set_phone(item, picked, source="olx_html")
                return

        # Otherwise fall back to relay if nothing else surfaces — still better
        # than no contact, and the user can call to reach the seller.
        all_cands = discover_phones(
            page_html, soup=soup, description=desc, allow_relay=True,
        )
        if not all_cands:
            return
        picked = best_phone(all_cands)
        if picked and picked.valid:
            self._maybe_set_phone(item, picked, source="olx_html_relay")

    @staticmethod
    def _maybe_set_phone(item: dict, picked, source: str) -> None:
        """Promote ``picked`` only when strictly better than what's stored."""
        current = item.get("contact_phone")
        if current:
            best = best_phone([current, picked.canonical])
            if not best or best.canonical == current:
                return
        item["contact_phone"]      = picked.canonical
        item["contact_source"]     = source
        item["phone_type"]         = picked.phone_type
        item["contact_confidence"] = picked.confidence

    # ── JSON-LD parser ────────────────────────────────────────────────────────

    def _parse_json_ld(self, soup: BeautifulSoup, item: dict) -> None:
        """
        Extract structured data from <script type="application/ld+json">.

        OLX PT injects a Product/Offer block confirmed to contain:
          - offers.price            → numeric price (avoids text-format ambiguity)
          - offers.priceCurrency    → always "EUR" on OLX PT
          - offers.areaServed.name  → precise city (e.g. "Lisboa", "Cascais")
          - description             → full ad text (backup if testid block absent)
          - sku                     → ad ID confirmation

        Merge rules (HTML-derived values always win; JSON-LD fills gaps only):
          price_raw    — only set when grid-level capture was empty
          location_raw — upgrade only when current value == zone_query (generic)
          description  — only set when _enrich_with_detail found nothing
          external_id  — only set when still missing
        """
        try:
            script_el = soup.find("script", type="application/ld+json")
            if not script_el:
                return

            data = json.loads(script_el.get_text())

            # Handle @graph array or bare list
            if isinstance(data, list):
                data = data[0] if data else {}
            graph = data.get("@graph", [])
            if graph:
                data = graph[0]

            offers = data.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            # ── Price ─────────────────────────────────────────────────────────
            # Only fill when the grid-level price_raw was not captured
            if not item.get("price_raw"):
                price_val = offers.get("price")
                currency  = offers.get("priceCurrency", "EUR")
                if price_val is not None:
                    try:
                        price_num = float(str(price_val).replace(",", "."))
                        # Format as "250 000 EUR" — parse_price handles any spacing
                        item["price_raw"] = f"{int(price_num):,} {currency}".replace(",", " ")
                    except (ValueError, TypeError):
                        pass

            # ── Location — areaServed.name ────────────────────────────────────
            # Replace location_raw only when it is the generic zone fallback;
            # a neighbourhood-level string from the card HTML is already better.
            area_served = offers.get("areaServed") or {}
            if isinstance(area_served, list):
                area_served = area_served[0] if area_served else {}
            city = (area_served.get("name") or "").strip()
            if city:
                current_loc = (item.get("location_raw") or "").strip()
                zone_q      = (item.get("zone_query")   or "").strip()
                if not current_loc or current_loc.lower() == zone_q.lower():
                    item["location_raw"] = city

            # ── Description backup ────────────────────────────────────────────
            if not item.get("description"):
                desc = (data.get("description") or "").strip()
                if desc:
                    item["description"] = desc[:4000]

            # ── SKU → ad ID confirmation ──────────────────────────────────────
            sku = data.get("sku")
            if sku and not item.get("external_id"):
                item["external_id"] = str(sku)

        except (json.JSONDecodeError, AttributeError, TypeError, KeyError):
            pass  # JSON-LD absent or malformed — not critical

    # ── Pagination ────────────────────────────────────────────────────────────

    @staticmethod
    def _has_next_page(soup: BeautifulSoup) -> bool:
        """Return True when a forward pagination link exists."""
        return bool(
            soup.select_one("a[data-cy='pagination-forward']") or
            soup.select_one("a[rel='next']")
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_id_from_url(url: str) -> str | None:
        """Fallback: extract OLX ad ID from URL slug (IDxxxxxx.html)."""
        match = re.search(r"ID([a-zA-Z0-9]+)\.html", url)
        if match:
            return match.group(1)
        parts = url.rstrip("/").split("/")
        return parts[-1][:100] if parts else None

    @staticmethod
    def _extract_typology_hint(title: str) -> str | None:
        """Quick typology extraction from listing title."""
        match = re.search(r"\bT\s*[0-9]\b", title or "", re.IGNORECASE)
        return match.group(0).replace(" ", "").upper() if match else None

    def _debug_dump(self, html: str, zone: str, force: bool = False) -> None:
        """
        Save raw HTML for selector debugging.
        Auto-triggered when zero cards found, or when LOG_LEVEL=DEBUG.
        """
        if not force and os.environ.get("LOG_LEVEL", "").upper() != "DEBUG":
            return
        path = f"/tmp/olx_debug_{zone.lower()}.html"
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            log.info("[olx] HTML dump saved → {path}", path=path)
        except Exception:
            pass

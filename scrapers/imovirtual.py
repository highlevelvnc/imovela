"""
Imovirtual scraper — Portugal's largest dedicated real estate portal.
URL: https://www.imovirtual.com

Owned by OLX Group (Adevinta). Uses server-side rendering — httpx + BeautifulSoup.

URL format (updated 2026-03):
  Old: /comprar/apartamento/{zone}-gmina/  ← 404 (deprecated)
  New: /pt/resultados/comprar/apartamento/{district}/{municipality}

Card selector (updated 2026-03):
  Container : [data-cy='search.listing.organic']
  Cards     : article[data-sentry-component='AdvertCard']  (36–37 per page)
  Title     : [data-cy='listing-item-title']
  URL       : a[href*='/pt/anuncio/']
  Price     : first <span> containing € (without /m²)
  Area      : first <span> containing m² (without €)
  Typology  : first <span> matching T\\d pattern
  Location  : second non-title <p> in card
  Pagination: ?page=N  (base URL + query param)
"""
from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from config.settings import settings
from config.zone_config import get_pw_limit, PW_INTER_FETCH_DELAY_MIN, PW_INTER_FETCH_DELAY_MAX
from scrapers.base import BaseScraper, PlaywrightPhoneRevealer
from utils.email_extractor import extract_first_email
from utils.logger import get_logger
from utils.phone import best_phone, validate_pt_phone
from utils.phone_discovery import discover_phones, discover_whatsapp

log = get_logger(__name__)

BASE_URL      = "https://www.imovirtual.com"
RESULTS_BASE  = f"{BASE_URL}/pt/resultados/comprar/apartamento"   # for-sale (legacy — kept for compat)
RENTAL_BASE   = f"{BASE_URL}/pt/resultados/arrendar/apartamento"  # for-rent (legacy)

# Categories scraped per zone. Each maps to a URL segment under
#   /pt/resultados/{comprar|arrendar}/{category}/{zone-path}
# With delta-crawl primed, only the first full run is expensive;
# subsequent runs hit the delta-stop threshold after 1-2 pages per category.
#
# Terreno + quinta-e-herdade add long-tail volume but have much lower
# turnover — toggle them off during rate-limit recovery if needed.
BUY_CATEGORIES:   tuple[str, ...] = ("apartamento", "moradia", "terreno", "quinta-e-herdade")
RENT_CATEGORIES:  tuple[str, ...] = ("apartamento", "moradia")

# Set to False to disable rental scraping (e.g. during testing or rate-limit recovery)
SCRAPE_RENTALS: bool = True

# Freguesia drill-downs — toggle to skip them on a given run (e.g. hourly
# pulses). Freguesia paths only get executed when this is True AND the zone
# key starts with "Lisboa-".
SCRAPE_FREGUESIAS: bool = True

# Legacy constant kept for any external code that imports it directly.
# scrape_zone now uses get_pw_limit(zone) from zone_config instead.
MAX_PLAYWRIGHT_PHONE_FETCHES = 20

# Imovirtual phone-reveal button — tried in priority order.
# Validated live March 2026 against imovirtual.com/pt/anuncio/...
_PHONE_BTN_SELECTORS = [
    "button[data-cy='phone-number.show-full-number-button']",  # primary — confirmed stable
    "button[data-cy*='phone']",                                 # broader fallback
    "button[aria-label*='Mostrar']",                           # label fallback
    "button[aria-label*='telefone']",
    "[data-cy='contact-phone'] button",
]

# OneTrust cookie consent modal — must be dismissed before clicking
# the phone button (it intercepts pointer events when visible).
_CONSENT_SELECTORS = [
    "#onetrust-accept-btn-handler",
    "button#onetrust-accept-btn-handler",
    "button.onetrust-close-btn-handler",
    "#accept-recommended-btn-handler",
    "button[title='Aceitar Todos os Cookies']",
    "button[title='Accept All Cookies']",
    ".ot-pc-refuse-all-handler",
]

# district/municipality paths for FOR-SALE listings — validated live 2026-03
#
# Lisbon freguesia drill-downs are listed individually: paginating them
# separately sidesteps the municipality-level ~1000-item ceiling and
# recovers long-tail listings that otherwise fall off the end of the
# global Lisboa pagination.
#
# Adjacent-zone entries (Oeiras, Amadora, Loures, Odivelas, Barreiro,
# Montijo, Palmela) broaden geographic coverage for the target client
# without changing parser logic — same slug format, same card structure.
ZONE_PATHS: dict[str, str] = {
    # ── Primary target municipalities (original set) ────────────────────
    "Lisboa":   "lisboa",
    "Cascais":  "lisboa/cascais",
    "Sintra":   "lisboa/sintra",
    "Almada":   "setubal/almada",
    "Seixal":   "setubal/seixal",
    "Sesimbra": "setubal/sesimbra",

    # ── Lisbon freguesia drill-downs ───────────────────────────────────
    # When the "Lisboa" municipality query hits the 1000-item cap, each
    # freguesia below serves as a parallel sweep. Dedup by external_id
    # filters any overlap downstream. Slug paths validated 2026-04 against
    # imovirtual.com/pt/resultados/comprar/apartamento/lisboa/{slug}.
    "Lisboa-Alvalade":          "lisboa/lisboa/alvalade",
    "Lisboa-Areeiro":           "lisboa/lisboa/areeiro",
    "Lisboa-Arroios":           "lisboa/lisboa/arroios",
    "Lisboa-Avenidas-Novas":    "lisboa/lisboa/avenidas-novas",
    "Lisboa-Beato":             "lisboa/lisboa/beato",
    "Lisboa-Belem":             "lisboa/lisboa/belem",
    "Lisboa-Benfica":           "lisboa/lisboa/benfica",
    "Lisboa-Campo-de-Ourique": "lisboa/lisboa/campo-de-ourique",
    "Lisboa-Campolide":         "lisboa/lisboa/campolide",
    "Lisboa-Carnide":           "lisboa/lisboa/carnide",
    "Lisboa-Estrela":           "lisboa/lisboa/estrela",
    "Lisboa-Lumiar":            "lisboa/lisboa/lumiar",
    "Lisboa-Marvila":           "lisboa/lisboa/marvila",
    "Lisboa-Misericordia":      "lisboa/lisboa/misericordia",
    "Lisboa-Olivais":           "lisboa/lisboa/olivais",
    "Lisboa-Parque-das-Nacoes": "lisboa/lisboa/parque-das-nacoes",
    "Lisboa-Penha-de-Franca":   "lisboa/lisboa/penha-de-franca",
    "Lisboa-Santa-Clara":       "lisboa/lisboa/santa-clara",
    "Lisboa-Santa-Maria-Maior": "lisboa/lisboa/santa-maria-maior",
    "Lisboa-Santo-Antonio":     "lisboa/lisboa/santo-antonio",
    "Lisboa-Sao-Domingos-de-Benfica": "lisboa/lisboa/sao-domingos-de-benfica",
    "Lisboa-Sao-Vicente":       "lisboa/lisboa/sao-vicente",

    # ── Adjacent-municipality expansion (Grande Lisboa + Margem Sul) ────
    "Oeiras":   "lisboa/oeiras",
    "Amadora":  "lisboa/amadora",
    "Loures":   "lisboa/loures",
    "Odivelas": "lisboa/odivelas",
    "Vila-Franca-de-Xira": "lisboa/vila-franca-de-xira",
    "Mafra":    "lisboa/mafra",
    "Barreiro": "setubal/barreiro",
    "Montijo":  "setubal/montijo",
    "Palmela":  "setubal/palmela",
    "Setubal":  "setubal/setubal",
    "Moita":    "setubal/moita",
    "Alcochete": "setubal/alcochete",
}

# Rental paths — mirror of ZONE_PATHS; both dicts use identical slugs.
ZONE_RENTAL_PATHS: dict[str, str] = dict(ZONE_PATHS)

# Keep old name for backwards-compat (runner uses ZONE_SLUGS indirectly via scrape_zone only)
ZONE_SLUGS = ZONE_PATHS


class ImovirtualScraper(BaseScraper):
    SOURCE = "imovirtual"

    def __init__(self, max_pages: int = 15, fetch_phone: bool = True):
        """
        Args:
            max_pages:   Max pages to paginate per zone.
            fetch_phone: After httpx detail fetch, use Playwright to reveal the
                         phone number for listings still missing one.
                         Requires playwright + chromium installed.
                         Silently skipped when Playwright is unavailable.
                         Limited to MAX_PLAYWRIGHT_PHONE_FETCHES per zone.
        """
        super().__init__()
        self.max_pages   = max_pages
        self.fetch_phone = fetch_phone
        self._pw_phone_count = 0   # reset per zone in scrape_zone

    def scrape_zone(self, client: httpx.Client, zone: str) -> Iterator[dict]:
        """
        Yield raw listing dicts for the given zone.

        Iterates over every configured category (apartamento / moradia /
        terreno / quinta-e-herdade) for both buy and rent, yielding the
        aggregated set. The Playwright phone-count cap is shared across
        all categories so per-zone overhead stays bounded.
        """
        if not SCRAPE_FREGUESIAS and zone.startswith("Lisboa-"):
            log.debug("[imovirtual] zone={z} skipped — SCRAPE_FREGUESIAS=False", z=zone)
            return

        self._pw_phone_count = 0   # reset per zone (shared across every category)
        pw_limit = get_pw_limit(zone)

        log.debug("[imovirtual] zone={z} pw_limit={lim}", z=zone, lim=pw_limit)

        buy_path = ZONE_PATHS.get(zone, f"lisboa/{zone.lower()}")

        # ── For-sale — iterate categories ──────────────────────────────────
        for category in BUY_CATEGORIES:
            base = f"{BASE_URL}/pt/resultados/comprar/{category}"
            log.debug("[imovirtual] zone={z} buy category={c}", z=zone, c=category)
            yield from self._scrape_results_base(client, zone, base, buy_path, pw_limit)

        # ── For-rent — iterate categories ──────────────────────────────────
        if SCRAPE_RENTALS:
            rental_path = ZONE_RENTAL_PATHS.get(zone)
            if rental_path:
                for category in RENT_CATEGORIES:
                    base = f"{BASE_URL}/pt/resultados/arrendar/{category}"
                    log.debug("[imovirtual] zone={z} rent category={c}", z=zone, c=category)
                    yield from self._scrape_results_base(client, zone, base, rental_path, pw_limit)

    def _scrape_results_base(
        self,
        client: httpx.Client,
        zone: str,
        results_base: str,
        zone_path: str,
        pw_limit: int,
    ) -> Iterator[dict]:
        """
        Inner page-pagination loop for a single results_base + zone_path combo.

        Phase 1 — httpx pass: paginate all pages, fetch detail pages, collect items.
        Phase 2 — batch Playwright reveal: ONE browser session for all pending phones.
        Phase 3 — yield all collected items.

        Using a single browser for the whole batch eliminates the 3-5s startup
        overhead that previously occurred for every individual phone reveal.
        """
        # ── Phase 1: httpx pass — pagination + detail pages ───────────────────
        all_items: list[dict] = []
        page = 1

        while page <= self.max_pages:
            if page == 1:
                url = f"{results_base}/{zone_path}"
            else:
                url = f"{results_base}/{zone_path}?page={page}"

            log.debug("[imovirtual] zone={z} page={p} → {url}", z=zone, p=page, url=url)

            resp = self._get(client, url)
            if not resp:
                break

            soup  = BeautifulSoup(resp.text, "html.parser")
            items = self._parse_listing_page(soup, zone)

            if not items:
                log.debug("[imovirtual] No items on page {p} for zone {z} — stopping", p=page, z=zone)
                break

            # Delta-crawl check BEFORE firing detail fetches — avoids wasting
            # HTTP budget on pages where nearly every item is already in DB.
            if self._page_is_mostly_seen(items):
                log.info(
                    "[imovirtual] zone={z} page={p} delta-stop — pagination halted",
                    z=zone, p=page,
                )
                break

            for item in items:
                # Skip detail fetch for items we already have — saves ~2-6s each
                if item.get("external_id") in self.known_external_ids:
                    all_items.append(item)
                    continue

                # httpx detail fetch — description, contact name, phone, email.
                # Run whenever we're missing either phone OR email — both are
                # extracted in the same request, so it's free to pull both.
                needs_detail = item.get("url") and (
                    not item.get("contact_phone") or not item.get("contact_email")
                )
                if needs_detail:
                    detail = self._fetch_detail(client, item["url"])
                    if detail.get("contact_phone") and not item.get("contact_phone"):
                        item["contact_phone"] = detail["contact_phone"]
                        item["contact_source"] = "imov_html"
                    if detail.get("contact_name") and not item.get("contact_name"):
                        item["contact_name"] = detail["contact_name"]
                    if detail.get("contact_email") and not item.get("contact_email"):
                        item["contact_email"] = detail["contact_email"]
                    if detail.get("description") and not item.get("description"):
                        item["description"] = detail["description"]
                all_items.append(item)

            page += 1

        # ── Phase 2: batch Playwright reveal ──────────────────────────────────
        if self.fetch_phone and self._pw_phone_count < pw_limit:
            remaining = pw_limit - self._pw_phone_count
            pending = [
                item for item in all_items
                if not item.get("contact_phone") and item.get("url")
            ][:remaining]

            if pending:
                log.info(
                    "[imovirtual] zone={z} batch Playwright reveal — {n} URLs (budget {r})",
                    z=zone, n=len(pending), r=remaining,
                )
                revealer = PlaywrightPhoneRevealer(
                    phone_btn_selectors=_PHONE_BTN_SELECTORS,
                    consent_selectors=_CONSENT_SELECTORS,
                    headless=settings.headless_browser,
                    inter_fetch_delay_min=PW_INTER_FETCH_DELAY_MIN,
                    inter_fetch_delay_max=PW_INTER_FETCH_DELAY_MAX,
                )
                phones = revealer.reveal_batch([item["url"] for item in pending])

                for item in pending:
                    phone = phones.get(item["url"])
                    if phone:
                        item["contact_phone"] = phone
                        item["contact_source"] = "imov_playwright"

                revealed = sum(1 for item in pending if item.get("contact_source") == "imov_playwright")
                log.info(
                    "[imovirtual] zone={z} Playwright batch done — {r}/{n} phones revealed",
                    z=zone, r=revealed, n=len(pending),
                )
                self._pw_phone_count += len(pending)

        # ── Phase 3: yield all items ───────────────────────────────────────────
        yield from all_items

    def _fetch_detail(self, client: httpx.Client, url: str) -> dict:
        """
        Fetch a listing detail page and extract contact phone and name.

        Imovirtual detail pages are SSR. Contact info lives in:
          - <a href="tel:..."> links
          - <span data-cy="agency-name"> or similar contact blocks
          - Description text (fallback)

        Returns a dict with any found fields; empty dict on failure.
        """
        try:
            resp = self._get(client, url)
            if not resp:
                return {}

            soup = BeautifulSoup(resp.text, "html.parser")
            result: dict = {}

            # ── Phone — aggressive multi-source discovery ──────────────────
            # discover_phones walks WhatsApp links, microdata, data-* attrs,
            # JSON-LD, inline scripts, meta tags, description. Priority:
            # mobile > landline > relay. We try real-only first; only fall
            # back to including relay numbers when nothing better surfaced.
            wa_candidates = discover_whatsapp(resp.text, soup=soup)
            if wa_candidates:
                result["contact_whatsapp"] = wa_candidates[0]

            phones_real = discover_phones(resp.text, soup=soup, allow_relay=False)
            if phones_real:
                picked = best_phone(phones_real)
                if picked and picked.valid:
                    result["contact_phone"] = picked.canonical
                    result["phone_type"]    = picked.phone_type
                    result["contact_confidence"] = picked.confidence
            if not result.get("contact_phone"):
                phones_any = discover_phones(resp.text, soup=soup, allow_relay=True)
                picked = best_phone(phones_any) if phones_any else None
                if picked and picked.valid:
                    result["contact_phone"] = picked.canonical
                    result["phone_type"]    = picked.phone_type
                    result["contact_confidence"] = picked.confidence

            # ── Contact/agency name ──────────────────────────────────────────
            name_el = (
                soup.select_one("[data-cy='agency-name']") or
                soup.select_one("[data-cy='seller-name']") or
                soup.select_one("[class*='ContactName']") or
                soup.select_one("[class*='contact-name']") or
                soup.select_one("[class*='seller-name']")
            )
            if name_el:
                name = name_el.get_text(strip=True)
                if name and len(name) > 2:
                    result["contact_name"] = name

            # ── Description (full text from detail page) ─────────────────────
            desc_el = (
                soup.select_one("[data-cy='advert-description']") or
                soup.select_one("[class*='description']") or
                soup.select_one("section[class*='Description']")
            )
            if desc_el:
                desc = desc_el.get_text(separator=" ", strip=True)
                if desc and len(desc) > 20:
                    result["description"] = desc[:2000]

            # ── Email — mailto href → JSON-LD → description → obfuscation ────
            # Scans the full HTML (not just description) so JSON blobs in
            # <script> tags and data-attributes on contact blocks are included.
            email = extract_first_email(resp.text)
            if email:
                result["contact_email"] = email

            return result

        except Exception as e:
            log.debug("[imovirtual] _fetch_detail failed for {u}: {e}", u=url, e=e)
            return {}

    def _parse_listing_page(self, soup: BeautifulSoup, zone: str) -> list[dict]:
        # New structure: all cards live inside the search.listing.organic container
        container = soup.select_one("[data-cy='search.listing.organic']")
        if container:
            cards = container.select("article[data-sentry-component='AdvertCard']")
        else:
            # Fallback for potential future markup changes
            cards = soup.select("article[data-sentry-component='AdvertCard']")

        if not cards:
            log.debug("[imovirtual] Zero cards for zone={z}", z=zone)
            return []

        items = []
        for card in cards:
            try:
                item = self._parse_card(card, zone)
                if item:
                    items.append(item)
            except Exception as e:
                log.debug("[imovirtual] Card parse error: {e}", e=e)
        return items

    def _parse_card(self, card, zone: str) -> dict | None:
        # ── URL + external ID ─────────────────────────────────────────────────
        link_el = card.select_one("a[href*='/pt/anuncio/']")
        if not link_el:
            link_el = card.select_one("a[href]")
        if not link_el:
            return None

        href = link_el.get("href", "")
        if not href:
            return None

        url = urljoin(BASE_URL, href)

        # External ID: trailing IDxxxxxx slug in URL path
        m = re.search(r"(ID[a-zA-Z0-9]+)/?$", href.rstrip("/"))
        external_id = m.group(1) if m else href.rstrip("/").split("/")[-1]

        # ── Title ─────────────────────────────────────────────────────────────
        title_el = card.select_one("[data-cy='listing-item-title']")
        title = title_el.get_text(strip=True) if title_el else None

        if not title:
            # Fallback: second <p> (first is usually photo counter)
            ps = [p.get_text(strip=True) for p in card.select("p") if p.get_text(strip=True)]
            title = next((t for t in ps if len(t) > 5 and "€" not in t), None)

        if not title or len(title) < 5:
            return None

        # ── Price — first <span> with € that is NOT a per-m² price ───────────
        price_raw = None
        for span in card.select("span"):
            t = span.get_text(strip=True)
            if "€" in t and "m²" not in t and any(c.isdigit() for c in t):
                price_raw = t
                break

        # ── Area — first <span> with m² and no € ─────────────────────────────
        area_raw = None
        for span in card.select("span"):
            t = span.get_text(strip=True)
            if "m²" in t and "€" not in t and any(c.isdigit() for c in t):
                area_raw = t
                break

        # ── Typology — first <span> matching T0..T9 ──────────────────────────
        typology_raw = None
        for span in card.select("span"):
            t = span.get_text(strip=True)
            if re.match(r"^T\d$", t, re.IGNORECASE):
                typology_raw = t.upper()
                break

        # ── Location — first <p> that is not the title and has location data ─
        # Imovirtual location strings look like:
        #   "Rua X, Carcavelos Centro, Carcavelos e Parede, Cascais, Lisboa"
        location_raw = zone
        for p in card.select("p"):
            t = p.get_text(strip=True)
            if t and t != title and len(t) > 5 and "€" not in t:
                location_raw = t
                break

        # ── Agency/owner classification from span labels ───────────────────
        agency_name = None
        owner_type_raw = ""
        is_owner = False
        spans = [s.get_text(strip=True) for s in card.select("span") if s.get_text(strip=True)]
        # Detect owner-type label first, then find agency name
        for t in spans:
            tl = t.lower().strip()
            if tl == "particular":
                owner_type_raw = "fsbo"
                is_owner = True
            elif tl == "profissional":
                owner_type_raw = "agency"
            elif tl == "promotor":
                owner_type_raw = "developer"
        # Agency name is typically the last span (e.g. "Remax Yes", "ERA Lisboa")
        _skip = {"profissional", "particular", "promotor"}
        for t in reversed(spans):
            if t.lower() not in _skip and len(t) > 3 and "€" not in t and "m²" not in t:
                agency_name = t
                break
        # If classified as particular, clear agency_name (it's the owner, not an agency)
        if owner_type_raw == "fsbo":
            agency_name = None

        return {
            "external_id":   external_id,
            "url":           url,
            "title":         title,
            "price_raw":     price_raw,
            "area_raw":      area_raw,
            "rooms_raw":     typology_raw,
            "typology_raw":  typology_raw,
            "location_raw":  location_raw,
            "agency_name":   agency_name,
            "owner_type_raw": owner_type_raw,
            "is_owner":      is_owner,
            "image_url":     None,
            "zone_query":    zone,
            "description":   None,
            "contact_name":  None,
            "contact_phone": None,
        }

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        """
        Imovirtual pagination uses JS-rendered buttons without href.
        The scrape_zone loop already stops when no items are returned,
        and max_pages provides the hard upper bound.
        This method is kept for interface compatibility but not used by scrape_zone.
        """
        return True

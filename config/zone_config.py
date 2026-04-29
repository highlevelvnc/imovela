"""
Zone-level configuration for the Imovirtual scraper.

Defines per-zone Playwright phone-reveal fetch limits and shared
anti-block settings (user-agent pool, inter-fetch delays).

Rationale for limits:
  - Lisboa has the most listings (150–180/run) and the most agency-only
    ads with no visible phone.  Higher cap = more absolute phones found.
  - Suburban zones (Sintra, Cascais, Almada, Seixal) have fewer listings
    per page-budget; 40 fetches covers ~100% of new private listings.
  - Cap is per zone per scrape run.  With 4–5s per fetch:
      Lisboa  80 × 5s = ~6.7 min extra overhead
      Others  40 × 5s = ~3.3 min each
  - Never set above 100 per zone on a single IP without proxy rotation.
"""
from __future__ import annotations

# ── Per-zone Playwright fetch limits ─────────────────────────────────────────
# Keys must match the zone strings used in ZONE_PATHS (imovirtual.py)
# and in settings.target_zones.
ZONE_PW_LIMITS: dict[str, int] = {
    # ── Primary target municipalities ─────────────────────────────────────
    "Lisboa":   150,  # highest volume — 15 pages × ~25 listings
    "Cascais":  80,
    "Sintra":   80,
    "Almada":   80,
    "Seixal":   80,
    "Sesimbra": 40,   # smaller market

    # ── Lisbon freguesias — smaller budgets (most have <200 listings) ─────
    # Each freguesia gets 30 Playwright fetches. That's enough to cover
    # every new private-seller listing per run without exceeding the per-IP
    # safety envelope.
    "Lisboa-Alvalade":              30,
    "Lisboa-Areeiro":               30,
    "Lisboa-Arroios":               30,
    "Lisboa-Avenidas-Novas":        30,
    "Lisboa-Beato":                 30,
    "Lisboa-Belem":                 30,
    "Lisboa-Benfica":               30,
    "Lisboa-Campo-de-Ourique":      30,
    "Lisboa-Campolide":             30,
    "Lisboa-Carnide":               30,
    "Lisboa-Estrela":               30,
    "Lisboa-Lumiar":                30,
    "Lisboa-Marvila":               30,
    "Lisboa-Misericordia":          30,
    "Lisboa-Olivais":               30,
    "Lisboa-Parque-das-Nacoes":     30,
    "Lisboa-Penha-de-Franca":       30,
    "Lisboa-Santa-Clara":           30,
    "Lisboa-Santa-Maria-Maior":     30,
    "Lisboa-Santo-Antonio":         30,
    "Lisboa-Sao-Domingos-de-Benfica": 30,
    "Lisboa-Sao-Vicente":           30,

    # ── Grande Lisboa adjacents ───────────────────────────────────────────
    "Oeiras":              60,
    "Amadora":             50,
    "Loures":              50,
    "Odivelas":            40,
    "Vila-Franca-de-Xira": 40,
    "Mafra":               30,

    # ── Margem Sul / Setúbal ──────────────────────────────────────────────
    "Barreiro":  40,
    "Montijo":   30,
    "Palmela":   25,
    "Setubal":   50,
    "Moita":     25,
    "Alcochete": 20,
}

# Fallback for zones not listed above
DEFAULT_PW_LIMIT: int = 40

# ── Inter-Playwright-call throttle ────────────────────────────────────────────
# Sleep range (seconds) *between* consecutive Playwright fetches within a zone.
# This is on top of the in-page sleep already inside _async_fetch_phone.
# Keeps outbound request rate to Imovirtual comfortably below rate-limit thresholds.
PW_INTER_FETCH_DELAY_MIN: float = 1.0   # seconds
PW_INTER_FETCH_DELAY_MAX: float = 3.0   # seconds

# ── Rotating user-agent pool ─────────────────────────────────────────────────
# All are real Chrome/macOS or Chrome/Windows strings validated against
# Imovirtual in 2025–2026.  Picked randomly per Playwright browser context.
USER_AGENTS: list[str] = [
    # Chrome 121 macOS (used in initial validation)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome 122 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome 123 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome 121 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome 122 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome 123 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Edge 121 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    # Chrome 120 macOS (older, broadens fingerprint diversity)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def get_pw_limit(zone: str) -> int:
    """Return the Playwright fetch limit for a given zone."""
    return ZONE_PW_LIMITS.get(zone, DEFAULT_PW_LIMIT)


def get_random_user_agent() -> str:
    """Return a random user-agent string from the pool."""
    import random
    return random.choice(USER_AGENTS)

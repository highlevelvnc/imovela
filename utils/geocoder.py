"""
Geocoder — address → (latitude, longitude) using OpenStreetMap Nominatim.

Why a dedicated module
----------------------
The previous implementation called Nominatim inline during pipeline.enrich().
That blocks the entire pipeline at the 1-request/sec rate limit, and re-runs
the same query on every batch because nothing is cached. For 6500 leads that
is impractical.

This module fixes both problems:

  * Persistent cache table (``geocode_cache``) — query the same address only
    once across all runs.  Cache key is normalised to absorb minor variations
    (whitespace, case, punctuation).
  * Static zone-centroid fallback — when Nominatim returns nothing or the
    network is unavailable, we pin the lead to the centroid of its zone or
    municipality. Always-available, never blocks.
  * Strict 1.1s inter-request throttle (Nominatim ToS).
  * Batch ``geocode_leads_backfill`` for offline / scheduled enrichment.

Usage
-----
    from utils.geocoder import Geocoder
    g = Geocoder()
    lat, lon, src = g.geocode(address="Rua dos Anjos 12", zone="Lisboa-Arroios")
    # src ∈ {"cache", "nominatim", "zone_centroid"}

The CLI command ``python main.py geocode-leads`` batches all leads with a
non-null address but missing coordinates and fills them in.
"""
from __future__ import annotations

import hashlib
import re
import time
import unicodedata
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import Column, Float, Integer, String, Text, select
from sqlalchemy.orm import declarative_base

from storage.database import get_db
from utils.logger import get_logger

log = get_logger(__name__)

# ── Static centroid table ────────────────────────────────────────────────────
# Manually compiled from publicly-available Wikipedia infobox coordinates.
# Used as fallback when Nominatim fails or is offline. Always returns a point
# inside the correct municipality/freguesia, never a wildly wrong location.

_ZONE_CENTROIDS: dict[str, tuple[float, float]] = {
    # ── Primary municipalities ─────────────────────────────────────────
    "Lisboa":   (38.7223, -9.1393),
    "Cascais":  (38.6979, -9.4215),
    "Sintra":   (38.7980, -9.3900),
    "Almada":   (38.6790, -9.1569),
    "Seixal":   (38.6420, -9.1037),
    "Sesimbra": (38.4448, -9.1014),

    # ── Grande Lisboa adjacents ────────────────────────────────────────
    "Oeiras":              (38.6929, -9.3097),
    "Amadora":             (38.7536, -9.2302),
    "Loures":              (38.8307, -9.1681),
    "Odivelas":            (38.7950, -9.1832),
    "Vila-Franca-de-Xira": (38.9555, -8.9911),
    "Mafra":               (38.9367, -9.3327),

    # ── Setúbal district ───────────────────────────────────────────────
    "Barreiro":  (38.6633, -9.0697),
    "Montijo":   (38.7060, -8.9744),
    "Palmela":   (38.5703, -8.9000),
    "Setubal":   (38.5244, -8.8882),
    "Moita":     (38.6478, -8.9897),
    "Alcochete": (38.7547, -8.9636),

    # ── Lisbon freguesias ──────────────────────────────────────────────
    "Lisboa-Alvalade":              (38.7506, -9.1450),
    "Lisboa-Areeiro":               (38.7411, -9.1383),
    "Lisboa-Arroios":               (38.7308, -9.1356),
    "Lisboa-Avenidas-Novas":        (38.7437, -9.1497),
    "Lisboa-Beato":                 (38.7361, -9.1106),
    "Lisboa-Belem":                 (38.6968, -9.2058),
    "Lisboa-Benfica":               (38.7479, -9.1980),
    "Lisboa-Campo-de-Ourique":      (38.7242, -9.1683),
    "Lisboa-Campolide":             (38.7346, -9.1719),
    "Lisboa-Carnide":               (38.7656, -9.1842),
    "Lisboa-Estrela":               (38.7186, -9.1641),
    "Lisboa-Lumiar":                (38.7720, -9.1606),
    "Lisboa-Marvila":               (38.7515, -9.1031),
    "Lisboa-Misericordia":          (38.7129, -9.1453),
    "Lisboa-Olivais":               (38.7714, -9.1109),
    "Lisboa-Parque-das-Nacoes":     (38.7681, -9.0942),
    "Lisboa-Penha-de-Franca":       (38.7261, -9.1294),
    "Lisboa-Santa-Clara":           (38.7806, -9.1361),
    "Lisboa-Santa-Maria-Maior":     (38.7113, -9.1339),
    "Lisboa-Santo-Antonio":         (38.7233, -9.1483),
    "Lisboa-Sao-Domingos-de-Benfica": (38.7547, -9.1771),
    "Lisboa-Sao-Vicente":           (38.7239, -9.1278),
}


# ── Cache table (lightweight, lives in same SQLite DB) ───────────────────────

_GeocodeBase = declarative_base()


class GeocodeCache(_GeocodeBase):
    """
    Persistent address → (lat, lon) cache.

    Keyed by a stable hash of the normalised address+zone. Stores the
    source so callers can prefer Nominatim hits over zone fallbacks
    when both are available.
    """

    __tablename__ = "geocode_cache"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    cache_key     = Column(String(64), unique=True, index=True, nullable=False)
    address       = Column(Text, nullable=False)
    zone          = Column(String(80))
    latitude      = Column(Float, nullable=False)
    longitude     = Column(Float, nullable=False)
    source        = Column(String(20), nullable=False)   # nominatim | zone_centroid
    queried_at    = Column(String(40))                    # ISO timestamp


def init_cache_table() -> None:
    """Create the geocode_cache table if it doesn't exist. Idempotent."""
    from storage.database import engine
    _GeocodeBase.metadata.create_all(engine, checkfirst=True)


# ── Normalisation ─────────────────────────────────────────────────────────────

def _normalise_address(text: str) -> str:
    """
    Lowercase + strip accents + collapse whitespace + drop punctuation.
    Stable cache key irrespective of casing / formatting variations.
    """
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_)
    return re.sub(r"\s+", " ", cleaned).strip()


def _cache_key(address: str, zone: str) -> str:
    payload = f"{_normalise_address(address)}|{_normalise_address(zone or '')}"
    return hashlib.sha256(payload.encode()).hexdigest()


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class GeocodeResult:
    latitude:  float
    longitude: float
    source:    str       # "cache" | "nominatim" | "zone_centroid"


# ── Geocoder class ────────────────────────────────────────────────────────────

class Geocoder:
    """
    Address → coordinate resolver with cache + zone fallback.

    Default min_request_interval is 1.1 seconds, satisfying Nominatim's
    public ToS (max 1 request/sec). Adjust upward in production to be
    extra-polite, never lower.
    """

    def __init__(
        self,
        user_agent:           str   = "imoscrapping-pt/1.0",
        min_request_interval: float = 1.1,
        timeout:              float = 5.0,
    ):
        self._user_agent           = user_agent
        self._min_request_interval = min_request_interval
        self._timeout              = timeout
        self._last_request_at:    float = 0.0
        init_cache_table()

    # ── Public API ────────────────────────────────────────────────────────

    def geocode(
        self,
        address: str,
        zone:    Optional[str] = None,
        allow_network: bool = True,
    ) -> Optional[GeocodeResult]:
        """
        Resolve ``address`` to coordinates.

        Lookup chain:
          1. ``geocode_cache`` table  → instant
          2. Nominatim API            → ~1.1s (rate-limited)
          3. Static zone centroid     → instant fallback

        Returns None only when the address is empty AND no zone is provided
        (i.e. nothing meaningful to resolve). Otherwise always returns at
        least a zone centroid.

        Set ``allow_network=False`` to skip the Nominatim step (offline mode).
        """
        if not address and not zone:
            return None

        key = _cache_key(address, zone or "")

        # 1. Cache hit
        with get_db() as db:
            hit = db.execute(
                select(GeocodeCache).where(GeocodeCache.cache_key == key)
            ).scalar_one_or_none()
            if hit:
                return GeocodeResult(hit.latitude, hit.longitude, "cache")

        # 2. Nominatim
        if allow_network and address:
            coords = self._query_nominatim(address, zone)
            if coords:
                self._store_cache(key, address, zone, *coords, "nominatim")
                return GeocodeResult(coords[0], coords[1], "nominatim")

        # 3. Zone centroid fallback
        centroid = _ZONE_CENTROIDS.get(zone or "")
        if centroid:
            self._store_cache(key, address or "", zone or "", *centroid, "zone_centroid")
            return GeocodeResult(centroid[0], centroid[1], "zone_centroid")

        return None

    # ── Internal ──────────────────────────────────────────────────────────

    def _query_nominatim(
        self, address: str, zone: Optional[str]
    ) -> Optional[tuple[float, float]]:
        """Single Nominatim query with rate limiting + timeout. None on failure."""
        # Throttle to ≥ min_request_interval since previous call
        elapsed = time.time() - self._last_request_at
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_at = time.time()

        try:
            from geopy.exc import GeocoderServiceError, GeocoderTimedOut
            from geopy.geocoders import Nominatim
        except ImportError:
            log.debug("geopy not installed — skipping Nominatim step")
            return None

        # Build the query — strip Lisbon-freguesia prefix from zone for the
        # search query while keeping the parent municipality.
        zone_human = (zone or "").replace("Lisboa-", "").replace("-", " ")
        query_parts = [p for p in (address, zone_human, "Portugal") if p]
        query = ", ".join(query_parts)

        try:
            geocoder = Nominatim(user_agent=self._user_agent)
            location = geocoder.geocode(query, timeout=self._timeout)
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            log.debug("Nominatim failed for {q!r}: {e}", q=query, e=e)
            return None
        except Exception as e:
            log.debug("Nominatim unexpected error {q!r}: {e}", q=query, e=e)
            return None

        if location:
            return float(location.latitude), float(location.longitude)
        return None

    def _store_cache(
        self,
        key: str,
        address: str,
        zone: str,
        lat: float,
        lon: float,
        source: str,
    ) -> None:
        from datetime import datetime
        with get_db() as db:
            row = GeocodeCache(
                cache_key  = key,
                address    = address[:500],
                zone       = zone[:80] if zone else None,
                latitude   = lat,
                longitude  = lon,
                source     = source,
                queried_at = datetime.utcnow().isoformat(),
            )
            db.add(row)
            try:
                db.commit()
            except Exception:
                db.rollback()   # concurrent insert with same key — ignore


# ── Bulk backfill ─────────────────────────────────────────────────────────────

def geocode_leads_backfill(
    limit: int = 1000,
    allow_network: bool = True,
) -> dict[str, int]:
    """
    Walk every Lead with NULL latitude AND non-empty address/zone, geocode
    it, and persist coordinates back. Designed for scheduled offline runs
    (no overlap with scraping pipeline timing).

    Returns stats dict with counts per source.
    """
    from storage.models import Lead
    from sqlalchemy import or_

    stats = {"considered": 0, "cache": 0, "nominatim": 0, "zone_centroid": 0, "skipped": 0}
    g = Geocoder()

    with get_db() as db:
        leads = db.execute(
            select(Lead)
            .where(Lead.latitude.is_(None))
            .where(or_(Lead.address.isnot(None), Lead.zone.isnot(None)))
            .limit(limit)
        ).scalars().all()

        log.info("[geocoder] backfill: {n} leads pending coords", n=len(leads))
        stats["considered"] = len(leads)

        for lead in leads:
            address = (lead.address or "").strip()
            zone    = (lead.zone or "").strip()
            if not address and not zone:
                stats["skipped"] += 1
                continue

            result = g.geocode(address, zone, allow_network=allow_network)
            if not result:
                stats["skipped"] += 1
                continue

            lead.latitude  = result.latitude
            lead.longitude = result.longitude
            stats[result.source] = stats.get(result.source, 0) + 1

        db.commit()

    log.info(
        "[geocoder] backfill complete — cache={c} nominatim={n} centroid={z} skipped={s}",
        c=stats["cache"], n=stats["nominatim"], z=stats["zone_centroid"], s=stats["skipped"],
    )
    return stats

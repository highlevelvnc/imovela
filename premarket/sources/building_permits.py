"""
Building-permit signal source — CM Lisboa open data API.

Why this is the strongest pre-market signal (score=85)
-------------------------------------------------------
A homeowner who files a building permit for renovation/alterations/conservation
works is investing capital in a property.  This investment is frequently
followed by a sale within 6-18 months once the works are complete.  The permit
record also gives us the exact street address — far more precise than anything
available from listing portals.

Data source
-----------
CM Lisboa publishes building permits as open data via their Opendatasoft portal:
  https://opendata.cm-lisboa.pt/explore/dataset/lic_obras/

API endpoint (JSON, no authentication required):
  GET https://opendata.cm-lisboa.pt/api/explore/v2.1/catalog/datasets/lic_obras/records
  ?select=morada,tipo_obra,requerente,data_emissao,tipo_licenca,descricao
  &limit=100
  &order_by=data_emissao+desc

Coverage limitations
--------------------
- Only covers the Lisboa municipality (Almada, Cascais, Sintra, Seixal, Sesimbra
  each have their own CM — see _STUB_ZONES below for status).
- 'requerente' (applicant) is sometimes an architect or contractor on behalf of
  the owner; we store it as-is in the 'name' field.
- Permit dates are when issued; works may span months after that.
- No phone / email available from this dataset.

Graceful degradation
--------------------
If the API is unreachable, returns [] without raising.
If a record is malformed, it is logged at DEBUG and skipped.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional

import httpx

from premarket.signals import PremktSignalData
from utils.logger import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# ArcGIS REST endpoint — confirmed working as of 2026-03.
# Source: dados.cm-lisboa.pt → package "alvaras-para-obras-de-edificacao-e-demolicao"
_API_URL = (
    "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services"
    "/AlvarasObras/FeatureServer/0/query"
)
_DEFAULT_LIMIT      = 100    # records per API call
_LOOKBACK_DAYS      = 90     # only import permits issued in the last N days
_REQUEST_TIMEOUT    = 15

# OP_URBANISTICA values that indicate residential renovation (pre-sell signal).
# Confirmed from live dataset: 'Alteração', 'Ampliação', 'Reconstrução',
# 'Obras de Conservação', 'Alteração Durante a Execução da Obra'
# Excluded: 'Construção' (new build, not renovation), 'Demolição'
_RENOVATION_KEYWORDS = (
    "alteração",
    "alteracao",
    "ampliação",
    "ampliacao",
    "reconstrução",
    "reconstrucao",
    "obras de conserv",
    "conservação",
    "conservacao",
)

# Zones covered by other municipal open data portals.
# Marked as "not available" — stubs for future implementation.
_STUB_ZONES = {
    "Cascais":  "https://opendata.cm-cascais.pt/ (API not yet integrated)",
    "Sintra":   "https://dados.gov.pt/pt/datasets/ (check for Sintra permits)",
    "Almada":   "https://www.m-almada.pt/ (no open API found)",
    "Seixal":   "https://www.cm-seixal.pt/ (no open API found)",
    "Sesimbra": "https://www.cm-sesimbra.pt/ (no open API found)",
}

# Zone keyword map for Lisboa sub-area matching
_LISBOA_ZONE_KEYWORDS = [
    "lisboa", "parque das nacoes", "lumiar", "belem", "alvalade",
    "areeiro", "benfica", "olivais", "loures", "odivelas",
    "bairro alto", "chiado", "intendente", "mouraria", "alfama",
    "anjos", "penha de franca", "campo de ourique", "estrela",
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LeadEngine/1.0; real estate research)"
    ),
    "Accept": "application/json",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_renovation(tipo_obra: str, tipo_licenca: str, descricao: str) -> bool:
    """Return True if the permit type indicates residential renovation works."""
    combined = f"{tipo_obra} {tipo_licenca} {descricao}".lower()
    return any(kw in combined for kw in _RENOVATION_KEYWORDS)


def _clean_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    # Strip leading/trailing whitespace and common suffixes
    name = raw.strip()
    # Remove Portuguese legal entity suffixes for cleaner display
    name = re.sub(r"\b(LDA\.?|SA\.?|LLP\.?|LLC\.?|UNIP\.?)\b", "", name, flags=re.IGNORECASE).strip(" ,.")
    return name or None


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            # Strip timezone info for comparison
            return datetime.strptime(date_str[:10], fmt[:8])
        except ValueError:
            pass
    return None


# ── API fetch ─────────────────────────────────────────────────────────────────

def _fetch_cm_lisboa(client: httpx.Client) -> list[dict]:
    """
    Fetch the most recent building permits from the CM Lisboa ArcGIS REST API.

    Endpoint: services.arcgis.com → AlvarasObras FeatureServer layer 0
    Returns a list of normalised dicts with lowercase keys matching the
    downstream _record_to_signal() parser.
    """
    cutoff = datetime.now(tz=None) - timedelta(days=_LOOKBACK_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    params = {
        "where":            f"DATA_ALVARA >= '{cutoff_str}'",
        "outFields":        "MORADA,FREGUESIA,OP_URBANISTICA,ASSUNTO,PROCEDIMENTO,DATA_ALVARA,N_PROCESSO",
        "resultRecordCount": _DEFAULT_LIMIT,
        "orderByFields":    "DATA_ALVARA DESC",
        "f":                "json",
    }

    try:
        resp = client.get(_API_URL, params=params, timeout=_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log.warning(
                "[building_permits] ArcGIS API returned HTTP {c}",
                c=resp.status_code,
            )
            return []
        data = resp.json()

        if "error" in data:
            log.warning(
                "[building_permits] ArcGIS API error: {e}",
                e=data["error"],
            )
            return []

        features = data.get("features", [])
        # Normalise to lowercase-key dicts matching _record_to_signal
        records = []
        for feat in features:
            a = feat.get("attributes", {})
            records.append({
                "morada":       a.get("MORADA", ""),
                "tipo_obra":    a.get("OP_URBANISTICA", ""),
                "tipo_licenca": a.get("PROCEDIMENTO", ""),
                "descricao":    a.get("ASSUNTO", ""),
                "requerente":   a.get("FREGUESIA", ""),   # no owner name in dataset
                "data_emissao": a.get("DATA_ALVARA", ""),
            })

        log.debug(
            "[building_permits] ArcGIS returned {n} records (last {d} days)",
            n=len(records), d=_LOOKBACK_DAYS,
        )
        return records
    except Exception as e:
        log.warning("[building_permits] ArcGIS API error: {e}", e=e)
        return []


# ── Parser ────────────────────────────────────────────────────────────────────

def _record_to_signal(record: dict) -> Optional[PremktSignalData]:
    """
    Convert a raw CM Lisboa permit record to a PremktSignalData.
    Returns None if the permit is not a renovation type or is malformed.
    """
    try:
        morada       = (record.get("morada") or "").strip()
        tipo_obra    = (record.get("tipo_obra") or "").strip()
        tipo_licenca = (record.get("tipo_licenca") or "").strip()
        descricao    = (record.get("descricao") or "").strip()
        requerente   = (record.get("requerente") or "").strip()
        data_emissao = record.get("data_emissao", "")

        if not morada:
            return None

        if not _is_renovation(tipo_obra, tipo_licenca, descricao):
            return None

        signal_text = (
            f"{tipo_licenca or tipo_obra}: {morada}"
            + (f" ({descricao[:80]})" if descricao else "")
        )

        return PremktSignalData(
            signal_type  = "building_permit",
            source       = "cm_lisboa",
            signal_text  = signal_text,
            location_raw = morada,
            zone         = "Lisboa",    # CM Lisboa data is always Lisboa municipality
            name         = _clean_name(requerente),
            url          = None,        # API does not expose a per-record URL
            extra        = {
                "tipo_obra":    tipo_obra,
                "tipo_licenca": tipo_licenca,
                "requerente":   requerente,
                "data_emissao": data_emissao,
            },
        )
    except Exception as e:
        log.debug("[building_permits] Record parse error: {e}", e=e)
        return None


# ── Public interface ──────────────────────────────────────────────────────────

class BuildingPermitsSource:
    """
    Fetch building permit signals from CM Lisboa open data.

    Zones NOT covered: Cascais, Sintra, Almada, Seixal, Sesimbra.
    These municipalities do not have accessible open data APIs at this time.
    They are listed in _STUB_ZONES for future integration.

    Usage:
        source  = BuildingPermitsSource()
        signals = source.fetch(zones=["Lisboa"])
    """

    def fetch(self, zones: list[str] | None = None) -> list[PremktSignalData]:
        """
        Fetch building permit signals.

        zones parameter: if provided and "Lisboa" is not in the list, returns []
        immediately (CM Lisboa permits are irrelevant for other zones).
        """
        if zones and "Lisboa" not in zones:
            log.debug(
                "[building_permits] Lisboa not in zone list {z} — skipping",
                z=zones,
            )
            return []

        # Log informational stubs for other zones
        if zones:
            for zone in zones:
                if zone != "Lisboa" and zone in _STUB_ZONES:
                    log.debug(
                        "[building_permits] {z}: {note}",
                        z=zone, note=_STUB_ZONES[zone],
                    )

        signals: list[PremktSignalData] = []

        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            records = _fetch_cm_lisboa(client)

        for record in records:
            sig = _record_to_signal(record)
            if sig:
                signals.append(sig)

        log.info(
            "[building_permits] {n} renovation permits found (last {d} days)",
            n=len(signals), d=_LOOKBACK_DAYS,
        )
        return signals

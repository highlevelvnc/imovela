"""
Lead similarity engine — "5 leads similares a este".

Why
---
When the operator opens a lead, they often want to see comparable
listings: same zone + similar typology, ±10% price, similar amenities.
Doing this manually means re-typing filters every time. With a similarity
engine, the system suggests them inline.

Approach
--------
TF-IDF on a structured "feature blob" per lead:

    "<typology> <zone> <amenity_tags> <title>"

The blob privileges the structured fields by repeating them, so a query
lead at T2/Lisboa/piscina+garagem matches another T2 in Lisboa with a
pool over a T3 in Cascais even if the descriptions read similarly.

Then we apply a price filter (±15% by default) and rank by cosine
similarity. Result: top-5 visually-similar comparable listings.

Caching
-------
Vectoriser + matrix are cached at module level. ``invalidate()`` lets
the dashboard refresh the cache after big batches (CSV import, run).

Public API
----------
similar_to(lead_id: int, top_n: int = 5, price_window: float = 0.15) -> list[Lead]
invalidate() -> None
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

import numpy as np
from sqlalchemy import select

from storage.database import get_db
from storage.models import Lead
from utils.logger import get_logger

log = get_logger(__name__)


# ── Module-level cache ───────────────────────────────────────────────────────

@dataclass
class _Index:
    ids:     list[int]
    matrix:  object         # scipy.sparse.csr_matrix
    vec:     object         # TfidfVectorizer
    prices:  np.ndarray     # parallel prices array (NaN where missing)


_cache: Optional[_Index] = None
_lock = threading.Lock()


def _build_blob(lead: Lead) -> str:
    typology = (lead.typology or "").strip()
    zone     = (lead.zone or "").strip()
    tags     = (lead.amenity_tags or "").replace(",", " ")
    title    = (lead.title or "")[:200]

    parts = []
    if typology:
        # Repeat structured fields so they dominate the TF-IDF score
        parts += [typology] * 4
    if zone:
        parts += [zone] * 4
    if tags:
        parts += [tags] * 3
    if title:
        parts.append(title)
    return " ".join(parts).lower()


def _build_index() -> Optional[_Index]:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError:
        log.warning("[similarity] scikit-learn not installed")
        return None

    with get_db() as db:
        rows = db.execute(
            select(Lead.id, Lead.typology, Lead.zone, Lead.amenity_tags,
                   Lead.title, Lead.price)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
        ).all()
    if not rows:
        return None

    ids:    list[int] = []
    blobs:  list[str] = []
    prices: list[float] = []
    for r in rows:
        # Re-create a tiny "lead-like" object with just the cols we used
        ids.append(r.id)
        blobs.append(_build_blob_from_row(r))
        prices.append(float(r.price) if r.price else float("nan"))

    vec = TfidfVectorizer(
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.85,
        sublinear_tf=True,
    )
    matrix = vec.fit_transform(blobs)

    log.info("[similarity] index built — {n} leads, vocab={v}",
             n=len(ids), v=len(vec.vocabulary_))
    return _Index(
        ids=ids, matrix=matrix, vec=vec, prices=np.array(prices, dtype=float),
    )


def _build_blob_from_row(r) -> str:
    """Same as _build_blob but for sqlalchemy row tuples."""
    typology = (r.typology or "").strip()
    zone     = (r.zone or "").strip()
    tags     = (r.amenity_tags or "").replace(",", " ")
    title    = (r.title or "")[:200]
    parts = []
    if typology: parts += [typology] * 4
    if zone:     parts += [zone] * 4
    if tags:     parts += [tags] * 3
    if title:    parts.append(title)
    return " ".join(parts).lower()


def _ensure_index() -> Optional[_Index]:
    global _cache
    with _lock:
        if _cache is None:
            _cache = _build_index()
        return _cache


def invalidate() -> None:
    """Drop the cached index — call after big inserts / updates."""
    global _cache
    with _lock:
        _cache = None


# ── Public lookup ────────────────────────────────────────────────────────────

def similar_to(
    lead_id:      int,
    top_n:        int   = 5,
    price_window: float = 0.15,
) -> list[Lead]:
    """
    Return the ``top_n`` most similar leads to ``lead_id``, filtered by
    a ±``price_window`` price band when both prices are known.
    """
    idx = _ensure_index()
    if not idx:
        return []
    try:
        from sklearn.metrics.pairwise import cosine_similarity
    except ImportError:
        return []

    try:
        anchor_pos = idx.ids.index(lead_id)
    except ValueError:
        return []

    sims = cosine_similarity(idx.matrix[anchor_pos], idx.matrix).ravel()

    # Price-band filter — NaN-safe
    anchor_price = idx.prices[anchor_pos]
    if not np.isnan(anchor_price) and price_window:
        lo = anchor_price * (1 - price_window)
        hi = anchor_price * (1 + price_window)
        # Keep candidates with NaN prices (don't penalise missing data)
        in_band = np.isnan(idx.prices) | ((idx.prices >= lo) & (idx.prices <= hi))
        sims = np.where(in_band, sims, -1.0)

    sims[anchor_pos] = -1.0   # never recommend the lead itself

    # Top-N indices (descending)
    top_idx = np.argpartition(-sims, range(min(top_n, len(sims))))[:top_n]
    top_idx = top_idx[np.argsort(-sims[top_idx])]
    top_ids = [idx.ids[i] for i in top_idx if sims[i] > 0]

    if not top_ids:
        return []

    with get_db() as db:
        rows = db.query(Lead).filter(Lead.id.in_(top_ids)).all()
    order = {lid: i for i, lid in enumerate(top_ids)}
    rows.sort(key=lambda r: order.get(r.id, 999))
    return rows

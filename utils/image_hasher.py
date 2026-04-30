"""
Perceptual image hashing — collapses leads that show the same physical
property across different portals.

Why
---
The fingerprint deduplicator merges listings that share
``typology + zone + price ± 1k + area ± 5m² + title-prefix``. That covers
the ~70% case but breaks down on:

  * One portal lists at €245 000, the other at €250 000
  * Title differences ("T2 Belém" vs "Apartamento T2 Belém vista mar")
  * Missing area on one of the two listings

The remaining ~30% of duplicates leave Nuno calling the same property
twice. Photos are the one signal that's identical across portals: the
seller uploads the same photo set to OLX, Imovirtual and Idealista.

Approach
--------
Compute a perceptual hash (pHash) of each lead's first listing image.
pHash is robust to JPEG re-compression, mild crops, and small overlays
(watermarks, badges) — the typical transformations portals apply on
upload. Comparing two pHashes returns the Hamming distance: ≤5 bits
of difference is reliable evidence of the same image.

Storage
-------
Adds a ``image_phash`` VARCHAR(20) column to ``leads``. Hashes are
stored as their 16-character hex string for portability.

Pipeline integration
--------------------
``photo_dedup_sweep()`` walks every lead that has an image_phash, builds
candidate pairs by binning hashes into 4-bit prefixes (cuts the n²
comparison to ~n × bin_size), and merges pairs whose Hamming distance
is ≤ ``MAX_PHASH_DISTANCE`` (default 5).

Public API
----------
hash_image_url(url)             -> str | None  (16-char hex)
backfill_image_hashes(limit)    -> dict stats
photo_dedup_sweep(threshold)    -> dict stats
"""
from __future__ import annotations

import io
from typing import Optional

import httpx

from utils.logger import get_logger

log = get_logger(__name__)

# Distance threshold — any pair with ≤ this many differing bits is
# considered the same photo. Empirically: 0-3 = identical, 4-5 = very
# similar (re-compressed / re-cropped), 6+ starts admitting false positives.
MAX_PHASH_DISTANCE: int = 5

# Connection settings for the image-download leg
_IMG_TIMEOUT = 12.0
_IMG_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def hash_image_url(url: str) -> Optional[str]:
    """
    Download ``url`` and return its 16-char hex pHash, or None on failure.
    """
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        import imagehash
        from PIL import Image
    except ImportError:
        log.debug("[image_hasher] imagehash/Pillow not installed")
        return None

    try:
        with httpx.Client(
            timeout=_IMG_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _IMG_USER_AGENT, "Accept": "image/*"},
        ) as client:
            r = client.get(url)
            if r.status_code != 200 or not r.content:
                return None
            img = Image.open(io.BytesIO(r.content))
            img = img.convert("RGB")
            return str(imagehash.phash(img))
    except Exception as e:
        log.debug("[image_hasher] failed for {u}: {e}", u=url[-60:], e=e)
        return None


def _hex_distance(h1: str, h2: str) -> int:
    """Hamming distance between two 16-char hex pHash strings."""
    if not h1 or not h2 or len(h1) != len(h2):
        return 999
    n1 = int(h1, 16)
    n2 = int(h2, 16)
    return bin(n1 ^ n2).count("1")


# ── Bulk operations ──────────────────────────────────────────────────────────

def backfill_image_hashes(limit: int = 500) -> dict:
    """
    Walk every lead with image_url but no image_phash, hash the image,
    persist the result. Idempotent.

    Concurrency-controlled via ``parallel_fetch`` so 500 leads complete
    in ~2-3 minutes instead of ~10.
    """
    from sqlalchemy import select
    from storage.database import get_db
    from storage.models import Lead

    stats = {"considered": 0, "hashed": 0, "skipped": 0, "errors": 0}

    with get_db() as db:
        leads = db.execute(
            select(Lead.id, Lead.image_url)
            .where(Lead.image_phash.is_(None))
            .where(Lead.image_url.isnot(None))
            .where(Lead.image_url != "")
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
            .limit(limit)
        ).all()

    stats["considered"] = len(leads)
    if not leads:
        return stats

    # Process in chunks of 12 concurrent downloads
    import asyncio

    async def _hash_one(client, lead_id: int, url: str) -> tuple[int, str | None]:
        try:
            r = await client.get(url)
            if r.status_code != 200 or not r.content:
                return lead_id, None
            import imagehash
            from PIL import Image
            img = Image.open(io.BytesIO(r.content)).convert("RGB")
            return lead_id, str(imagehash.phash(img))
        except Exception:
            return lead_id, None

    async def _run() -> list[tuple[int, str | None]]:
        sema = asyncio.Semaphore(12)
        results: list[tuple[int, str | None]] = []

        async with httpx.AsyncClient(
            timeout=_IMG_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _IMG_USER_AGENT, "Accept": "image/*"},
        ) as client:

            async def _wrap(lid: int, u: str):
                async with sema:
                    res = await _hash_one(client, lid, u)
                    results.append(res)

            await asyncio.gather(*(_wrap(lid, u) for lid, u in leads))
        return results

    try:
        results = asyncio.run(_run())
    except RuntimeError:
        results = [(lid, hash_image_url(u)) for lid, u in leads]

    # Persist results
    with get_db() as db:
        for lid, h in results:
            if h is None:
                stats["errors"] += 1
                continue
            db.query(Lead).filter(Lead.id == lid).update({"image_phash": h})
            stats["hashed"] += 1
        db.commit()

    log.info(
        "[image_hasher] hashed={h} errors={e} from {n} leads",
        h=stats["hashed"], e=stats["errors"], n=len(leads),
    )
    return stats


def photo_dedup_sweep(threshold: int = MAX_PHASH_DISTANCE) -> dict:
    """
    Find leads with images close enough to be the same property and merge
    the contact data of the duplicates into a single canonical lead.

    Strategy:
      1. Bin every (id, phash) by the first 4 hex chars (= 16 bit prefix).
         Same-photo pairs share at least the prefix; this prunes n² to
         ~n × avg_bin_size, which is small.
      2. Inside each bin, compute pairwise Hamming distance.
      3. Pairs with distance ≤ threshold AND distinct discovery_source
         are merged — the older lead wins, the newer becomes its source.
    """
    from collections import defaultdict
    from sqlalchemy import select
    from storage.database import get_db
    from storage.models import Lead

    stats = {
        "considered":      0,
        "candidate_pairs": 0,
        "merged":          0,
        "skipped_chain":   0,
    }

    with get_db() as db:
        rows = db.execute(
            select(Lead.id, Lead.image_phash, Lead.discovery_source,
                   Lead.first_seen_at, Lead.archived)
            .where(Lead.image_phash.isnot(None))
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo  == False)              # noqa: E712
        ).all()

    stats["considered"] = len(rows)
    if not rows:
        return stats

    # Bin by first 4 hex chars
    bins: dict[str, list] = defaultdict(list)
    for r in rows:
        bins[r.image_phash[:4]].append(r)

    # For each bin, find close pairs
    pairs: list[tuple[int, int]] = []
    for entries in bins.values():
        if len(entries) < 2:
            continue
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                a, b = entries[i], entries[j]
                if a.discovery_source == b.discovery_source:
                    continue                # same portal — already deduped
                if _hex_distance(a.image_phash, b.image_phash) > threshold:
                    continue
                pairs.append((a.id, b.id))

    stats["candidate_pairs"] = len(pairs)
    if not pairs:
        return stats

    # Merge: older lead wins, newer is archived + sources merged in
    from json import loads as _loads, dumps as _dumps
    with get_db() as db:
        for a_id, b_id in pairs:
            try:
                a = db.query(Lead).get(a_id)
                b = db.query(Lead).get(b_id)
                if not a or not b:
                    continue
                if a.archived or b.archived:
                    stats["skipped_chain"] += 1
                    continue
                # Determine canonical (older) and duplicate (newer)
                canonical, duplicate = (a, b) if a.first_seen_at <= b.first_seen_at else (b, a)
                # Merge sources_json
                try:
                    can_sources = _loads(canonical.sources_json or "[]")
                    dup_sources = _loads(duplicate.sources_json or "[]")
                except Exception:
                    can_sources, dup_sources = [], []
                seen = {(s.get("source"), s.get("url")) for s in can_sources}
                for s in dup_sources:
                    key = (s.get("source"), s.get("url"))
                    if key not in seen:
                        can_sources.append(s)
                        seen.add(key)
                canonical.sources_json = _dumps(can_sources, ensure_ascii=False)
                # Promote richer contact data when canonical is missing it
                if not canonical.contact_phone and duplicate.contact_phone:
                    canonical.contact_phone = duplicate.contact_phone
                    canonical.phone_type = duplicate.phone_type
                if not canonical.contact_email and duplicate.contact_email:
                    canonical.contact_email = duplicate.contact_email
                if not canonical.contact_whatsapp and duplicate.contact_whatsapp:
                    canonical.contact_whatsapp = duplicate.contact_whatsapp
                if not canonical.contact_name and duplicate.contact_name:
                    canonical.contact_name = duplicate.contact_name
                # Take the higher score
                if (duplicate.score or 0) > (canonical.score or 0):
                    canonical.score       = duplicate.score
                    canonical.score_label = duplicate.score_label
                duplicate.archived = True
                duplicate.crm_stage = "merged"
                stats["merged"] += 1
            except Exception as e:
                log.debug("[image_hasher] merge {a}↔{b}: {e}", a=a_id, b=b_id, e=e)
        db.commit()

    log.info(
        "[image_hasher] sweep done — pairs={p} merged={m} considered={c}",
        p=stats["candidate_pairs"], m=stats["merged"], c=stats["considered"],
    )
    return stats

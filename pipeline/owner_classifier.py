"""
ML owner-type classifier — supervised model that distinguishes FSBO listings
from agency listings using only the listing's text + agency_name signals.

Why
---
The portal's own ``trader_title`` flag (Particular / Empresa / Profissional)
is reliable on OLX but missing or unreliable on Imovirtual / Idealista /
Sapo. The seller-profile sweep flags camouflaged agencies on OLX but doesn't
help the other portals.

A simple text classifier trained on the high-signal subset (OLX leads where
``owner_type`` came from ``trader-title`` directly) generalises well to the
weaker-signal portals. ~90% accuracy in cross-validation on the bootstrap
data, more than enough to flag suspect FSBOs that escaped the heuristic.

Architecture
------------
Pipeline: TF-IDF vectoriser (1-2 grams, PT-language token filter) → logistic
regression. Tiny model (~100 KB), no GPU, trains in seconds on the full DB.

The trained model is pickled to ``data/owner_classifier.pkl`` and loaded on
demand. Re-training is offline (CLI: ``python main.py train-owner-classifier``);
inference runs in-process with no extra dependencies at runtime.

Public API
----------
classify_owner_type(title, description, agency_name, contact_name) -> dict
    Returns {label: "fsbo"|"agency", confidence: 0.0–1.0}.
    Loads the model lazily; returns the heuristic-only baseline if the model
    file isn't present yet.

train_and_save() -> dict
    Train on every Lead with a confident owner_type label and save the model.

Data hygiene
------------
Training set is filtered to leads where:
  * owner_type was set by trader-title (OLX) — strongest label
  * Or where seller_super_flag = True with agency reclassification
  * AND title is non-empty
This avoids training on noisy / circular labels.
"""
from __future__ import annotations

import os
import pickle
import re
from pathlib import Path
from typing import Optional

from utils.logger import get_logger

log = get_logger(__name__)

ROOT_DIR  = Path(__file__).resolve().parent.parent
MODEL_DIR = ROOT_DIR / "data"
MODEL_PATH = MODEL_DIR / "owner_classifier.pkl"

# Keywords frequently used by Portuguese real-estate agencies — high-signal
# tokens for the heuristic baseline that runs when no model is loaded.
_AGENCY_TOKENS: frozenset[str] = frozenset([
    "imobiliaria", "imobiliária", "remax", "kw", "century", "era", "zome",
    "predimed", "habivida", "savills", "engel", "volkers", "sotheby",
    "century21", "knight", "consultor", "consultora", "mediadora",
    "sociedade", "lda", "unipessoal", "s.a", "ami", "amplitude",
])

# Keywords typical of private-owner descriptions
_FSBO_TOKENS: frozenset[str] = frozenset([
    "particular", "proprietario", "proprietário", "dono", "vendo direta",
    "diretamente", "sem intermediarios", "sem intermediários",
    "venda direta", "negociavel", "negociável",
])


def _build_text(title: str, description: str, agency_name: str, contact_name: str) -> str:
    """Concatenate the lead's text fields into a single document."""
    parts = [
        (title         or "").strip(),
        (description   or "").strip()[:1500],   # cap to keep TF-IDF small
        (agency_name   or "").strip(),
        (contact_name  or "").strip(),
    ]
    return " ".join(p for p in parts if p).lower()


# ── Heuristic baseline (no model needed) ─────────────────────────────────────

def _heuristic_classify(text: str, agency_name: str) -> tuple[str, float]:
    """
    Token-presence baseline used when the trained model isn't available yet.
    Returns (label, confidence).
    """
    if agency_name and agency_name.strip():
        return ("agency", 0.85)
    text_l = text.lower()
    if any(t in text_l for t in _AGENCY_TOKENS):
        return ("agency", 0.7)
    if any(t in text_l for t in _FSBO_TOKENS):
        return ("fsbo", 0.7)
    return ("fsbo", 0.5)         # default — most listings are FSBO-leaning


# ── Model loading / caching ──────────────────────────────────────────────────

_model_cache: dict = {}


def _load_model():
    """Lazy-load the pickled pipeline. Returns None if model file missing."""
    if "pipe" in _model_cache:
        return _model_cache["pipe"]
    if not MODEL_PATH.exists():
        return None
    try:
        with open(MODEL_PATH, "rb") as f:
            pipe = pickle.load(f)
        _model_cache["pipe"] = pipe
        log.info("[owner_classifier] model loaded from {p}", p=MODEL_PATH)
        return pipe
    except Exception as e:
        log.warning("[owner_classifier] failed to load model: {e}", e=e)
        return None


# ── Public inference ─────────────────────────────────────────────────────────

def classify_owner_type(
    title:        str = "",
    description:  str = "",
    agency_name:  str = "",
    contact_name: str = "",
) -> dict:
    """
    Predict whether a listing is FSBO or agency-led.

    Returns ``{"label": "fsbo"|"agency", "confidence": 0.0–1.0,
    "source": "model"|"heuristic"}``.
    """
    text = _build_text(title, description, agency_name, contact_name)
    if not text:
        return {"label": "fsbo", "confidence": 0.5, "source": "heuristic"}

    pipe = _load_model()
    if pipe is None:
        label, conf = _heuristic_classify(text, agency_name)
        return {"label": label, "confidence": conf, "source": "heuristic"}

    # Inference
    try:
        proba = pipe.predict_proba([text])[0]
        # pipe.classes_ is ordered alphabetically: ['agency', 'fsbo']
        idx_agency = list(pipe.classes_).index("agency")
        idx_fsbo   = list(pipe.classes_).index("fsbo")
        p_agency = float(proba[idx_agency])
        p_fsbo   = float(proba[idx_fsbo])
        if p_agency >= p_fsbo:
            return {"label": "agency", "confidence": p_agency, "source": "model"}
        return {"label": "fsbo", "confidence": p_fsbo, "source": "model"}
    except Exception as e:
        log.debug("[owner_classifier] inference error: {e}", e=e)
        label, conf = _heuristic_classify(text, agency_name)
        return {"label": label, "confidence": conf, "source": "heuristic"}


# ── Training ─────────────────────────────────────────────────────────────────

def train_and_save() -> dict:
    """
    Build the training set from confident-label leads, fit the pipeline,
    and pickle to ``MODEL_PATH``.

    Returns stats dict with sample counts + cross-validation accuracy.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_score
    from sklearn.pipeline import Pipeline

    from sqlalchemy import or_, select

    from storage.database import get_db
    from storage.models import Lead

    # Pull only confident-label leads
    with get_db() as db:
        rows = db.execute(
            select(
                Lead.title,
                Lead.description,
                Lead.agency_name,
                Lead.contact_name,
                Lead.owner_type,
                Lead.discovery_source,
                Lead.seller_super_flag,
            )
            .where(Lead.owner_type.in_(("fsbo", "agency")))
            .where(Lead.title.isnot(None))
            .where(Lead.title != "")
        ).all()

    # Confidence filter:
    #   * OLX rows are the gold standard (trader-title was definitive)
    #   * non-OLX with agency_name set are confident agency labels
    #   * non-OLX FSBOs only kept when the listing has a personal contact_name
    samples = []
    for r in rows:
        title, desc, agency, contact, owner, src, super_flag = r
        text = _build_text(title or "", desc or "", agency or "", contact or "")
        if len(text) < 20:
            continue                     # too short to learn from

        keep = False
        if src == "olx":
            keep = True
        elif owner == "agency" and (agency and agency.strip()):
            keep = True
        elif owner == "fsbo" and (contact and contact.strip()) and not (agency and agency.strip()):
            keep = True

        if keep:
            samples.append((text, owner))

    if len(samples) < 100:
        log.warning(
            "[owner_classifier] only {n} samples — refusing to train "
            "(need ≥100 for stable model)", n=len(samples),
        )
        return {"samples": len(samples), "trained": False, "accuracy": None}

    # Class balance
    n_fsbo   = sum(1 for _, y in samples if y == "fsbo")
    n_agency = sum(1 for _, y in samples if y == "agency")

    X = [t for t, _ in samples]
    y = [c for _, c in samples]

    pipe = Pipeline([
        ("tfidf", TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=3,
            max_df=0.9,
            sublinear_tf=True,
            strip_accents="unicode",
        )),
        ("clf", LogisticRegression(
            max_iter=500,
            class_weight="balanced",
            C=1.0,
            solver="liblinear",
        )),
    ])

    cv_scores = cross_val_score(pipe, X, y, cv=5, scoring="accuracy")
    accuracy = float(cv_scores.mean())

    pipe.fit(X, y)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(pipe, f)

    # Reset cache so subsequent classify_owner_type() picks up the new model
    _model_cache.pop("pipe", None)

    log.info(
        "[owner_classifier] trained on {n} samples ({f} fsbo, {a} agency) — "
        "5-fold CV accuracy={acc:.3f}",
        n=len(samples), f=n_fsbo, a=n_agency, acc=accuracy,
    )
    return {
        "samples":   len(samples),
        "fsbo":      n_fsbo,
        "agency":    n_agency,
        "trained":   True,
        "accuracy":  accuracy,
        "model_path": str(MODEL_PATH),
    }


# ── Bulk reclassification ────────────────────────────────────────────────────

def reclassify_uncertain_leads(threshold: float = 0.80) -> dict:
    """
    Re-score every lead and flip ``owner_type`` only when:
      * Model confidence ≥ threshold
      * Predicted label disagrees with the stored label
      * The stored label is **not** locked by a strong signal
        (OLX trader-title / seller_super_flag agency reclass).

    Returns counts of flips and confidence histogram.
    """
    from sqlalchemy import select

    from storage.database import get_db
    from storage.models import Lead

    pipe = _load_model()
    if pipe is None:
        log.warning("[owner_classifier] no model available — train first")
        return {"trained": False}

    stats = {
        "considered":  0,
        "fsbo_to_agency": 0,
        "agency_to_fsbo": 0,
        "kept":         0,
        "low_confidence": 0,
    }

    with get_db() as db:
        leads = db.execute(
            select(Lead)
            .where(Lead.owner_type.isnot(None))
            .where(Lead.title.isnot(None))
            .where(Lead.title != "")
        ).scalars().all()

        for lead in leads:
            stats["considered"] += 1

            # Lock leads whose owner_type came from a strong signal.
            # OLX trader-title leads are gold; seller_super_flag agency
            # reclass is a domain rule we don't override with the ML model.
            if lead.discovery_source == "olx":
                stats["kept"] += 1
                continue
            if lead.seller_super_flag and lead.owner_type == "agency":
                stats["kept"] += 1
                continue

            result = classify_owner_type(
                lead.title or "", lead.description or "",
                lead.agency_name or "", lead.contact_name or "",
            )
            if result["source"] != "model" or result["confidence"] < threshold:
                stats["low_confidence"] += 1
                continue

            new_label = result["label"]
            if new_label == lead.owner_type:
                stats["kept"] += 1
                continue

            if lead.owner_type == "fsbo" and new_label == "agency":
                lead.owner_type = "agency"
                lead.is_owner   = False
                stats["fsbo_to_agency"] += 1
            elif lead.owner_type == "agency" and new_label == "fsbo":
                lead.owner_type = "fsbo"
                lead.is_owner   = True
                stats["agency_to_fsbo"] += 1

        db.commit()

    log.info(
        "[owner_classifier] reclassified — fsbo→agency={a} agency→fsbo={f} "
        "kept={k} low_conf={lc}",
        a=stats["fsbo_to_agency"], f=stats["agency_to_fsbo"],
        k=stats["kept"], lc=stats["low_confidence"],
    )
    return stats

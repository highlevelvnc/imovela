"""
pipeline/backfill_phone.py — Retroactive phone enrichment job.

Processes leads that were scraped without a phone number and attempts to
recover contact information using three progressive strategies:

  Stage 1 — Text extraction  (free, instant)
    Scan the lead's description and title with the improved PT phone regex.
    Catches numbers buried in free-text descriptions that the scraper missed.
    Tags contact_source as "backfill_text".

  Stage 2 — Cross-match  (free, instant)
    Re-runs the existing CrossMatcher against the live DB.  After Stage 1
    some previously contactless leads now have phones — they become donors for
    further phoneless leads.  CrossMatcher already applies a 4-dimension
    similarity model (price + typology + area + title) with MATCH_THRESHOLD=0.72
    and never overwrites existing data.
    contact_source is set by CrossMatcher as "cross_portal:<source>".

  Stage 3 — Playwright reveal  (browser-based, slow)
    For leads still missing phone that have a listing URL, run
    PlaywrightPhoneRevealer grouped by discovery_source — one browser session
    per source, one batch per run.  Respects a per-source Playwright budget.
    Tags contact_source as "backfill_playwright_{source}".

After every phone save the lead's scored_at is cleared so the next
`score` run re-evaluates with the new contact data.

Usage:
    python main.py backfill-phones
    python main.py backfill-phones --limit 300 --pw-limit 50
    python main.py backfill-phones --sources imovirtual,olx --skip-pw
    python main.py backfill-phones --dry-run
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from utils.logger import get_logger

log = get_logger(__name__)


# ── Playwright selectors per source ───────────────────────────────────────────

_IMOVIRTUAL_PHONE_BTNS = [
    "button[data-cy='phone-number.show-full-number-button']",
    "button[data-cy*='phone']",
    "button[aria-label*='Mostrar']",
    "button[aria-label*='telefone']",
    "[data-cy='contact-phone'] button",
]
_OLX_PHONE_BTNS = [
    "button[data-testid='show-phone']",
    "a[data-cy='call-ad-button']",
    "button[data-testid='show-phone-button']",
    "button[data-testid='phone-show-number']",
    "a[data-testid='show-phone']",
    "button[class*='show-phone']",
    "a[class*='show-phone']",
]
_IDEALISTA_PHONE_BTNS = [
    "button[class*='phone']",
    "a[href^='tel:']",
    "span[class*='phone']",
    ".phone-btn",
    "[data-cy*='phone']",
]
_CUSTOJUSTO_PHONE_BTNS = [
    # MUI button with "Ver número" / "Ligar" text — matched by text fallback
    "a[href^='tel:']",
    "button[class*='MuiButton'][class*='contained']",
    "button[class*='phone']",
]
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
_SOURCE_PHONE_BTNS: dict[str, list[str]] = {
    "imovirtual": _IMOVIRTUAL_PHONE_BTNS,
    "olx":        _OLX_PHONE_BTNS,
    "idealista":  _IDEALISTA_PHONE_BTNS,
    "custojusto": _CUSTOJUSTO_PHONE_BTNS,
}
_DEFAULT_PHONE_BTNS = _OLX_PHONE_BTNS


# ── URL extractor ─────────────────────────────────────────────────────────────

def _lead_url(lead) -> Optional[str]:
    """
    Extract the primary listing URL from a Lead's sources_json field.
    The sources list stores {"source": ..., "url": ..., "seen_at": ...} dicts.
    Returns the first non-empty URL, or None.
    """
    try:
        for src in lead.sources:                  # uses Lead.sources @property
            url = src.get("url", "").strip()
            if url and url.startswith("http"):
                return url
    except Exception:
        pass
    return None


# ── Result container ──────────────────────────────────────────────────────────

@dataclass
class BackfillStats:
    """Summary of one backfill run."""
    total_phoneless:   int = 0
    found_text:        int = 0
    found_name_match:  int = 0
    found_cross_match: int = 0
    found_playwright:  int = 0
    dry_run:           bool = False

    # Per-source breakdown: {source: {"candidates": int, "found": int}}
    pw_by_source: dict = field(default_factory=dict)

    @property
    def total_found(self) -> int:
        return self.found_text + self.found_name_match + self.found_cross_match + self.found_playwright

    @property
    def total_remaining(self) -> int:
        return self.total_phoneless - self.total_found

    def as_text(self) -> str:
        mode = " [DRY RUN — nada guardado]" if self.dry_run else ""
        lines = [
            f"── Backfill Phone Enrichment{mode} ──",
            f"  Leads sem telefone analisados : {self.total_phoneless}",
            f"  ✓ Encontrados (total)         : {self.total_found}",
            f"    Stage 1 — texto/descrição   : {self.found_text}",
            f"    Stage 1b— nome (cross-name) : {self.found_name_match}",
            f"    Stage 2 — cross-match (relax): {self.found_cross_match}",
            f"    Stage 3 — Playwright        : {self.found_playwright}",
            f"  Ainda sem telefone             : {self.total_remaining}",
        ]
        if self.pw_by_source:
            lines.append("  Playwright por fonte:")
            for src, counts in self.pw_by_source.items():
                lines.append(
                    f"    {src:15s}  candidatos={counts['candidates']:3d}  "
                    f"encontrados={counts['found']:3d}"
                )
        return "\n".join(lines)


# ── Main backfiller ───────────────────────────────────────────────────────────

class PhoneBackfiller:
    """
    Retroactive phone enrichment across three stages.

    Instantiate once per run; do not reuse across multiple runs.
    """

    def __init__(
        self,
        pw_limit_per_source: int = 40,
        dry_run: bool = False,
    ):
        self.pw_limit = pw_limit_per_source
        self.dry_run  = dry_run

    # ── Public entry point ────────────────────────────────────────────────────

    def run(
        self,
        limit:   int = 500,
        sources: Optional[list[str]] = None,
        skip_pw: bool = False,
    ) -> BackfillStats:
        """
        Run all three enrichment stages for phoneless leads.

        Args:
            limit:   Max leads to process in this run.
            sources: Filter by discovery_source (None = all sources).
            skip_pw: Skip Playwright stage (quick text-only pass).

        Returns BackfillStats summarising what was found.
        """
        from sqlalchemy import or_
        from storage.database import get_db
        from storage.models import Lead

        stats = BackfillStats(dry_run=self.dry_run)

        with get_db() as db:
            q = (
                db.query(Lead)
                .filter(
                    Lead.is_demo == False,      # noqa: E712
                    Lead.archived == False,     # noqa: E712
                    or_(
                        Lead.contact_phone == None,
                        Lead.contact_phone == "",
                    ),
                )
            )
            if sources:
                q = q.filter(Lead.discovery_source.in_(sources))
            leads = (
                q.order_by(Lead.score.desc())
                 .limit(limit)
                 .all()
            )

        stats.total_phoneless = len(leads)
        if not leads:
            log.info("[backfill] No phoneless leads found — nothing to do")
            return stats

        log.info(
            "[backfill] {n} phoneless leads to process (limit={lim}, sources={src})",
            n=len(leads), lim=limit, src=sources or "all",
        )

        enriched_ids: set[int] = set()

        # ── Stage 1: text extraction ──────────────────────────────────────────
        n1 = self._stage_text(leads, enriched_ids)
        stats.found_text = n1
        log.info("[backfill] Stage 1  (text)        : +{n}", n=n1)

        # ── Stage 1b: name-based match ────────────────────────────────────────
        # Free instant pass: if two leads share the same contact_name + zone
        # and one has a phone, propagate it to the other.
        n1b = self._stage_name_match(leads, enriched_ids)
        stats.found_name_match = n1b
        log.info("[backfill] Stage 1b (name-match)  : +{n}", n=n1b)

        # ── Stage 2: relaxed cross-match ──────────────────────────────────────
        # Inline fuzzy matcher with threshold=0.60 and min 1 signal.
        # Unlike pipeline/cross_matcher.py this allows same-source matching
        # and requires only title similarity when price is unavailable.
        n2 = self._stage_cross_match(leads, enriched_ids)
        stats.found_cross_match = n2
        log.info("[backfill] Stage 2  (cross-match) : +{n}", n=n2)

        # Refresh enriched_ids after cross-match (DB was updated)
        self._refresh_enriched_ids(leads, enriched_ids)

        # ── Stage 3: Playwright ───────────────────────────────────────────────
        if not skip_pw:
            remaining = [
                l for l in leads
                if l.id not in enriched_ids and _lead_url(l)
            ]
            n3, pw_src = self._stage_playwright(remaining)
            stats.found_playwright = n3
            stats.pw_by_source     = pw_src
            log.info("[backfill] Stage 3 (Playwright)  : +{n}", n=n3)

        log.info(
            "[backfill] Complete — {found}/{total} leads enriched",
            found=stats.total_found, total=stats.total_phoneless,
        )
        return stats

    # ── Stage 1: text extraction ──────────────────────────────────────────────

    def _stage_text(self, leads: list, enriched_ids: set) -> int:
        """
        Scan description and title for PT phone numbers.
        Tries description first (more likely to have raw numbers), then title.
        """
        from utils.phone import extract_phone_from_text

        found = 0
        diag_count = 0   # log first 3 checked descriptions for operator visibility
        for lead in leads:
            phone = ""
            if lead.description:
                if diag_count < 3:
                    log.info(
                        "[backfill] text DIAG id={id} desc_preview={d!r:.120}",
                        id=lead.id, d=(lead.description or "")[:120],
                    )
                    diag_count += 1
                phone = extract_phone_from_text(lead.description)
            if not phone and lead.title:
                phone = extract_phone_from_text(lead.title)

            if phone:
                if self._save_phone(lead, phone, "backfill_text"):
                    enriched_ids.add(lead.id)
                    found += 1
                    log.debug("[backfill] text id={id} → {p}", id=lead.id, p=phone)
        return found

    # ── Stage 1b: name-based match ────────────────────────────────────────────

    def _stage_name_match(self, phoneless_leads: list, enriched_ids: set) -> int:
        """
        Propagate phones between leads that share the same contact_name + zone.

        Same landlord/owner often appears on multiple portals; one listing
        may have had the phone visible, another may not.  This pass catches
        those cases cheaply — no HTTP, instant.
        """
        import unicodedata
        import re as _re
        from storage.database import get_db
        from storage.models import Lead

        def _nname(s: str) -> str:
            if not s:
                return ""
            nfkd = unicodedata.normalize("NFKD", s.lower())
            ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
            return _re.sub(r"\s+", " ", _re.sub(r"[^\w\s]", " ", ascii_)).strip()

        # Load donors: leads that have phone + name
        with get_db() as db:
            donors = (
                db.query(Lead.contact_name, Lead.contact_phone, Lead.zone)
                  .filter(
                      Lead.is_demo == False,           # noqa: E712
                      Lead.contact_phone.isnot(None),
                      Lead.contact_phone != "",
                      Lead.contact_name.isnot(None),
                      Lead.contact_name != "",
                  )
                  .all()
            )

        # Build lookup: (norm_zone, norm_name) → canonical phone
        name_phone: dict[tuple[str, str], str] = {}
        for d_name, d_phone, d_zone in donors:
            key = (_nname(d_zone or ""), _nname(d_name))
            if key[1] and key not in name_phone:
                name_phone[key] = d_phone

        log.debug("[backfill] name-match: {n} donor (zone,name) keys", n=len(name_phone))

        found = 0
        for lead in phoneless_leads:
            if lead.id in enriched_ids or not lead.contact_name:
                continue
            key = (_nname(lead.zone or ""), _nname(lead.contact_name))
            phone = name_phone.get(key)
            if phone and self._save_phone(lead, phone, "backfill_name_match"):
                enriched_ids.add(lead.id)
                found += 1
                log.debug(
                    "[backfill] name-match id={id} name={nm!r} → {p}",
                    id=lead.id, nm=lead.contact_name, p=phone,
                )
        return found

    # ── Stage 2: precision cross-match ───────────────────────────────────────

    def _stage_cross_match(self, phoneless_leads: list, enriched_ids: set) -> int:
        """
        Precision cross-match for phone propagation.

        Design goals: fewer false positives, higher commercial value.

        Hard gates (score → 0.0 immediately):
          1. Zone must match
          2. Property type must match when both sides have a known type
             (quarto ≠ apartamento, moradia ≠ terreno, etc.)

        Scoring dimensions:
          title_sim  weight=1.0   SequenceMatcher ratio on normalised title
          price_sim  weight=1.5   only when BOTH leads have price > 0

        Adaptive threshold:
          0.60  when both leads have a price (price confirms the match)
          0.65  when either lead has no price (title-only, stricter)

        Price asymmetry penalty:
          When one lead has a price and the other does not, a penalty
          signal (sim=0.20, weight=0.5) is injected, pulling the score down.
          Rationale: same property listed by the same owner should always
          have the same price; its absence on one side is suspicious.

        Diagnostics:
          Every comparison that scores > 0.30 logs a one-line breakdown
          (type_a, type_b, title_sim, price_sim, penalty, final_score,
          effective_threshold).  First 10 per run are logged at INFO;
          the rest at DEBUG to avoid log flooding on large datasets.
        """
        import unicodedata
        import re as _re
        from difflib import SequenceMatcher
        from storage.database import get_db
        from storage.models import Lead

        if self.dry_run:
            log.info("[backfill] [DRY RUN] skipping cross-match stage")
            return 0

        # ── helpers ──────────────────────────────────────────────────────────

        def _norm(s: str) -> str:
            if not s:
                return ""
            nfkd = unicodedata.normalize("NFKD", s.lower())
            ascii_ = "".join(c for c in nfkd if not unicodedata.combining(c))
            return _re.sub(r"\s+", " ", _re.sub(r"[^\w\s]", " ", ascii_)).strip()

        def _norm_zone(z: str) -> str:
            return _norm(z or "")

        # ── property type classifier ─────────────────────────────────────────
        # Keywords are checked against the normalised combined text
        # "title + typology" so both fields contribute.
        # Order matters: quarto before apartamento because "apartamento t2" is
        # an apartment, but "quarto em apartamento t3" is a quarto.
        _TYPE_RULES: list[tuple[str, list[str]]] = [
            ("quarto",     ["quarto", " cama ", "room", "suite", "quartos para",
                            "quarto para", "partilhado", "partilhada"]),
            ("terreno",    ["terreno", " lote ", "parcela"]),
            ("comercial",  ["comercial", "escritorio", "escritório", "loja",
                            "armazem", "armazém", "oficina", "garagem", "office"]),
            ("moradia",    ["moradia", "vivenda", "villa", "chalet", " casa ",
                            "casa de", "quinta ", "moradia "]),
            ("apartamento",["apartamento", " apt ", "apto", "flat",
                            " t1 ", " t2 ", " t3 ", " t4 ", " t5 ", " t6 ",
                            "t1\n", "t2\n", "t3\n", "t4\n", "t5\n"]),
        ]
        _UNKNOWN_TYPE = "desconhecido"

        def _detect_type(lead) -> str:
            # Pad with spaces so boundary keywords like " casa " match at start/end
            combined = " " + _norm(
                (lead.title or "") + " " + (lead.typology or "")
            ) + " "
            # Quick typology-field shortcut (e.g. "T2" stored in typology)
            typo = _norm(lead.typology or "")
            if typo and typo not in {"desconhecido", "unknown", "none", "null", ""}:
                if _re.match(r"^t[0-9]$", typo):
                    return "apartamento"
                if typo == "quarto":
                    return "quarto"
                if typo in ("moradia", "vivenda"):
                    return "moradia"
                if typo == "terreno":
                    return "terreno"
                if typo in ("comercial", "escritório", "escritorio"):
                    return "comercial"
            # Fall back to keyword scan on padded combined text
            for type_name, keywords in _TYPE_RULES:
                for kw in keywords:
                    if kw in combined:
                        return type_name
            return _UNKNOWN_TYPE

        # ── scoring function ─────────────────────────────────────────────────

        def _score_and_explain(
            target, donor, t_type: str, d_type: str
        ) -> tuple[float, float, list[str]]:
            """
            Returns (score, effective_threshold, reasons).
            score == 0.0 means hard-gated (do not match).
            """
            reasons: list[str] = [f"type_a={t_type}", f"type_b={d_type}"]

            # Gate 1: zone
            if not target.zone or not donor.zone:
                return 0.0, 0.65, reasons + ["GATE:no_zone"]
            if _norm_zone(target.zone) != _norm_zone(donor.zone):
                return 0.0, 0.65, reasons + ["GATE:zone_mismatch"]

            # Gate 2: property type — hard block on known type mismatch
            if t_type != _UNKNOWN_TYPE and d_type != _UNKNOWN_TYPE:
                if t_type != d_type:
                    return 0.0, 0.65, reasons + [f"GATE:type_mismatch({t_type}≠{d_type})"]
                reasons.append("type_match=YES")
            else:
                reasons.append("type_match=UNKNOWN(soft)")

            signals: list[tuple[float, float]] = []

            # Signal 1: title similarity
            ta = _norm(target.title or "")
            tb = _norm(donor.title or "")
            if ta and tb:
                title_sim = SequenceMatcher(None, ta, tb).ratio()
                signals.append((title_sim, 1.0))
                reasons.append(f"title_sim={title_sim:.3f}")
            else:
                reasons.append("title_sim=N/A")

            # Signal 2: price — adaptive threshold + asymmetry penalty
            has_price_a = bool(target.price and target.price > 0)
            has_price_b = bool(donor.price and donor.price > 0)

            threshold = 0.60 if (has_price_a and has_price_b) else 0.65

            if has_price_a and has_price_b:
                diff = abs(target.price - donor.price) / max(target.price, donor.price)
                if diff <= 0.02:    p_sim = 1.00
                elif diff <= 0.10:  p_sim = 0.75
                elif diff <= 0.20:  p_sim = 0.40
                else:               p_sim = 0.00
                signals.append((p_sim, 1.5))
                reasons.append(f"price_sim={p_sim:.2f}(diff={diff:.1%})")
            elif has_price_a != has_price_b:
                # Penalty: one lead has a price, the other doesn't
                signals.append((0.20, 0.5))
                reasons.append("price=ASYMMETRIC(penalised)")
            else:
                reasons.append("price=BOTH_MISSING")

            if not signals:
                return 0.0, threshold, reasons + ["no_signals"]

            total_w = sum(w for _, w in signals)
            total_s = sum(s * w for s, w in signals)
            score = total_s / total_w
            reasons.append(f"score={score:.3f}(threshold={threshold})")
            return score, threshold, reasons

        # ── load donors ───────────────────────────────────────────────────────

        with get_db() as db:
            donors = (
                db.query(Lead)
                  .filter(
                      Lead.is_demo == False,      # noqa: E712
                      Lead.archived == False,     # noqa: E712
                      Lead.contact_phone.isnot(None),
                      Lead.contact_phone != "",
                  )
                  .all()
            )

        if not donors:
            log.info("[backfill] cross-match: no donor leads with phones in DB")
            return 0

        # Pre-compute type for every donor (avoid recomputing per comparison)
        donor_type: dict[int, str] = {d.id: _detect_type(d) for d in donors}

        # Index donors by (normalised_zone, prop_type) for faster lookup.
        # Unknown-type donors are indexed under every type slot so they can
        # match any target (matching is still subject to the score threshold).
        by_zone_type: dict[tuple[str, str], list] = {}
        for d in donors:
            zone_key = _norm_zone(d.zone or "unknown")
            d_type = donor_type[d.id]
            # index under own type
            by_zone_type.setdefault((zone_key, d_type), []).append(d)
            # also index unknown donors under a wildcard key
            if d_type == _UNKNOWN_TYPE:
                by_zone_type.setdefault((zone_key, "__any__"), []).append(d)

        log.info(
            "[backfill] cross-match: {d} donor leads, {zt} (zone×type) buckets",
            d=len(donors), zt=len(by_zone_type),
        )

        # ── main loop ─────────────────────────────────────────────────────────

        found = 0
        comparisons = 0
        diag_logged = 0    # INFO for first 10 relevant comparisons, then DEBUG

        for lead in phoneless_leads:
            if lead.id in enriched_ids:
                continue

            zone_key = _norm_zone(lead.zone or "unknown")
            t_type = _detect_type(lead)

            # Candidates: same type + unknown-type donors in same zone
            candidates: list = (
                by_zone_type.get((zone_key, t_type), [])
                + (by_zone_type.get((zone_key, "__any__"), [])
                   if t_type != _UNKNOWN_TYPE else [])
            )
            if not candidates:
                continue

            best_score = 0.0
            best_threshold = 0.65
            best_donor = None
            best_reasons: list[str] = []

            for donor in candidates:
                if donor.id == lead.id:
                    continue
                d_type = donor_type[donor.id]
                score, threshold, reasons = _score_and_explain(
                    lead, donor, t_type, d_type
                )
                comparisons += 1

                if score > 0.30:
                    msg = (
                        f"[backfill] cross-match DIAG  "
                        f"id={lead.id} vs id={donor.id}  "
                        + "  ".join(reasons)
                    )
                    if diag_logged < 10:
                        log.info(msg)
                        diag_logged += 1
                    else:
                        log.debug(msg)

                if score > best_score:
                    best_score = score
                    best_threshold = threshold
                    best_donor = donor
                    best_reasons = reasons

            if best_donor and best_score >= best_threshold:
                if self._save_phone(
                    lead, best_donor.contact_phone, "backfill_cross_match"
                ):
                    enriched_ids.add(lead.id)
                    found += 1
                    log.info(
                        "[backfill] cross-match MATCH  id={a} ← id={b}  "
                        + "  ".join(best_reasons),
                        a=lead.id, b=best_donor.id,
                    )

        log.info(
            "[backfill] cross-match: {c} comparisons, {f} phones found",
            c=comparisons, f=found,
        )
        return found

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _refresh_enriched_ids(self, leads: list, enriched_ids: set) -> None:
        """
        Reload contact_phone from DB for all leads in the list and add
        any that now have a phone to enriched_ids.
        """
        from storage.database import get_db
        from storage.models import Lead

        ids = [l.id for l in leads if l.id not in enriched_ids]
        if not ids:
            return
        with get_db() as db:
            rows = (
                db.query(Lead.id, Lead.contact_phone)
                  .filter(Lead.id.in_(ids))
                  .all()
            )
        for lead_id, phone in rows:
            if phone:
                enriched_ids.add(lead_id)
                # Reflect back onto the in-memory object
                for l in leads:
                    if l.id == lead_id:
                        l.contact_phone = phone
                        break

    # ── Stage 3: Playwright ───────────────────────────────────────────────────

    def _stage_playwright(self, leads: list) -> tuple[int, dict]:
        """
        Group phoneless leads by discovery_source and run one
        PlaywrightPhoneRevealer batch per source.
        Each source respects self.pw_limit.

        Returns (total_found, {source: {"candidates": N, "found": N}}).
        """
        from scrapers.base import PlaywrightPhoneRevealer
        from config.settings import settings

        if not leads:
            return 0, {}

        # Group by source
        by_source: dict[str, list] = {}
        for lead in leads:
            src = lead.discovery_source or "unknown"
            by_source.setdefault(src, []).append(lead)

        total_found = 0
        by_src_stats: dict[str, dict] = {}

        # Sources where phone-reveal via Playwright is not applicable
        _SKIP_SOURCES = {"standvirtual", "linkedin", "sapo", "unknown"}

        for source, src_leads in by_source.items():
            if source in _SKIP_SOURCES:
                log.info(
                    "[backfill] Playwright source={src} — SKIPPED (no phone-reveal button)",
                    src=source,
                )
                by_src_stats[source] = {"candidates": 0, "found": 0}
                continue

            candidates = src_leads[:self.pw_limit]
            btn_selectors = _SOURCE_PHONE_BTNS.get(source, _DEFAULT_PHONE_BTNS)

            log.info(
                "[backfill] Playwright source={src} — {n}/{total} candidatos",
                src=source, n=len(candidates), total=len(src_leads),
            )

            if self.dry_run:
                log.info(
                    "[backfill] [DRY RUN] would Playwright-reveal {n} URLs for {src}",
                    n=len(candidates), src=source,
                )
                by_src_stats[source] = {"candidates": len(candidates), "found": 0}
                continue

            # Build url→lead map (URL from sources_json)
            url_map: dict[str, object] = {}
            for lead in candidates:
                url = _lead_url(lead)
                if url:
                    url_map[url] = lead

            if not url_map:
                log.debug("[backfill] No URLs for source={src} — skipping Playwright", src=source)
                by_src_stats[source] = {"candidates": 0, "found": 0}
                continue

            sample_urls = list(url_map.keys())[:3]
            log.info(
                "[backfill] Playwright source={src} — {n} URLs to test. "
                "First 3: {urls}",
                src=source, n=len(url_map),
                urls=[u[-70:] for u in sample_urls],
            )

            revealer = PlaywrightPhoneRevealer(
                phone_btn_selectors=btn_selectors,
                consent_selectors=_CONSENT_SELECTORS,
                headless=settings.headless_browser,
                inter_fetch_delay_min=1.0,
                inter_fetch_delay_max=2.0,
            )
            phones = revealer.reveal_batch(list(url_map.keys()))

            source_found = 0
            for url, phone in phones.items():
                lead = url_map.get(url)
                if lead and self._save_phone(
                    lead, phone, f"backfill_playwright_{source}"
                ):
                    source_found += 1
                    log.debug("[backfill] playwright id={id} → {p}", id=lead.id, p=phone)

            total_found += source_found
            by_src_stats[source] = {
                "candidates": len(url_map),
                "found":      source_found,
            }
            log.info(
                "[backfill] Playwright source={src} — {f}/{c} encontrados",
                src=source, f=source_found, c=len(url_map),
            )

        return total_found, by_src_stats

    # ── DB write ──────────────────────────────────────────────────────────────

    def _save_phone(self, lead, raw_phone: str, source_tag: str) -> bool:
        """
        Validate raw_phone and persist to the lead record.

        Updates: contact_phone, phone_type, contact_confidence, contact_source.
        Clears scored_at so the next `score` run recalculates the lead.
        Returns True if the phone was valid and the record was updated.
        """
        from utils.phone import validate_pt_phone

        result = validate_pt_phone(raw_phone)
        if not result.valid:
            log.debug(
                "[backfill] invalid phone {p} for id={id}: {r}",
                p=raw_phone, id=lead.id, r=result.reason,
            )
            return False

        if self.dry_run:
            log.info(
                "[backfill] [DRY RUN] id={id} receberia {p} ({t}) via {src}",
                id=lead.id, p=result.canonical, t=result.phone_type, src=source_tag,
            )
            return True   # count it but don't write

        from storage.database import get_db
        from storage.models import Lead

        with get_db() as db:
            obj = db.query(Lead).filter(Lead.id == lead.id).first()
            if not obj:
                return False
            # Guard: if another stage already filled this lead, skip
            if obj.contact_phone:
                return False
            obj.contact_phone      = result.canonical
            obj.phone_type         = result.phone_type
            obj.contact_confidence = result.confidence
            obj.contact_source     = source_tag
            obj.scored_at          = None   # triggers re-score
            db.commit()

        # Reflect back to in-memory object for subsequent stage guards
        lead.contact_phone      = result.canonical
        lead.phone_type         = result.phone_type
        lead.contact_confidence = result.confidence
        lead.contact_source     = source_tag

        return True

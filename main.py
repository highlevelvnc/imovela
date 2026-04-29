"""
IMOVELA — Lead intelligence imobiliária

Motor de descoberta, classificação e scoring de oportunidades imobiliárias
em Portugal. Multi-fonte (OLX, Imovirtual, Idealista, Sapo, Custojusto),
multi-canal (telefone, WhatsApp, e-mail, website) e multi-sinal (FSBO,
agência, leilão, banco, queda de preço, super-seller).

Comandos principais:
  python main.py init              Inicializar base de dados
  python main.py run               Pipeline completo (scraping + scoring + alertas)
  python main.py scrape            Apenas scraping
  python main.py process           Apenas processamento
  python main.py score             Calcular / atualizar scores
  python main.py alerts            Despoletar alertas para leads HOT
  python main.py dashboard         Abrir o dashboard web (Streamlit)
  python main.py scheduler         Arrancar agendamento diário

Enriquecimento avançado:
  python main.py enrich-websites           Visitar sites de agências (telefone+e-mail)
  python main.py enrich-sellers            Sweep de perfis OLX (super-sellers)
  python main.py train-owner-classifier    Treinar classificador FSBO/agência
  python main.py reclassify-owners         Aplicar classificador na DB
  python main.py detect-price-drops        Marcar leads com queda de preço
  python main.py geocode-leads             Resolver coordenadas (cache + Nominatim)

Exportação comercial:
  python main.py export-contacts --format xlsx --score-min 50
  python main.py export-commercial --premium-limit 30 --expanded-limit 100

Opções globais:
  --zones     Zonas separadas por vírgula (default: todas do .env)
  --sources   Fontes: olx,imovirtual,idealista,sapo,custojusto
  --debug     Activar logging detalhado
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich import box

# Ensure project root is on path when running as `python main.py`
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

console = Console()


def _ensure_db():
    from storage.database import init_db
    init_db()


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.group()
@click.option("--debug", is_flag=True, help="Enable DEBUG logging")
def cli(debug: bool):
    """\b
    ◆  IMOVELA  ·  Lead intelligence imobiliária
       Real-estate opportunity engine for Portugal.
    """
    if debug:
        import os
        os.environ["LOG_LEVEL"] = "DEBUG"
    _ensure_db()


@cli.command()
def init():
    """Initialise the database and create all tables."""
    from storage.database import init_db
    init_db()
    console.print("[bold green]✓ Database initialised[/bold green]")


@cli.command()
@click.option("--zones", default=None, help="Comma-separated zones")
@click.option("--sources", default=None, help="Comma-separated sources")
def run(zones: str, sources: str):
    """Full pipeline: scrape → process → score → alerts → report."""
    from pipeline.runner import PipelineRunner
    from scoring.scorer import Scorer
    from alerts.notifier import Notifier
    from reports.generator import ReportGenerator

    zone_list   = [z.strip() for z in zones.split(",")] if zones else None
    source_list = [s.strip() for s in sources.split(",")] if sources else None

    console.print("[bold cyan]🚀 Starting full pipeline...[/bold cyan]")

    with console.status("[cyan]Running pipeline...[/cyan]"):
        runner = PipelineRunner()
        stats = runner.run_full(sources=source_list, zones=zone_list)

    console.print(f"[green]✓ Pipeline complete[/green]")
    console.print(f"  Raw processed: {stats.raw_processed}")
    console.print(f"  Leads created: [bold]+{stats.leads_created}[/bold]")
    console.print(f"  Leads updated: ↑{stats.leads_updated}")
    console.print(f"  Errors: {'[red]' + str(stats.errors) + '[/red]' if stats.errors else '0'}")

    with console.status("[cyan]Scoring...[/cyan]"):
        n_scored = Scorer().score_all_pending()
    console.print(f"[green]✓ Scored {n_scored} leads[/green]")

    with console.status("[cyan]Sending alerts...[/cyan]"):
        n_alerted = Notifier().check_and_alert_hot_leads()
    console.print(f"[green]✓ {n_alerted} hot lead alerts sent[/green]")

    leads_report = ReportGenerator().daily_report_leads(20)
    Notifier().send_daily_report(leads_report)
    console.print(f"[green]✓ Daily report sent ({len(leads_report)} leads)[/green]")


@cli.command()
@click.option("--zones", default=None, help="Comma-separated zones")
@click.option("--sources", default=None, help="Comma-separated sources: olx,imovirtual,idealista,sapo,custojusto")
def scrape(zones: str, sources: str):
    """Scrape all sources and persist raw listings."""
    from pipeline.runner import PipelineRunner

    zone_list   = [z.strip() for z in zones.split(",")] if zones else None
    source_list = [s.strip() for s in sources.split(",")] if sources else ["olx", "imovirtual", "idealista", "sapo", "custojusto"]

    from config.settings import settings
    console.print(f"[cyan]Scraping: {source_list}[/cyan]")
    runner = PipelineRunner()
    runner._run_scrapers(source_list, zone_list or settings.zones)


@cli.command()
@click.option("--source", default=None, help="Filter by source name")
@click.option("--limit", default=1000, help="Max raw listings to process")
def process(source: str, limit: int):
    """Process raw listings through normalise → deduplicate → enrich → upsert."""
    from pipeline.runner import PipelineRunner

    console.print("[cyan]Processing raw listings...[/cyan]")
    stats = PipelineRunner().process_raw(source=source, limit=limit)
    console.print(f"[green]Done: +{stats.leads_created} new, ↑{stats.leads_updated} updated, ✗{stats.errors} errors[/green]")


@cli.command(name="scrape-olx")
@click.option("--zones",      default=None,  help="Comma-separated zones (default: all from .env)")
@click.option("--max-pages",  default=5,     type=int, show_default=True, help="Max pages per zone")
@click.option("--no-details", is_flag=True,  default=False, help="Skip detail-page fetches (faster)")
@click.option("--no-process", is_flag=True,  default=False, help="Skip pipeline after scraping")
def scrape_olx(zones: str, max_pages: int, no_details: bool, no_process: bool):
    """Scrape OLX Portugal → persist raw → run pipeline → score."""
    from scrapers.olx import OLXScraper
    from pipeline.runner import PipelineRunner
    from scoring.scorer import Scorer
    from config.settings import settings

    zone_list   = [z.strip() for z in zones.split(",")] if zones else settings.zones
    fetch_dets  = not no_details

    console.print(f"[bold cyan]OLX Scraper[/bold cyan]")
    console.print(f"  Zonas:        {zone_list}")
    console.print(f"  Max páginas:  {max_pages}")
    console.print(f"  Detail pages: {'sim' if fetch_dets else 'não (--no-details)'}")
    console.print("")

    # ── 1. Scrape ─────────────────────────────────────────────────────────────
    scraper = OLXScraper(max_pages=max_pages, fetch_details=fetch_dets)

    with console.status("[cyan]A recolher anúncios OLX...[/cyan]"):
        result = scraper.run(zones=zone_list)

    if result.errors:
        for err in result.errors:
            console.print(f"[yellow]  ⚠ {err}[/yellow]")

    console.print(
        f"[green]✓ Scraping concluído[/green] — "
        f"{len(result.items)} anúncios recolhidos | {len(result.errors)} erros"
    )

    # ── 2. Persist raw listings ───────────────────────────────────────────────
    runner = PipelineRunner()
    saved  = runner._persist_raw(result.items, "olx", result.batch_id)
    console.print(f"  → {saved} novos raw listings guardados ({len(result.items) - saved} duplicados ignorados)")

    if no_process:
        console.print("[dim]Pipeline ignorado (--no-process). Corre 'python main.py process' manualmente.[/dim]")
        return

    # ── 3. Normalise → deduplicate → enrich → upsert ─────────────────────────
    with console.status("[cyan]A processar pipeline...[/cyan]"):
        stats = runner.process_raw(source="olx")

    console.print(
        f"[green]✓ Pipeline completo[/green] — "
        f"+{stats.leads_created} novos leads | ↑{stats.leads_updated} actualizados | "
        f"✗{stats.errors} erros"
    )

    # ── 4. Score ──────────────────────────────────────────────────────────────
    with console.status("[cyan]A calcular scores...[/cyan]"):
        n_scored = Scorer().score_all_pending()

    console.print(f"[green]✓ {n_scored} leads com score calculado[/green]")
    console.print("")

    # ── 5. Summary ────────────────────────────────────────────────────────────
    from reports.generator import ReportGenerator
    summary = ReportGenerator().get_summary_stats()

    table = Table(title="Resumo", box=box.SIMPLE)
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor",   style="bold white")
    table.add_row("Total leads activos",  str(summary.get("total_active", 0)))
    table.add_row("🔴 HOT",              str(summary.get("hot_count", 0)))
    table.add_row("🟡 WARM",             str(summary.get("warm_count", 0)))
    table.add_row("🔵 COLD",             str(summary.get("cold_count", 0)))
    table.add_row("Novos hoje",           str(summary.get("added_today", 0)))
    console.print(table)
    console.print("[dim]Dashboard: python main.py dashboard → http://localhost:8501[/dim]")


@cli.command(name="scrape-sapo")
@click.option("--zones",      default=None, help="Comma-separated zones (default: all from .env)")
@click.option("--max-pages",  default=6,    type=int, show_default=True, help="Max pages per zone")
@click.option("--no-process", is_flag=True, default=False, help="Skip pipeline after scraping")
def scrape_sapo(zones: str, max_pages: int, no_process: bool):
    """Scrape Sapo Casa → persist raw → run pipeline → score."""
    from scrapers.sapo import SapoScraper
    from pipeline.runner import PipelineRunner
    from scoring.scorer import Scorer
    from config.settings import settings

    zone_list = [z.strip() for z in zones.split(",")] if zones else settings.zones

    console.print(f"[bold cyan]Sapo Casa Scraper[/bold cyan]")
    console.print(f"  Zonas:       {zone_list}")
    console.print(f"  Max páginas: {max_pages}")

    scraper = SapoScraper(max_pages=max_pages)

    with console.status("[cyan]A recolher anúncios Sapo Casa...[/cyan]"):
        result = scraper.run(zones=zone_list)

    if result.errors:
        for err in result.errors:
            console.print(f"[yellow]  ⚠ {err}[/yellow]")

    console.print(
        f"[green]✓ Scraping concluído[/green] — "
        f"{len(result.items)} anúncios recolhidos | {len(result.errors)} erros"
    )

    runner = PipelineRunner()
    saved  = runner._persist_raw(result.items, "sapo", result.batch_id)
    console.print(f"  → {saved} novos raw listings guardados ({len(result.items) - saved} duplicados ignorados)")

    if no_process:
        console.print("[dim]Pipeline ignorado (--no-process).[/dim]")
        return

    with console.status("[cyan]A processar pipeline...[/cyan]"):
        stats = runner.process_raw(source="sapo")

    console.print(
        f"[green]✓ Pipeline completo[/green] — "
        f"+{stats.leads_created} novos | ↑{stats.leads_updated} actualizados | ✗{stats.errors} erros"
    )

    with console.status("[cyan]A calcular scores...[/cyan]"):
        n_scored = Scorer().score_all_pending()

    console.print(f"[green]✓ {n_scored} leads com score calculado[/green]")


@cli.command(name="scrape-custojusto")
@click.option("--zones",      default=None, help="Comma-separated zones (default: all from .env)")
@click.option("--max-pages",  default=5,    type=int, show_default=True, help="Max pages per zone")
@click.option("--no-process", is_flag=True, default=False, help="Skip pipeline after scraping")
def scrape_custojusto(zones: str, max_pages: int, no_process: bool):
    """Scrape Custojusto Imóveis → persist raw → run pipeline → score."""
    from scrapers.custojusto import CustojustoScraper
    from pipeline.runner import PipelineRunner
    from scoring.scorer import Scorer
    from config.settings import settings

    zone_list = [z.strip() for z in zones.split(",")] if zones else settings.zones

    console.print(f"[bold cyan]Custojusto Scraper[/bold cyan]")
    console.print(f"  Zonas:       {zone_list}")
    console.print(f"  Max páginas: {max_pages}")

    scraper = CustojustoScraper(max_pages=max_pages)

    with console.status("[cyan]A recolher anúncios Custojusto...[/cyan]"):
        result = scraper.run(zones=zone_list)

    if result.errors:
        for err in result.errors:
            console.print(f"[yellow]  ⚠ {err}[/yellow]")

    console.print(
        f"[green]✓ Scraping concluído[/green] — "
        f"{len(result.items)} anúncios recolhidos | {len(result.errors)} erros"
    )

    runner = PipelineRunner()
    saved  = runner._persist_raw(result.items, "custojusto", result.batch_id)
    console.print(f"  → {saved} novos raw listings guardados ({len(result.items) - saved} duplicados ignorados)")

    if no_process:
        console.print("[dim]Pipeline ignorado (--no-process).[/dim]")
        return

    with console.status("[cyan]A processar pipeline...[/cyan]"):
        stats = runner.process_raw(source="custojusto")

    console.print(
        f"[green]✓ Pipeline completo[/green] — "
        f"+{stats.leads_created} novos | ↑{stats.leads_updated} actualizados | ✗{stats.errors} erros"
    )

    with console.status("[cyan]A calcular scores...[/cyan]"):
        n_scored = Scorer().score_all_pending()

    console.print(f"[green]✓ {n_scored} leads com score calculado[/green]")


@cli.command()
def score():
    """Score all leads that haven't been scored in the last 24 hours."""
    from scoring.scorer import Scorer

    console.print("[cyan]Scoring leads...[/cyan]")
    n = Scorer().score_all_pending()
    console.print(f"[green]✓ {n} leads scored[/green]")


@cli.command(name="cross-match")
@click.option("--dry-run", is_flag=True, default=False,
              help="Analyse matches without writing to DB (preview only)")
def cross_match(dry_run: bool):
    """Cross-portal contact discovery — propagate contacts across matching leads.

    Scans all active leads. For each lead missing a phone/email/WhatsApp,
    searches leads from OTHER portals in the same zone for a property match
    (zone + price ±20% + typology + area ±20% + title similarity).

    If score >= 0.72 → copies missing contact fields from the matching lead.

    Safe to run multiple times — only fills NULL fields, never overwrites.
    """
    from pipeline.runner import PipelineRunner

    if dry_run:
        console.print("[yellow]⚠  Dry-run mode — no DB writes[/yellow]")
        # In dry-run: load and score but do not commit
        from pipeline.cross_matcher import CrossMatcher, find_candidate_matches, _missing_contact, _norm_zone
        from sqlalchemy import select
        from storage.models import Lead
        from storage.database import get_db

        with get_db() as db:
            all_leads = db.execute(select(Lead).where(Lead.archived == False)).scalars().all()  # noqa: E712
            by_zone: dict[str, list] = {}
            for lead in all_leads:
                key = _norm_zone(lead.zone or "unknown")
                by_zone.setdefault(key, []).append(lead)

            preview_rows = []
            for lead in all_leads:
                if not _missing_contact(lead):
                    continue
                zone_key   = _norm_zone(lead.zone or "unknown")
                candidates = by_zone.get(zone_key, [])
                matches    = find_candidate_matches(lead, candidates)
                if not matches:
                    continue
                best, score = matches[0]
                preview_rows.append((lead, best, score))

        if not preview_rows:
            console.print("[dim]No cross-portal matches found above threshold.[/dim]")
            return

        from rich.table import Table
        from rich import box as rbox
        tbl = Table(title=f"Cross-match preview ({len(preview_rows)} matches)", box=rbox.SIMPLE)
        tbl.add_column("Lead ID", style="dim")
        tbl.add_column("Source", style="cyan")
        tbl.add_column("Zone")
        tbl.add_column("Score", justify="right")
        tbl.add_column("Donor", style="green")
        tbl.add_column("Donor phone", style="green")
        tbl.add_column("Donor email", style="green")
        for lead, donor, score in preview_rows[:40]:
            tbl.add_row(
                str(lead.id),
                lead.discovery_source or "—",
                lead.zone or "—",
                f"{score:.2f}",
                donor.discovery_source or "—",
                donor.contact_phone or "—",
                donor.contact_email or "—",
            )
        console.print(tbl)
        console.print(f"[dim]Run without --dry-run to apply these {len(preview_rows)} merges.[/dim]")
        return

    console.print("[cyan]Running cross-portal contact discovery...[/cyan]")
    with console.status("[cyan]Scanning leads...[/cyan]"):
        stats = PipelineRunner().run_cross_match()

    console.print(
        f"[green]✓ Cross-match complete[/green]\n"
        f"  Leads checked (missing contact): [bold]{stats['checked']}[/bold]\n"
        f"  Leads matched & enriched:        [bold green]+{stats['matched']}[/bold green]\n"
        f"  Phone numbers gained:            +{stats['phone']}\n"
        f"  Email addresses gained:          +{stats['email']}\n"
        f"  WhatsApp numbers gained:         +{stats['whatsapp']}\n"
        f"  Contact names gained:            +{stats['name']}\n"
        f"  Websites gained:                 +{stats['website']}\n"
        f"  [dim]Skipped (already contacted / no match): {stats['skipped']}[/dim]"
    )


@cli.command("backfill-contacts")
def backfill_contacts():
    """Backfill contact_source + contact_confidence for all leads where it is NULL.

    Scans the stored description and title of every lead that has no
    contact_source, attempts phone/email extraction, and updates the DB.
    Also applies the agency website lookup table for known PT agencies.
    Safe to run multiple times — only touches leads with contact_source IS NULL.
    """
    from pipeline.runner import PipelineRunner

    runner = PipelineRunner()
    console.print("[cyan]Backfilling contact channels (source / whatsapp / website)...[/cyan]")
    counts = runner.backfill_contact_source()
    console.print(
        f"[green]✓ Checked:           {counts['checked']} leads[/green]\n"
        f"[green]  source updated:    {counts['updated_source']}[/green]\n"
        f"[green]  whatsapp found:    {counts['updated_whatsapp']}[/green]\n"
        f"[green]  website found:     {counts['updated_website']}[/green]\n"
        f"[yellow]  skipped (no data): {counts['skipped']}[/yellow]"
    )
    # Also apply agency lookup to any remaining leads without a website
    console.print("[cyan]Applying agency website lookup...[/cyan]")
    ag_counts = runner.backfill_agency_contacts()
    console.print(
        f"[green]  Agency lookup — matched: {ag_counts['matched']} "
        f"website=+{ag_counts['updated_website']} "
        f"quality=+{ag_counts['updated_quality']}[/green]"
    )


@cli.command("backfill-agency")
def backfill_agency():
    """Apply agency website lookup table to leads with agency names but no website.

    Maps known PT real estate agency names (RE/MAX, ZOME, ERA, KW, Knight Frank,
    Century21, etc.) to their public website domains.  Fast — no HTTP requests.

    Safe to run multiple times — only fills NULL/empty contact_website.
    """
    from pipeline.runner import PipelineRunner

    console.print("[cyan]Agency website lookup backfill...[/cyan]")
    counts = PipelineRunner().backfill_agency_contacts()
    console.print(
        f"[green]✓ Leads checked : {counts['checked']}[/green]\n"
        f"[green]  Agency matches : {counts['matched']}[/green]\n"
        f"[green]  Websites added : [bold]+{counts['updated_website']}[/bold][/green]\n"
        f"[green]  Quality bumped : +{counts['updated_quality']} (low→medium)[/green]"
    )


@cli.command(name="enrich-websites")
@click.option("--max-agencies", default=40, type=int, show_default=True,
              help="Max unique agency websites to visit per run")
def enrich_websites(max_agencies: int):
    """Visit agency homepages to extract phone/email for leads without contact.

    Targets leads where owner_type=agency, agency_name is set, and both
    contact_phone and contact_email are missing.  Skips major franchise
    chains (RE/MAX, KW, Century21, ERA, Zome, etc.).

    Safe to run multiple times — only fills NULL/empty fields.
    """
    from pipeline.runner import PipelineRunner
    from pipeline import website_enricher as _we

    _we.MAX_AGENCIES = max_agencies

    console.print(
        f"[cyan]Website enrichment — visiting up to {max_agencies} agency homepages...[/cyan]"
    )
    with console.status("[cyan]Fetching agency websites...[/cyan]"):
        stats = PipelineRunner().run_website_enrichment()

    console.print(
        f"[green]✓ Website enrichment complete[/green]\n"
        f"  Agency leads targeted:   [bold]{stats['candidates']}[/bold]\n"
        f"  Websites probed:         {stats['sites_tried']}\n"
        f"  Websites returned 200:   {stats['sites_ok']}\n"
        f"  Agencies with contacts:  [bold green]{stats['agencies_ok']}[/bold green]\n"
        f"  Phone numbers gained:    [bold green]+{stats['phone']}[/bold green]\n"
        f"  Email addresses gained:  +{stats['email']}\n"
        f"  Websites filled:         +{stats['website']}\n"
        f"  [dim]Skipped (chain/no-slug/cap): {stats['skipped']}[/dim]"
    )


@cli.command(name="trend-report")
@click.option("--out",  default=None, help="Output PDF path (default: data/imovela_trend_YYYYMMDD.pdf)")
@click.option("--days", default=7, type=int, show_default=True, help="Window length")
def trend_report(out: str, days: int):
    """Generate the weekly Imovela PDF trend report.

    One-page PDF with hero numbers (new leads / HOT / price drops /
    super-sellers / contacted), top 10 opportunities, funnel, agency
    leaderboard, and zone breakdown. Designed to be e-mailed Monday morning.
    """
    from reports.trend_pdf import generate_trend_report
    console.print(f"[cyan]Building trend report (window={days}d)...[/cyan]")
    path = generate_trend_report(output_path=out, days=days)
    console.print(f"[green]✓ PDF saved → {path}[/green]")


@cli.command(name="detect-price-drops")
@click.option("--lookback-days",      default=30, type=int, show_default=True)
@click.option("--recent-window-days", default=14, type=int, show_default=True)
@click.option("--min-drop-pct",       default=0.05, type=float, show_default=True,
              help="Minimum % drop to flag (0.05 = 5%)")
def detect_price_drops(lookback_days: int, recent_window_days: int, min_drop_pct: float):
    """Flag leads whose asking price dropped meaningfully in recent days.

    Walks every active lead with ≥2 price observations in the lookback window,
    compares latest price vs. peak, and flags rows where drop ≥ threshold AND
    the latest observation was within the recent window.

    Flagged leads get priority_flag=True, score boost +15, and a CRMNote.
    """
    from pipeline.price_drop_detector import PriceDropDetector

    console.print(
        f"[cyan]Scanning price history (lookback={lookback_days}d, "
        f"recent={recent_window_days}d, min_drop={min_drop_pct:.0%})...[/cyan]"
    )
    stats = PriceDropDetector().run(
        lookback_days=lookback_days,
        recent_window_days=recent_window_days,
        min_drop_pct=min_drop_pct,
    )
    console.print(
        f"[green]✓ Price-drop scan complete[/green]\n"
        f"  Active leads:           {stats['considered']}\n"
        f"  With sufficient history:{stats['with_history']}\n"
        f"  Dropped ≥ threshold:    [bold yellow]{stats['dropped']}[/bold yellow]\n"
        f"  Newly flagged urgent:   [bold red]{stats['newly_flagged']}[/bold red]\n"
        f"  Average drop:           {stats['avg_drop']:.1%}"
    )


@cli.command(name="train-owner-classifier")
def train_owner_classifier():
    """Train the FSBO/agency text classifier on confident-label leads.

    Builds a TF-IDF + logistic-regression pipeline from leads where the
    owner_type label came from a strong signal (OLX trader-title, agency_name
    set on agency labels, or contact_name set on FSBO labels). Saves the
    model to data/owner_classifier.pkl.

    Reports 5-fold cross-validation accuracy.
    """
    from pipeline.owner_classifier import train_and_save
    console.print("[cyan]Training owner-type classifier...[/cyan]")
    stats = train_and_save()
    if not stats.get("trained"):
        console.print(f"[yellow]Not trained — only {stats['samples']} samples (need ≥100)[/yellow]")
        return
    console.print(
        f"[green]✓ Model trained and saved[/green]\n"
        f"  Samples used:    [bold]{stats['samples']}[/bold] "
        f"({stats['fsbo']} FSBO, {stats['agency']} agency)\n"
        f"  CV accuracy:     [bold]{stats['accuracy']:.1%}[/bold]\n"
        f"  Saved to:        {stats['model_path']}"
    )


@cli.command(name="reclassify-owners")
@click.option("--threshold", default=0.80, type=float, show_default=True,
              help="Min confidence to flip owner_type")
def reclassify_owners(threshold: float):
    """Re-score every lead with the trained model and flip uncertain labels.

    Only leads with weak signal are touched (OLX leads kept as-is — their
    trader-title label is more reliable than any model). Within the
    candidate set, the model only flips when prediction confidence ≥ threshold.
    """
    from pipeline.owner_classifier import reclassify_uncertain_leads
    console.print(f"[cyan]Re-classifying owners (threshold={threshold:.0%})...[/cyan]")
    stats = reclassify_uncertain_leads(threshold=threshold)
    if not stats.get("trained", True):
        console.print("[red]Model not found — run `train-owner-classifier` first[/red]")
        return
    console.print(
        f"[green]✓ Reclassification complete[/green]\n"
        f"  Considered:        {stats['considered']}\n"
        f"  FSBO → agency:     [bold red]{stats['fsbo_to_agency']}[/bold red]\n"
        f"  Agency → FSBO:     [bold green]{stats['agency_to_fsbo']}[/bold green]\n"
        f"  Kept (locked):     {stats['kept']}\n"
        f"  Below threshold:   {stats['low_confidence']}"
    )


@cli.command(name="enrich-sellers")
def enrich_sellers():
    """Visit OLX seller profiles to detect super-sellers and camouflaged agencies.

    Groups leads by seller_profile_url, fetches each unique profile once, and
    writes back: total active listings, "membro desde", super-flag, and
    reclassifies obvious camouflaged-agency profiles (≥10 active ads) from
    FSBO to agency owner_type.

    Idempotent — only visits profiles where seller_total_listings is still null.
    """
    from pipeline.seller_profile_enricher import SellerProfileEnricher

    console.print("[cyan]Visiting OLX seller profiles...[/cyan]")
    stats = SellerProfileEnricher().run()
    console.print(
        f"[green]✓ Seller profile enrichment complete[/green]\n"
        f"  Distinct profiles:        [bold]{stats['candidates']}[/bold]\n"
        f"  Profiles visited:         {stats['visited']}\n"
        f"  Super-sellers flagged:    [bold yellow]{stats['super_flagged']}[/bold yellow]\n"
        f"  FSBO → agency reclassif.: [bold]{stats['reclassified']}[/bold]\n"
        f"  Lead rows updated:        +{stats['updated_leads']}\n"
        f"  [dim]Fetch errors: {stats['errors']}[/dim]"
    )


@cli.command(name="geocode-leads")
@click.option("--limit",     default=1000, type=int, show_default=True,
              help="Max leads to geocode in this run")
@click.option("--no-network", is_flag=True, default=False,
              help="Cache + zone-centroid only (skip Nominatim)")
def geocode_leads(limit: int, no_network: bool):
    """Backfill latitude/longitude on every lead missing coords.

    Lookup chain: cache → Nominatim (1.1s rate-limited) → zone centroid.

    Safe to interrupt at any time — partial progress is committed in batches.
    Results are cached so repeated runs only call Nominatim for new addresses.
    """
    from utils.geocoder import geocode_leads_backfill

    console.print(
        f"[cyan]Geocoding leads (limit={limit}, network={'OFF' if no_network else 'ON'})...[/cyan]"
    )
    stats = geocode_leads_backfill(limit=limit, allow_network=not no_network)
    console.print(
        f"[green]✓ Geocoding complete[/green]\n"
        f"  Considered:        {stats['considered']}\n"
        f"  Cache hits:        {stats['cache']}\n"
        f"  Nominatim hits:    {stats['nominatim']}\n"
        f"  Zone centroid:     {stats['zone_centroid']}\n"
        f"  Skipped:           {stats['skipped']}"
    )


@cli.command()
def alerts():
    """Check for HOT leads and send notifications."""
    from alerts.notifier import Notifier

    console.print("[cyan]Checking hot leads...[/cyan]")
    n = Notifier().check_and_alert_hot_leads()
    console.print(f"[green]✓ {n} alerts sent[/green]")


@cli.command()
def report():
    """Generate and send the daily top-20 report."""
    from reports.generator import ReportGenerator
    from alerts.notifier import Notifier

    leads = ReportGenerator().daily_report_leads(20)
    Notifier().send_daily_report(leads)
    console.print(f"[green]✓ Report sent with {len(leads)} leads[/green]")


@cli.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "json"]), default="csv")
@click.option("--score-min", default=0, help="Minimum score filter")
@click.option("--output", default=None, help="Output file path")
def export(fmt: str, score_min: int, output: str):
    """Export leads to CSV or JSON."""
    from reports.generator import ReportGenerator

    gen = ReportGenerator()
    if fmt == "csv":
        path = gen.export_csv(output_path=output, score_min=score_min)
    else:
        path = gen.export_json(score_min=score_min)

    console.print(f"[green]✓ Exported to: {path}[/green]")


@cli.command(name="export-contacts")
@click.option("--score-min",        default=0,     help="Score mínimo (default: 0 = todos com telefone)")
@click.option("--zones",            default=None,  help="Zonas separadas por vírgula (ex: Lisboa,Cascais)")
@click.option("--format", "fmt",    type=click.Choice(["csv", "xlsx", "both"]), default="both",
              help="Formato de saída (default: both)")
@click.option("--include-agencies", is_flag=True, default=False,
              help="Incluir leads prováveis de agência (por default são excluídos)")
@click.option("--limit",            default=2000,  type=int, help="Máximo de contactos a exportar")
@click.option("--output-dir",       default=None,  help="Directório de saída (default: data/)")
def export_contacts(
    score_min: int,
    zones: str,
    fmt: str,
    include_agencies: bool,
    limit: int,
    output_dir: str,
):
    """Gerar lista de contactos pronta para envio ao cliente.

    Exporta apenas leads COM TELEFONE VÁLIDO, deduplicados por número,
    ordenados por score descendente.  Inclui link direto WhatsApp por
    cada contacto.  Sinaliza prováveis agências (excluídas por default).

    \b
    Exemplos:
      python main.py export-contacts
      python main.py export-contacts --score-min 50
      python main.py export-contacts --zones Lisboa,Cascais --format xlsx
      python main.py export-contacts --include-agencies --score-min 0
    """
    from reports.contact_export import run_export

    zone_list = [z.strip() for z in zones.split(",")] if zones else None

    console.print("[bold cyan]📋 Export — Lista de Contactos[/bold cyan]")
    console.print(f"  Score mínimo : {score_min}")
    console.print(f"  Zonas        : {zone_list or 'todas'}")
    console.print(f"  Formato      : {fmt}")
    console.print(f"  Agências     : {'incluídas' if include_agencies else 'excluídas'}")
    console.print("")

    with console.status("[cyan]A gerar lista de contactos...[/cyan]"):
        results = run_export(
            score_min=score_min,
            zones=zone_list,
            include_agencies=include_agencies,
            fmt=fmt,
            output_dir=output_dir,
            limit=limit,
        )

    if not results:
        console.print("[yellow]⚠  Nenhum lead encontrado com os critérios especificados.[/yellow]")
        console.print("[dim]Experimenta: python main.py export-contacts --score-min 0[/dim]")
        return

    console.print("[bold green]✓ Export concluído![/bold green]")
    for fmt_key, path in results.items():
        console.print(f"  [{fmt_key.upper()}] → [bold]{path}[/bold]")

    # Quick summary stats
    from reports.contact_export import generate_contact_list
    rows = generate_contact_list(score_min=score_min, zones=zone_list,
                                 include_agencies=include_agencies, limit=limit)
    hot  = sum(1 for r in rows if "HOT"  in r.get("label", ""))
    warm = sum(1 for r in rows if "WARM" in r.get("label", ""))
    ag   = sum(1 for r in rows if r.get("agencia") == "SIM")

    table = Table(title="Resumo da Lista", box=box.SIMPLE)
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor",   style="bold white")
    table.add_row("Total contactos únicos",  str(len(rows)))
    table.add_row("🔴 HOT leads",            str(hot))
    table.add_row("🟡 WARM leads",           str(warm))
    table.add_row("🔵 COLD leads",           str(len(rows) - hot - warm))
    table.add_row("Prováveis agências",      str(ag))
    table.add_row("Proprietários directos",  str(len(rows) - ag))
    console.print(table)


@cli.command(name="export-commercial")
@click.option("--premium-limit",  default=50,   type=int,  help="Máx leads na Lista Premium (default: 50)")
@click.option("--expanded-limit", default=150,  type=int,  help="Máx leads na Lista Expandida (default: 150)")
@click.option("--zones",          default=None,            help="Zonas separadas por vírgula (default: .env)")
@click.option("--format", "fmt",  type=click.Choice(["xlsx", "csv", "both"]), default="xlsx",
              help="Formato de saída (default: xlsx)")
@click.option("--output-dir",     default=None,            help="Directório de saída (default: data/)")
def export_commercial(
    premium_limit: int,
    expanded_limit: int,
    zones: str,
    fmt: str,
    output_dir: str,
):
    """Gerar lista comercial Premium + Expandida para entrega ao cliente.

    Produz um único ficheiro XLSX com três folhas:
      • Lista Premium   — proprietários directos, mobile-first, score alto
      • Lista Expandida — oportunidades adicionais, mais abrangente
      • Resumo Executivo — KPIs prontos para apresentação

    \b
    Exemplos:
      python main.py export-commercial
      python main.py export-commercial --premium-limit 30 --expanded-limit 100
      python main.py export-commercial --zones Lisboa,Cascais
      python main.py export-commercial --format both --output-dir exports/
    """
    from reports.commercial_export import run_commercial_export, summary_as_text

    zone_list = [z.strip() for z in zones.split(",")] if zones else None

    console.print("[bold cyan]📋 Export Comercial — Lista Premium + Expandida[/bold cyan]")
    console.print(f"  Premium limit  : {premium_limit}")
    console.print(f"  Expandida limit: {expanded_limit}")
    console.print(f"  Zonas          : {zone_list or 'todas (.env)'}")
    console.print(f"  Formato        : {fmt}")
    console.print("")

    with console.status("[cyan]A construir listas...[/cyan]"):
        result = run_commercial_export(
            premium_limit=premium_limit,
            expanded_limit=expanded_limit,
            zones=zone_list,
            fmt=fmt,
            output_dir=output_dir,
        )

    premium  = result["premium"]
    expanded = result["expanded"]
    summary  = result["summary"]
    files    = result["files"]

    # Print executive summary
    console.print(summary_as_text(summary))

    # File paths
    if files:
        console.print("[bold green]✓ Ficheiros gerados:[/bold green]")
        for key, path in files.items():
            console.print(f"  [{key.upper()}] → [bold]{path}[/bold]")
    else:
        console.print("[yellow]⚠  Nenhum ficheiro gerado (verifica openpyxl: pip install openpyxl)[/yellow]")
        return

    # Rich summary table
    console.print("")
    table = Table(title="Resumo Executivo", box=box.ROUNDED)
    table.add_column("Lista",   style="cyan",       min_width=16)
    table.add_column("Total",   style="bold white",  justify="right")
    table.add_column("HOT 🔴",  style="bold red",    justify="right")
    table.add_column("WARM 🟡", style="bold yellow", justify="right")
    table.add_column("Mobile 📱",style="bold green", justify="right")
    table.add_column("Relay 🔁", style="bold blue",  justify="right")

    p_mobile = sum(1 for r in premium  if r.get("_phone_type") == "mobile")
    p_relay  = sum(1 for r in premium  if r.get("_phone_type") == "relay")
    e_mobile = sum(1 for r in expanded if r.get("_phone_type") == "mobile")
    e_relay  = sum(1 for r in expanded if r.get("_phone_type") == "relay")

    table.add_row(
        "Premium",
        str(len(premium)),
        str(summary["premium_hot"]),
        str(summary["premium_warm"]),
        str(p_mobile),
        str(p_relay),
    )
    table.add_row(
        "Expandida",
        str(len(expanded)),
        str(summary["expanded_hot"]),
        str(summary["expanded_warm"]),
        str(e_mobile),
        str(e_relay),
    )
    table.add_row(
        "[bold]TOTAL[/bold]",
        str(summary["total_combined"]),
        str(summary["premium_hot"] + summary["expanded_hot"]),
        str(summary["premium_warm"] + summary["expanded_warm"]),
        str(summary["mobile_count"]),
        str(summary["relay_count"]),
    )
    console.print(table)

    # Top zones
    if summary["top_zones"]:
        console.print("")
        zone_table = Table(title="Top Zonas (Premium)", box=box.SIMPLE)
        zone_table.add_column("Zona",  style="cyan")
        zone_table.add_column("Leads", style="bold white", justify="right")
        for zone, count in summary["top_zones"]:
            zone_table.add_row(zone, str(count))
        console.print(zone_table)


@cli.command(name="import-leads")
@click.argument("path")
@click.option("--pattern",  default=None,  help="Glob pattern para múltiplos ficheiros (ex: 'contactos_*.csv')")
@click.option("--sheet",    default=None,  help="Nome da sheet XLSX (default: primeira)")
@click.option("--dry-run",  is_flag=True,  help="Mostrar o que faria sem guardar")
@click.option("--no-score", is_flag=True,  help="Não re-score após import")
def import_leads(path: str, pattern: str, sheet: str, dry_run: bool, no_score: bool):
    """Importar leads de ficheiros CSV/XLSX para a base de dados.

    Aceita um ficheiro ou directório. Normaliza campos, valida telefones,
    split nome/apelido, deduplica por fingerprint, e faz upsert na BD.

    Exemplos:
        python main.py import-leads data/contactos.csv
        python main.py import-leads data/ --pattern "contactos_*.csv"
        python main.py import-leads data/comercial.xlsx --sheet "Lista Premium"
        python main.py import-leads data/ --pattern "*.xlsx" --dry-run
    """
    from pipeline.importer import LeadImporter
    import os

    importer = LeadImporter(dry_run=dry_run, score_after=not no_score)

    if os.path.isdir(path):
        if not pattern:
            pattern = "*.csv"
            console.print(f"[yellow]Nenhum pattern especificado — usando '{pattern}'[/yellow]")
        stats = importer.import_directory(path, pattern)
    else:
        stats = importer.import_file(path, sheet=sheet)

    console.print(f"\n{stats.as_text()}")


@cli.command(name="normalize-export")
@click.argument("path")
@click.option("--pattern", default="*.csv",  help="Glob pattern (default: *.csv)")
@click.option("--output",  required=True,    help="Ficheiro de saída (.csv ou .xlsx)")
def normalize_export(path: str, pattern: str, output: str):
    """Normalizar múltiplos ficheiros e exportar lista limpa deduplicated.

    Lê todos os ficheiros matching, normaliza campos, remove duplicados,
    e gera um ficheiro único pronto para uso.

    Exemplos:
        python main.py normalize-export data/ --pattern "contactos_*.csv" --output data/normalizado.xlsx
        python main.py normalize-export data/ --pattern "*.xlsx" --output data/todos_leads.csv
    """
    from pipeline.importer import LeadImporter

    importer = LeadImporter()
    stats = importer.normalize_and_export(path, pattern, output)
    console.print(f"\n{stats.as_text()}")
    console.print(f"[green]✓ Exportado para {output}[/green]")


@cli.command(name="backfill-phones")
@click.option("--limit",    default=500,  type=int,   help="Máx leads a processar (default: 500)")
@click.option("--pw-limit", default=40,   type=int,   help="Máx Playwright reveals por fonte (default: 40)")
@click.option("--sources",  default=None,             help="Fontes separadas por vírgula (default: todas)")
@click.option("--skip-pw",  is_flag=True, default=False, help="Saltar Stage 3 Playwright (mais rápido)")
@click.option("--dry-run",  is_flag=True, default=False, help="Simular sem guardar nada na DB")
def backfill_phones(
    limit: int,
    pw_limit: int,
    sources: str,
    skip_pw: bool,
    dry_run: bool,
):
    """Enriquecer leads sem telefone com 3 estratégias progressivas.

    Stage 1 — Texto: extrai número da descrição/título com regex PT melhorada.
    Stage 2 — Cross-match: reutiliza telefones de leads com propriedade idêntica
               noutro portal (mesmo preço ± 5%, mesma zona, título similar).
    Stage 3 — Playwright: revela número via browser headless para leads com URL.

    Após cada número encontrado, o lead é marcado para re-scoring automático.

    \b
    Exemplos:
      python main.py backfill-phones
      python main.py backfill-phones --limit 300 --pw-limit 50
      python main.py backfill-phones --sources imovirtual,olx --skip-pw
      python main.py backfill-phones --dry-run
    """
    from pipeline.backfill_phone import PhoneBackfiller

    source_list = [s.strip() for s in sources.split(",")] if sources else None

    console.print("[bold cyan]📲 Backfill Phone Enrichment[/bold cyan]")
    if dry_run:
        console.print("[yellow]  ⚠  DRY RUN — nada será guardado[/yellow]")
    console.print(f"  Limit       : {limit}")
    console.print(f"  PW limit    : {pw_limit} por fonte")
    console.print(f"  Fontes      : {source_list or 'todas'}")
    console.print(f"  Skip PW     : {'sim' if skip_pw else 'não'}")
    console.print("")

    backfiller = PhoneBackfiller(pw_limit_per_source=pw_limit, dry_run=dry_run)

    with console.status("[cyan]Stage 1 — extracção de texto...[/cyan]"):
        pass  # status shown during .run()

    stats = backfiller.run(limit=limit, sources=source_list, skip_pw=skip_pw)

    console.print(stats.as_text())

    # Re-score leads that received a phone
    if not dry_run and stats.total_found > 0:
        console.print("")
        with console.status(f"[cyan]Re-scoring {stats.total_found} leads enriquecidos...[/cyan]"):
            from scoring.scorer import Scorer
            n_scored = Scorer().score_all_pending()
        console.print(f"[green]✓ {n_scored} leads re-scored[/green]")

    console.print("")
    if stats.total_found > 0:
        console.print(f"[bold green]✓ {stats.total_found} contactos novos encontrados[/bold green]")
    else:
        console.print("[yellow]Nenhum contacto novo encontrado neste run.[/yellow]")


@cli.command()
def status():
    """Show system status and lead statistics."""
    from reports.generator import ReportGenerator

    stats = ReportGenerator().get_summary_stats()

    # Main stats table
    table = Table(title="LeadEngine — Status", box=box.ROUNDED, show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="bold white")

    table.add_row("Total leads activos", str(stats.get("total_active", 0)))
    table.add_row("🔴 HOT", str(stats.get("hot_count", 0)))
    table.add_row("🟡 WARM", str(stats.get("warm_count", 0)))
    table.add_row("🔵 COLD", str(stats.get("cold_count", 0)))
    table.add_row("Novos hoje", str(stats.get("added_today", 0)))
    table.add_row("Score médio", str(stats.get("avg_score", 0)))
    console.print(table)

    # Zone breakdown
    zone_table = Table(title="Por Zona", box=box.SIMPLE)
    zone_table.add_column("Zona")
    zone_table.add_column("Leads", justify="right")
    for zone, count in sorted(stats.get("by_zone", {}).items(), key=lambda x: -x[1]):
        zone_table.add_row(zone, str(count))
    console.print(zone_table)

    # CRM pipeline
    stage_table = Table(title="CRM Pipeline", box=box.SIMPLE)
    stage_table.add_column("Stage")
    stage_table.add_column("Leads", justify="right")
    for stage, count in stats.get("by_stage", {}).items():
        stage_table.add_row(stage, str(count))
    console.print(stage_table)


@cli.command()
@click.option("--run-now", is_flag=True, help="Run pipeline immediately on start")
def scheduler(run_now: bool):
    """Start the daily scheduler (runs indefinitely — use Ctrl+C to stop)."""
    from scheduler.jobs import Scheduler as S

    sched = S()
    sched.start(run_now=run_now)
    console.print(f"[green]✓ Scheduler started — daily run at {__import__('config').settings.schedule_time}[/green]")
    console.print("[dim]Press Ctrl+C to stop[/dim]")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sched.stop()
        console.print("\n[yellow]Scheduler stopped[/yellow]")


@cli.command(name="seed-demo")
@click.option("--clear", is_flag=True, help="Clear existing leads before seeding")
def seed_demo(clear: bool):
    """Populate database with 35 realistic demo leads for client presentation."""
    from data.seed_demo import run_seed

    if clear:
        console.print("[yellow]⚠  Clearing existing leads...[/yellow]")

    with console.status("[cyan]Loading demo dataset...[/cyan]"):
        result = run_seed(clear_existing=clear)

    console.print("[bold green]✓ Demo data loaded![/bold green]")
    console.print(f"  Leads criados:  [bold]+{result['created']}[/bold]")
    console.print(f"  Leads saltados: {result['skipped']} (já existiam)")
    console.print(f"  Notas CRM:      {result['notes_added']}")
    console.print(f"  Histórico preço:{result['history_added']}")
    console.print("")
    console.print("[dim]Abrir dashboard: python main.py dashboard[/dim]")


@cli.command(name="clear-demo")
@click.confirmation_option(prompt="Apagar todos os leads demo? Os dados reais ficam intactos.")
def clear_demo():
    """Remove all demo leads — real scraped data (OLX, etc.) is preserved."""
    from storage.database import get_db
    from storage.repository import LeadRepo

    with get_db() as db:
        n = LeadRepo(db).delete_demo()

    if n:
        console.print(f"[bold green]✓ {n} leads demo apagados[/bold green]")
    else:
        console.print("[dim]Nenhum lead demo encontrado (já estavam limpos).[/dim]")
    console.print("[dim]Para re-criar dados demo: python main.py seed-demo[/dim]")


@cli.command(name="reset-db")
@click.confirmation_option(prompt="⚠  Isto apaga TODOS os dados. Tens a certeza?")
def reset_db():
    """Drop and recreate all database tables (WARNING: destroys all data)."""
    from storage.database import engine
    from storage.models import Base

    console.print("[yellow]Dropping all tables...[/yellow]")
    Base.metadata.drop_all(engine)
    console.print("[yellow]Recreating tables...[/yellow]")
    Base.metadata.create_all(engine)
    console.print("[bold green]✓ Base de dados reiniciada![/bold green]")
    console.print("[dim]Para popular com dados demo: python main.py seed-demo[/dim]")


@cli.command()
@click.option("--zones",   default=None,  help="Comma-separated zones (default: all from .env)")
@click.option("--sources", default="all", show_default=True,
              help="Sources: all | renovation | permits | linkedin")
def premarket(zones: str, sources: str):
    """Scan for pre-market property signals (renovation ads, building permits, LinkedIn).

    Detects owners likely to sell before any listing appears on the main portals.
    Results stored in the premarket_signals table and visible in the dashboard
    under the Pre-Market page.

    Signal types collected:

    \b
      Licenca de Obras    (85) -- CM Lisboa official building permits
      Anuncio Remodelacao (70) -- OLX/CustoJusto: owner seeking contractor
      Mudanca de Cidade   (60) -- LinkedIn relocation signal (via DuckDuckGo)
      Mudanca Profissional(40) -- LinkedIn career change (via DuckDuckGo)

    Examples:

    \b
      python main.py premarket
      python main.py premarket --zones Lisboa,Cascais
      python main.py premarket --sources renovation,permits
    """
    from premarket.enricher import PremktEnricher

    zone_list = [z.strip() for z in zones.split(",")] if zones else None

    console.print("[bold cyan]Pre-Market Signal Scan[/bold cyan]")
    console.print(f"  Zonas:   {zone_list or 'todas'}")
    console.print(f"  Fontes:  {sources}")
    console.print("")

    enricher = PremktEnricher()

    # Selective source loading when --sources is specified
    if sources != "all":
        src_filter = {s.strip().lower() for s in sources.split(",")}
        filtered = []
        for src in enricher._sources:
            name = type(src).__name__.lower()
            keep = (
                ("renovation" in src_filter and "renovation" in name) or
                ("permits"    in src_filter and "building"   in name) or
                ("linkedin"   in src_filter and "linkedin"   in name)
            )
            if keep:
                filtered.append(src)
        enricher._sources = filtered
        console.print(
            f"[dim]Sources active: {[type(s).__name__ for s in enricher._sources]}[/dim]\n"
        )

    with console.status("[cyan]A analisar sinais pre-mercado...[/cyan]"):
        result = enricher.run(zones=zone_list)

    # ── Summary table ─────────────────────────────────────────────────────────
    console.print("[green]Scan concluido[/green]")
    console.print(f"  Sinais encontrados:   [bold]{result.total_found}[/bold]")
    console.print(f"  Novos persistidos:    [bold green]+{result.new_signals}[/bold green]")
    console.print(f"  Duplicados ignorados: {result.skipped}")
    if result.errors:
        for err in result.errors:
            console.print(f"  [yellow]  {err}[/yellow]")

    if result.source_counts:
        console.print("")
        tbl = Table(title="Por Fonte", box=box.SIMPLE)
        tbl.add_column("Fonte",   style="cyan")
        tbl.add_column("Sinais",  justify="right", style="bold white")
        for src_name, count in result.source_counts.items():
            tbl.add_row(src_name, str(count))
        console.print(tbl)

    console.print("")
    console.print("[dim]Ver sinais: python main.py dashboard -> Pre-Market[/dim]")


@cli.command()
def dashboard():
    """Launch the Streamlit dashboard."""
    import subprocess
    dashboard_path = ROOT / "dashboard" / "app.py"
    console.print("[cyan]Launching dashboard...[/cyan]")
    console.print("[dim]Open http://localhost:8501 in your browser[/dim]")
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard_path)], check=True)


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()

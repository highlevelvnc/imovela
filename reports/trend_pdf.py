"""
Weekly trend report — generates a polished PDF summary of the last 7 days.

Designed to be e-mailed to the operator every Monday morning. Single-file
PDF with brand styling, readable on phone or desktop.

Sections (in order):
  1. Hero — week numbers (new leads, new HOT, price drops, super-sellers)
  2. Top 10 opportunities (HOT leads ranked by score)
  3. Pipeline funnel
  4. Agency leaderboard top 10
  5. Zone breakdown — count + avg score + price/m²
  6. Sources breakdown — leads per source

CLI: ``python main.py trend-report [--out path.pdf]``
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from sqlalchemy import desc, select

from reports.bi import (
    agency_leaderboard,
    conversion_funnel,
    recent_signal_summary,
    zone_heatmap_data,
)
from utils.logger import get_logger

log = get_logger(__name__)


_BRAND_PRIMARY = "#10b981"           # mint deep
_BRAND_ACCENT  = "#a78bfa"           # violet
_BRAND_BG      = "#0b1020"
_BRAND_TXT     = "#f8fafc"
_BRAND_MUTED   = "#94a3b8"


def generate_trend_report(
    output_path: Optional[Path | str] = None,
    days: int = 7,
) -> Path:
    """
    Render a single-page weekly trend report.
    Returns the absolute Path to the generated file.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    output_path = Path(output_path or _default_output_path())
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title", parent=sample["Title"],
        fontName="Helvetica-Bold", fontSize=22,
        textColor=colors.HexColor(_BRAND_PRIMARY),
        alignment=0, spaceAfter=10,
    )
    sub_style = ParagraphStyle(
        "Sub", parent=sample["Normal"],
        fontName="Helvetica", fontSize=11,
        textColor=colors.HexColor("#475569"),
        spaceAfter=14,
    )
    h2_style = ParagraphStyle(
        "H2", parent=sample["Heading2"],
        fontName="Helvetica-Bold", fontSize=14,
        textColor=colors.HexColor("#0f172a"),
        spaceBefore=14, spaceAfter=8,
    )
    body_style = ParagraphStyle(
        "Body", parent=sample["Normal"], fontName="Helvetica", fontSize=10,
        textColor=colors.HexColor("#1e293b"), leading=14,
    )

    doc = SimpleDocTemplate(
        str(output_path), pagesize=A4,
        leftMargin=1.6 * cm, rightMargin=1.6 * cm,
        topMargin=1.6 * cm, bottomMargin=1.6 * cm,
    )
    story = []

    # ── Hero ──────────────────────────────────────────────────────────────
    summary = recent_signal_summary(window_days=days)
    week_label = (
        f"Semana de {(datetime.utcnow() - timedelta(days=days)).strftime('%d %b')} "
        f"a {datetime.utcnow().strftime('%d %b %Y')}"
    )
    story += [
        Paragraph("◆ Imovela — Relatório semanal", title_style),
        Paragraph(week_label, sub_style),
    ]
    hero_data = [[
        Paragraph(f"<b>{summary['new_leads_7d']}</b><br/><font size=8 color='#475569'>NOVOS LEADS</font>", body_style),
        Paragraph(f"<b>{summary['new_hot_7d']}</b><br/><font size=8 color='#475569'>NOVOS HOT</font>", body_style),
        Paragraph(f"<b>{summary['price_drops_7d']}</b><br/><font size=8 color='#475569'>QUEDAS DE PREÇO</font>", body_style),
        Paragraph(f"<b>{summary['super_sellers']}</b><br/><font size=8 color='#475569'>SUPER-SELLERS</font>", body_style),
        Paragraph(f"<b>{summary['contacted']}</b><br/><font size=8 color='#475569'>CONTACTADOS</font>", body_style),
    ]]
    hero_tbl = Table(hero_data, colWidths=[3.5 * cm] * 5)
    hero_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f1f5f9")),
        ("BOX",        (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("INNERGRID",  (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
    ]))
    story += [hero_tbl, Spacer(1, 0.6 * cm)]

    # ── Top 10 opportunities ──────────────────────────────────────────────
    story.append(Paragraph("Top 10 oportunidades HOT", h2_style))
    top = _top_opportunities(limit=10)
    if top:
        rows = [["#", "Score", "Tipo", "Zona", "Preço (€)", "Contacto"]] + [
            [
                i + 1, str(t["score"]), (t["typology"] or "—")[:6],
                (t["zone"] or "—")[:18],
                f"{int(t['price']):,}".replace(",", " ") if t.get("price") else "—",
                "📞" if t.get("contact_phone") else ("✉" if t.get("contact_email") else "—"),
            ]
            for i, t in enumerate(top)
        ]
        tbl = Table(rows, colWidths=[0.8*cm, 1.4*cm, 2.0*cm, 4.0*cm, 3.2*cm, 1.8*cm])
        tbl.setStyle(_branded_table_style())
        story.append(tbl)
    else:
        story.append(Paragraph(
            "<i>Sem leads HOT ainda. Corre <code>python main.py run</code> para alimentar o sistema.</i>",
            body_style,
        ))

    # ── Funnel ────────────────────────────────────────────────────────────
    story += [Spacer(1, 0.5 * cm), Paragraph("Funil de conversão", h2_style)]
    fn = conversion_funnel()
    funnel_rows = [["Fase", "Leads", "% topo"]] + [
        [r["label"], r["count"], f"{r['pct_of_top']}%"] for r in fn
    ]
    tbl = Table(funnel_rows, colWidths=[6.5*cm, 3.5*cm, 3.5*cm])
    tbl.setStyle(_branded_table_style())
    story.append(tbl)

    # ── Agency leaderboard ────────────────────────────────────────────────
    story += [Spacer(1, 0.5 * cm), Paragraph("Top 10 agências por volume", h2_style)]
    leaders = agency_leaderboard(limit=10)
    if leaders:
        ag_rows = [["Agência", "Listings", "HOT", "Score méd.", "€/m² méd."]] + [
            [
                (a["agency"] or "—")[:34], a["total_listings"], a["hot_count"],
                a["avg_score"],
                f"{int(a['avg_price_per_m2']):,}".replace(",", " ") if a["avg_price_per_m2"] else "—",
            ]
            for a in leaders
        ]
        tbl = Table(ag_rows, colWidths=[7.0*cm, 2.2*cm, 1.6*cm, 2.4*cm, 2.4*cm])
        tbl.setStyle(_branded_table_style())
        story.append(tbl)
    else:
        story.append(Paragraph("<i>Nenhuma agência com dados suficientes.</i>", body_style))

    # ── Zone breakdown ────────────────────────────────────────────────────
    story += [Spacer(1, 0.5 * cm), Paragraph("Zonas com mais oportunidades", h2_style)]
    zones = sorted(zone_heatmap_data(min_count=3), key=lambda z: z["count"], reverse=True)[:15]
    if zones:
        zr = [["Zona", "Leads", "HOT", "Score méd.", "€/m² méd."]] + [
            [
                z["zone"][:24], z["count"], z["hot_count"],
                z["avg_score"],
                f"{int(z['avg_price_per_m2']):,}".replace(",", " ") if z["avg_price_per_m2"] else "—",
            ]
            for z in zones
        ]
        tbl = Table(zr, colWidths=[6.0*cm, 2.0*cm, 1.6*cm, 2.6*cm, 3.2*cm])
        tbl.setStyle(_branded_table_style())
        story.append(tbl)

    # Footer
    story += [
        Spacer(1, 0.7 * cm),
        Paragraph(
            f"<font size=8 color='#94a3b8'>Gerado por Imovela em "
            f"{datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC</font>",
            body_style,
        ),
    ]

    doc.build(story)
    log.info("[trend_pdf] report saved → {p}", p=output_path)
    return output_path


def _branded_table_style():
    from reportlab.lib import colors
    from reportlab.platypus import TableStyle
    return TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0), colors.HexColor("#0b1020")),
        ("TEXTCOLOR",   (0, 0), (-1, 0), colors.HexColor("#34d399")),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0), 9),
        ("FONTNAME",    (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",    (0, 1), (-1, -1), 9),
        ("TEXTCOLOR",   (0, 1), (-1, -1), colors.HexColor("#1e293b")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
            [colors.white, colors.HexColor("#f8fafc")]),
        ("INNERGRID",   (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ("BOX",         (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("ALIGN",       (1, 1), (-1, -1), "CENTER"),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",  (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ])


def _top_opportunities(limit: int = 10) -> list[dict]:
    from storage.database import get_db
    from storage.models import Lead
    with get_db() as db:
        rows = db.execute(
            select(Lead)
            .where(Lead.archived == False)              # noqa: E712
            .where(Lead.is_demo == False)               # noqa: E712
            .where(Lead.score_label == "HOT")
            .order_by(desc(Lead.score), desc(Lead.last_seen_at))
            .limit(limit)
        ).scalars().all()
        return [
            {
                "score":         l.score,
                "typology":      l.typology,
                "zone":          l.zone,
                "price":         l.price,
                "contact_phone": l.contact_phone,
                "contact_email": l.contact_email,
            }
            for l in rows
        ]


def _default_output_path() -> Path:
    from config.settings import settings
    return settings.data_dir / f"imovela_trend_{datetime.utcnow().strftime('%Y%m%d')}.pdf"

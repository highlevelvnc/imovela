"""
Notifier — sends alerts via email and/or Telegram.

Channels:
  - Email: via SMTP (Gmail, Mailtrap, etc.) — stdlib smtplib
  - Telegram: via Bot API — python-telegram-bot

Alerts:
  - hot_lead: fired immediately when a lead reaches HOT threshold
  - daily_report: top 20 leads sent every morning
  - price_drop: fired when a tracked lead's price drops
"""
from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import requests

from config.settings import settings
from storage.database import get_db
from storage.models import Lead
from storage.repository import AlertRepo, LeadRepo
from utils.logger import get_logger

log = get_logger(__name__)


class Notifier:

    def send_hot_lead_alert(self, lead: Lead) -> bool:
        """Send immediate alert for a newly detected HOT lead."""
        subject = f"[ImoScrapping] 🔴 HOT Lead — {lead.score}pts | {lead.typology} {lead.zone} | {self._format_price(lead.price)}"
        body = self._build_hot_lead_body(lead)

        success = True
        if settings.alert_email_enabled:
            ok = self._send_email(subject, body)
            success = success and ok

        if settings.alert_telegram_enabled:
            ok = self._send_telegram(self._build_telegram_hot_lead(lead))
            success = success and ok

        if not settings.alert_email_enabled and not settings.alert_telegram_enabled:
            log.info("[ALERT] {subject}", subject=subject)
            log.info("[ALERT] Lead {id}: {body}", id=lead.id, body=body[:200])

        with get_db() as db:
            AlertRepo(db).log(
                alert_type="hot_lead",
                channel="email+telegram",
                lead_id=lead.id,
                payload={"score": lead.score, "zone": lead.zone, "price": lead.price},
                success=success,
            )

        return success

    def send_daily_report(self, leads: List[Lead]) -> bool:
        """Send the daily top-20 opportunities report."""
        subject = f"📊 LeadEngine — Relatório Diário | {len(leads)} oportunidades"
        body = self._build_daily_report_body(leads)

        success = True
        if settings.alert_email_enabled:
            ok = self._send_email(subject, body, html=True)
            success = success and ok

        if settings.alert_telegram_enabled:
            ok = self._send_telegram(self._build_telegram_daily(leads))
            success = success and ok

        if not settings.alert_email_enabled and not settings.alert_telegram_enabled:
            log.info("[DAILY REPORT] Top {n} leads ready — check dashboard", n=len(leads))

        with get_db() as db:
            AlertRepo(db).log(
                alert_type="daily_report",
                channel="email+telegram",
                payload={"count": len(leads)},
                success=success,
            )

        return success

    def send_price_drop_alert(self, lead: Lead, old_price: float, new_price: float) -> bool:
        """Alert when a tracked lead's price drops."""
        drop_pct = abs((new_price - old_price) / old_price * 100)
        subject = f"📉 Redução de preço — {lead.typology} {lead.zone} | -{drop_pct:.1f}%"
        msg = (
            f"Preço reduzido em {drop_pct:.1f}%\n"
            f"Antes: {self._format_price(old_price)}\n"
            f"Agora: {self._format_price(new_price)}\n"
            f"Score actual: {lead.score}pts\n"
            f"Ver: {self._get_lead_url(lead)}"
        )

        if settings.alert_telegram_enabled:
            self._send_telegram(f"📉 {subject}\n\n{msg}")

        if settings.alert_email_enabled:
            self._send_email(subject, msg)

        if not settings.alert_email_enabled and not settings.alert_telegram_enabled:
            log.info("[PRICE DROP] {msg}", msg=msg)

        with get_db() as db:
            AlertRepo(db).log(
                alert_type="price_drop",
                channel="email+telegram",
                lead_id=lead.id,
                payload={"old_price": old_price, "new_price": new_price, "drop_pct": drop_pct},
            )

        return True

    def check_and_alert_hot_leads(self) -> int:
        """Check for new HOT leads not yet alerted today. Returns count alerted."""
        alerted = 0
        with get_db() as db:
            lead_repo = LeadRepo(db)
            alert_repo = AlertRepo(db)
            hot_leads = lead_repo.get_hot_leads(threshold=settings.hot_score_threshold)

            for lead in hot_leads:
                if alert_repo.already_alerted_today(lead.id, "hot_lead"):
                    continue
                self.send_hot_lead_alert(lead)
                alerted += 1

        log.info("Hot lead check — {n} alerts sent", n=alerted)
        return alerted

    # ── Email ─────────────────────────────────────────────────────────────────

    def _send_email(self, subject: str, body: str, html: bool = False) -> bool:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = settings.smtp_user
            msg["To"] = settings.alert_email_to

            part = MIMEText(body, "html" if html else "plain", "utf-8")
            msg.attach(part)

            with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(settings.smtp_user, settings.smtp_password)
                server.sendmail(settings.smtp_user, settings.alert_email_to, msg.as_string())

            log.info("Email sent: {subject}", subject=subject)
            return True

        except Exception as e:
            log.error("Email failed: {e}", e=e)
            return False

    # ── Telegram ─────────────────────────────────────────────────────────────

    def _send_telegram(self, text: str) -> bool:
        try:
            url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
            payload = {
                "chat_id": settings.telegram_chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            log.info("Telegram message sent")
            return True
        except Exception as e:
            log.error("Telegram failed: {e}", e=e)
            return False

    # ── Message builders ──────────────────────────────────────────────────────

    def _build_hot_lead_body(self, lead: Lead) -> str:
        lines = [
            f"NOVO HOT LEAD — Score {lead.score}/100 ({lead.score_label})",
            "─" * 40,
            f"Imóvel:    {lead.title}",
            f"Tipologia: {lead.typology} | Área: {lead.area_m2 or '?'} m²",
            f"Preço:     {self._format_price(lead.price)}",
            f"Zona:      {lead.zone}",
            f"Endereço:  {lead.address or '—'}",
            "",
            f"Proprietário directo: {'Sim' if lead.is_owner else 'Não'}",
            f"Contacto:  {lead.contact_phone or lead.contact_email or '—'}",
            f"Dias no mercado: {lead.days_on_market}",
            "",
            f"Delta vs mercado: {lead.price_delta_pct:+.1f}%" if lead.price_delta_pct else "",
            "",
            f"Descrição:\n{(lead.description or '')[:300]}",
            "",
            f"Ver anúncio: {self._get_lead_url(lead)}",
        ]
        return "\n".join(l for l in lines if l)

    def _build_telegram_hot_lead(self, lead: Lead) -> str:
        delta = f" ({lead.price_delta_pct:+.1f}% vs mercado)" if lead.price_delta_pct else ""
        return (
            f"🔴 <b>HOT LEAD — {lead.score}pts</b>\n\n"
            f"📍 {lead.typology} | {lead.zone}\n"
            f"💶 {self._format_price(lead.price)}{delta}\n"
            f"📐 {lead.area_m2 or '?'} m²\n"
            f"👤 {'Proprietário' if lead.is_owner else 'Agência'}\n"
            f"📞 {lead.contact_phone or '—'}\n"
            f"⏱ {lead.days_on_market} dias no mercado\n\n"
            f"🔗 {self._get_lead_url(lead)}"
        )

    def _build_telegram_daily(self, leads: List[Lead]) -> str:
        lines = ["📊 <b>Relatório Diário — Top Oportunidades</b>\n"]
        for i, lead in enumerate(leads[:10], 1):
            label_icon = "🔴" if lead.score_label == "HOT" else "🟡"
            lines.append(
                f"{label_icon} {i}. <b>{lead.score}pts</b> | "
                f"{lead.typology} {lead.zone} | {self._format_price(lead.price)}"
            )
        lines.append(f"\nTotal: {len(leads)} oportunidades. Ver dashboard para detalhes.")
        return "\n".join(lines)

    def _build_daily_report_body(self, leads: List[Lead]) -> str:
        rows = ""
        for i, lead in enumerate(leads[:20], 1):
            label_color = "#d32f2f" if lead.score_label == "HOT" else "#f57c00" if lead.score_label == "WARM" else "#1565c0"
            delta_str = f"{lead.price_delta_pct:+.1f}%" if lead.price_delta_pct else "—"
            rows += f"""
            <tr>
                <td>{i}</td>
                <td><b style="color:{label_color}">{lead.score}</b></td>
                <td>{lead.typology}</td>
                <td>{lead.zone}</td>
                <td>{self._format_price(lead.price)}</td>
                <td>{delta_str}</td>
                <td>{lead.days_on_market}d</td>
                <td>{'✓' if lead.is_owner else ''}</td>
            </tr>"""

        return f"""
        <html><body style="font-family:Arial,sans-serif">
        <h2>📊 LeadEngine — Relatório Diário</h2>
        <p>Top {min(20, len(leads))} de {len(leads)} oportunidades hoje.</p>
        <table border="1" cellpadding="8" style="border-collapse:collapse;width:100%">
            <thead style="background:#1a237e;color:white">
                <tr><th>#</th><th>Score</th><th>Tipo</th><th>Zona</th>
                <th>Preço</th><th>vs Mercado</th><th>Dias</th><th>Owner</th></tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        </body></html>"""

    # ── Utils ─────────────────────────────────────────────────────────────────

    def _format_price(self, price: Optional[float]) -> str:
        if not price:
            return "—"
        return f"{price:,.0f} €".replace(",", ".")

    def _get_lead_url(self, lead: Lead) -> str:
        sources = lead.sources
        if sources:
            return sources[0].get("url", "—")
        return "—"

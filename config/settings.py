"""
Central settings — loaded from .env file.
All modules import `settings` from here.
Designed for easy migration: change DATABASE_URL to PostgreSQL in .env only.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Application ──────────────────────────────────────────────────────────
    app_name: str = "Imovela"
    app_tagline: str = "Lead intelligence imobiliária"
    app_env: str = "development"
    log_level: str = "INFO"
    log_file: str = "logs/imovela.log"

    # Outreach message signature — appended to auto-drafted contact
    # messages in the dashboard. Per-tenant configuration.
    contact_signature: str = "Cumprimentos"

    # ── Database ─────────────────────────────────────────────────────────────
    database_url: str = f"sqlite:///{ROOT_DIR}/data/imoscrapping.db"

    # ── Proxy ────────────────────────────────────────────────────────────────
    use_proxies: bool = False
    proxy_list: str = ""

    @property
    def proxies(self) -> List[str]:
        if not self.proxy_list:
            return []
        return [p.strip() for p in self.proxy_list.split(",") if p.strip()]

    # ── Scraping ─────────────────────────────────────────────────────────────
    scrape_delay_min: float = 2.0
    scrape_delay_max: float = 6.0
    max_retries: int = 3
    request_timeout: int = 30
    headless_browser: bool = True

    # ── Zones ────────────────────────────────────────────────────────────────
    target_zones: str = "Lisboa,Cascais,Sintra,Almada,Seixal,Sesimbra"

    @property
    def zones(self) -> List[str]:
        return [z.strip() for z in self.target_zones.split(",") if z.strip()]

    # ── Scheduler ────────────────────────────────────────────────────────────
    schedule_time: str = "08:00"
    schedule_enabled: bool = True

    # ── Scoring ──────────────────────────────────────────────────────────────
    hot_score_threshold: int = 60
    warm_score_threshold: int = 40

    # ── Email Alerts ─────────────────────────────────────────────────────────
    alert_email_enabled: bool = False
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_email_to: str = ""

    # ── Telegram Alerts ──────────────────────────────────────────────────────
    alert_telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # ── Derived paths ────────────────────────────────────────────────────────
    @property
    def data_dir(self) -> Path:
        p = ROOT_DIR / "data"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def logs_dir(self) -> Path:
        p = ROOT_DIR / "logs"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


settings = Settings()

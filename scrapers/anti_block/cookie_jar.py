"""
Persistent cookie jar — sessions survive across runs.

Why
---
Anti-bot systems (Incapsula, DataDome, Cloudflare) hand out a hard-won
"clear" cookie after JS challenges, geolocation hints, or form submissions.
Tossing that cookie at the end of every run means we re-do the JS challenge
on the next run, which often triggers a stricter follow-up challenge.

This module persists cookie jars per-source to ``data/cookies/<source>.json``
and exposes:

  * ``load_into_client(client, source)``  — populate an httpx.Client
  * ``save_from_client(client, source)``  — extract + persist
  * ``load_into_playwright_context(context, source)`` /
    ``save_from_playwright_context(context, source)`` for browser-based scrapers

Idempotent. Missing files are handled silently. Old cookies are pruned
when over MAX_AGE_DAYS so we don't cling to stale anti-bot tokens.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import httpx

from utils.logger import get_logger

log = get_logger(__name__)

ROOT_DIR  = Path(__file__).resolve().parent.parent.parent
COOKIE_DIR = ROOT_DIR / "data" / "cookies"

# Cookies older than this are dropped on read — Incapsula/DataDome typically
# rotate signing keys every few days, so re-using week-old tokens is futile.
MAX_AGE_DAYS: int = 5


def _path_for(source: str) -> Path:
    safe = source.replace("/", "_").replace(" ", "_").lower()
    return COOKIE_DIR / f"{safe}.json"


def _ensure_dir() -> None:
    COOKIE_DIR.mkdir(parents=True, exist_ok=True)


def _is_stale(saved_at: float) -> bool:
    return (time.time() - saved_at) > MAX_AGE_DAYS * 86_400


# ── httpx.Client ─────────────────────────────────────────────────────────────

def save_from_client(client: "httpx.Client", source: str) -> None:
    """Persist every cookie currently in ``client.cookies``."""
    try:
        _ensure_dir()
        cookies: list[dict[str, Any]] = []
        for cookie in client.cookies.jar:
            cookies.append({
                "name":   cookie.name,
                "value":  cookie.value,
                "domain": cookie.domain,
                "path":   cookie.path,
                "expires": cookie.expires,
                "secure": cookie.secure,
            })
        payload = {"saved_at": time.time(), "cookies": cookies}
        _path_for(source).write_text(json.dumps(payload, ensure_ascii=False))
        log.debug("[cookie_jar] {src} → saved {n} cookies", src=source, n=len(cookies))
    except Exception as e:
        log.debug("[cookie_jar] save error for {src}: {e}", src=source, e=e)


def load_into_client(client: "httpx.Client", source: str) -> bool:
    """
    Replay cookies into a fresh httpx.Client.
    Returns True when at least one cookie was reused, False otherwise.
    """
    path = _path_for(source)
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text())
        if _is_stale(payload.get("saved_at", 0)):
            log.debug("[cookie_jar] {src} cookies stale — discarding", src=source)
            return False
        cookies = payload.get("cookies", [])
        for c in cookies:
            client.cookies.set(
                name=c["name"], value=c["value"],
                domain=c["domain"], path=c.get("path", "/"),
            )
        log.debug("[cookie_jar] {src} ← loaded {n} cookies",
                  src=source, n=len(cookies))
        return bool(cookies)
    except Exception as e:
        log.debug("[cookie_jar] load error for {src}: {e}", src=source, e=e)
        return False


# ── Playwright BrowserContext ────────────────────────────────────────────────

async def save_from_playwright_context(context, source: str) -> None:
    """Async — persist Playwright storage_state to disk."""
    try:
        _ensure_dir()
        state = await context.storage_state()
        payload = {"saved_at": time.time(), "playwright_state": state}
        _path_for(source).write_text(json.dumps(payload, ensure_ascii=False))
        log.debug("[cookie_jar] {src} → saved Playwright state", src=source)
    except Exception as e:
        log.debug("[cookie_jar] pw save error for {src}: {e}", src=source, e=e)


def load_into_playwright_state(source: str) -> Optional[str]:
    """
    Return a path to a temporary file with the Playwright ``storage_state``
    payload, ready to pass into ``browser.new_context(storage_state=...)``.
    Returns None when no fresh state exists.
    """
    path = _path_for(source)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        if _is_stale(payload.get("saved_at", 0)):
            return None
        state = payload.get("playwright_state")
        if not state:
            return None
        # Playwright accepts the bare storage_state JSON — write to a tmp
        # file inside the same dir so it gets cleaned up next purge_stale().
        tmp = COOKIE_DIR / f"_pw_state_{source}.json"
        tmp.write_text(json.dumps(state, ensure_ascii=False))
        return str(tmp)
    except Exception as e:
        log.debug("[cookie_jar] pw load error for {src}: {e}", src=source, e=e)
        return None


# ── Maintenance ──────────────────────────────────────────────────────────────

def purge_stale() -> int:
    """Delete every persisted cookie file older than MAX_AGE_DAYS."""
    if not COOKIE_DIR.exists():
        return 0
    removed = 0
    for f in COOKIE_DIR.glob("*.json"):
        try:
            payload = json.loads(f.read_text())
            if _is_stale(payload.get("saved_at", 0)):
                f.unlink()
                removed += 1
        except Exception:
            pass
    return removed

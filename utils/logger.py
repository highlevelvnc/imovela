"""
Centralized logging via loguru.
All modules call: from utils import get_logger; log = get_logger(__name__)
"""
import sys
from pathlib import Path

from loguru import logger

from config.settings import settings

_configured = False


def _configure_logger() -> None:
    global _configured
    if _configured:
        return

    logger.remove()  # remove default stderr handler

    # Console — coloured, human-readable
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File — full detail, rotated daily, kept 7 days
    log_path = Path(settings.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} — {message}",
        rotation="00:00",      # new file every midnight
        retention="7 days",
        compression="zip",
        encoding="utf-8",
    )

    _configured = True


def get_logger(name: str):
    _configure_logger()
    return logger.bind(name=name)

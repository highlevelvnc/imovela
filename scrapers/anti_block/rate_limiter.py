"""
Token-bucket rate limiter + human-like delay between requests.
Each scraper gets its own limiter instance tuned for that source.
"""
from __future__ import annotations

import random
import time
from threading import Lock

from utils.logger import get_logger

log = get_logger(__name__)


class RateLimiter:
    """
    Combines:
    - Minimum delay between requests (random within [min, max])
    - Per-minute cap (token bucket)
    - Adaptive backoff on error responses
    """

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 6.0,
        max_per_minute: int = 15,
        source: str = "unknown",
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_per_minute = max_per_minute
        self.source = source

        self._lock = Lock()
        self._last_request_time: float = 0.0
        self._request_times: list[float] = []  # timestamps of last 60s

    def wait(self) -> None:
        """Call before each request. Blocks until it's safe to proceed."""
        with self._lock:
            self._purge_old_timestamps()

            # Token bucket: pause if we've hit the per-minute cap
            if len(self._request_times) >= self.max_per_minute:
                oldest = self._request_times[0]
                wait_needed = 60.0 - (time.monotonic() - oldest)
                if wait_needed > 0:
                    log.debug(
                        "[{src}] Rate cap reached — waiting {wait:.1f}s",
                        src=self.source, wait=wait_needed,
                    )
                    time.sleep(wait_needed)
                    self._purge_old_timestamps()

            # Human-like delay since last request
            elapsed = time.monotonic() - self._last_request_time
            delay = random.uniform(self.min_delay, self.max_delay)
            if elapsed < delay:
                sleep_for = delay - elapsed
                log.debug(
                    "[{src}] Waiting {sleep:.2f}s before next request",
                    src=self.source, sleep=sleep_for,
                )
                time.sleep(sleep_for)

            self._last_request_time = time.monotonic()
            self._request_times.append(self._last_request_time)

    def backoff(self, attempt: int) -> None:
        """Exponential backoff after a failed request."""
        delay = min(self.min_delay * (2 ** attempt), 120.0)
        jitter = random.uniform(0, delay * 0.2)
        total = delay + jitter
        log.warning(
            "[{src}] Backoff attempt {attempt} — sleeping {total:.1f}s",
            src=self.source, attempt=attempt, total=total,
        )
        time.sleep(total)

    def _purge_old_timestamps(self) -> None:
        cutoff = time.monotonic() - 60.0
        self._request_times = [t for t in self._request_times if t > cutoff]

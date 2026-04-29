"""
Adaptive rate limiter — token bucket + human-like delay + automatic
recovery on 429/403 / connection errors.

Architecture
------------
Each scraper gets its own ``RateLimiter`` instance. The limiter tracks:

  * Last-request timestamp + per-minute token bucket (existing behaviour)
  * Recent error rate sliding window (NEW): every call to ``backoff()``
    is recorded; when the error rate exceeds ``ERROR_RATE_TRIGGER`` over
    ``ERROR_RATE_WINDOW`` requests, the limiter auto-doubles its baseline
    delay range and refuses to go faster until the error rate drops.

Effect: a steady drip of 429s no longer just retries the same offending
cadence — the scraper visibly slows down for the duration of the storm,
which by itself recovers most rate-limit problems without the operator
needing to intervene or rotate IPs.

Throttle states
---------------
  NORMAL    — baseline (min_delay, max_delay)
  CAUTIOUS  — 2x baseline (entered after error rate >= 25%)
  COOLOFF   — 4x baseline + circuit-break sleep of 5-15 min once

Transitions are logged at INFO level. State automatically degrades back
to NORMAL after a clean RECOVERY_WINDOW of successful requests.
"""
from __future__ import annotations

import random
import time
from collections import deque
from threading import Lock

from utils.logger import get_logger

log = get_logger(__name__)

# Rolling window: count of recent requests considered for error-rate calc
ERROR_RATE_WINDOW: int = 20
# Error fraction above which the limiter shifts to CAUTIOUS state
ERROR_RATE_TRIGGER: float = 0.25
# Error fraction above which the limiter shifts to COOLOFF state
COOLOFF_TRIGGER: float = 0.50
# Number of consecutive successes required to drop one severity level
RECOVERY_WINDOW: int = 12
# Hard cool-off pause when entering COOLOFF state (seconds)
COOLOFF_PAUSE_RANGE: tuple[float, float] = (300.0, 900.0)


class RateLimiter:
    """
    Adaptive limiter with throttle-state machine.

    Public methods used by scrapers:
      wait()              — block until safe to issue next request
      backoff(attempt)    — record an error, exponentially back off, and
                            potentially upgrade the throttle state
      record_success()    — call after a successful response so the limiter
                            can drop back to NORMAL
    """

    NORMAL    = "NORMAL"
    CAUTIOUS  = "CAUTIOUS"
    COOLOFF   = "COOLOFF"

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 6.0,
        max_per_minute: int = 15,
        source: str = "unknown",
    ):
        self._base_min = min_delay
        self._base_max = max_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_per_minute = max_per_minute
        self.source = source

        self._lock = Lock()
        self._last_request_time: float = 0.0
        self._request_times: list[float] = []           # last-60s timestamps
        # Rolling window of {True=success, False=error}
        self._outcomes: deque[bool] = deque(maxlen=ERROR_RATE_WINDOW)
        self._consecutive_ok: int = 0
        self._state: str = self.NORMAL

    # ── Public API ────────────────────────────────────────────────────────

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

            # Human-like delay since last request — uses CURRENT state range
            elapsed = time.monotonic() - self._last_request_time
            delay = random.uniform(self.min_delay, self.max_delay)
            if elapsed < delay:
                time.sleep(delay - elapsed)

            self._last_request_time = time.monotonic()
            self._request_times.append(self._last_request_time)

    def backoff(self, attempt: int) -> None:
        """
        Record an error AND apply exponential backoff for this single retry.

        Side-effects beyond the sleep:
          1. Pushes a ``False`` into the outcomes window
          2. Resets the consecutive-success counter
          3. May escalate the throttle state to CAUTIOUS / COOLOFF
        """
        self._outcomes.append(False)
        self._consecutive_ok = 0
        self._maybe_escalate()

        delay = min(self._base_min * (2 ** attempt), 120.0)
        jitter = random.uniform(0, delay * 0.2)
        total = delay + jitter
        log.warning(
            "[{src}] Backoff attempt={a} state={st} sleep={t:.1f}s",
            src=self.source, a=attempt, st=self._state, t=total,
        )
        time.sleep(total)

    def record_success(self) -> None:
        """Record a 2xx response; potentially de-escalates throttle state."""
        self._outcomes.append(True)
        self._consecutive_ok += 1
        self._maybe_recover()

    @property
    def state(self) -> str:
        return self._state

    # ── Throttle state machine ────────────────────────────────────────────

    def _maybe_escalate(self) -> None:
        """Promote NORMAL→CAUTIOUS or CAUTIOUS→COOLOFF based on error rate."""
        if len(self._outcomes) < 4:
            return
        error_rate = self._outcomes.count(False) / len(self._outcomes)

        if error_rate >= COOLOFF_TRIGGER and self._state != self.COOLOFF:
            self._enter_state(self.COOLOFF, error_rate)
            pause = random.uniform(*COOLOFF_PAUSE_RANGE)
            log.warning(
                "[{src}] COOLOFF circuit-break — pausing {p:.0f}s",
                src=self.source, p=pause,
            )
            time.sleep(pause)
            return

        if error_rate >= ERROR_RATE_TRIGGER and self._state == self.NORMAL:
            self._enter_state(self.CAUTIOUS, error_rate)

    def _maybe_recover(self) -> None:
        """Drop one severity level after RECOVERY_WINDOW clean successes."""
        if self._state == self.NORMAL:
            return
        if self._consecutive_ok < RECOVERY_WINDOW:
            return
        if self._state == self.COOLOFF:
            self._enter_state(self.CAUTIOUS, 0.0)
        elif self._state == self.CAUTIOUS:
            self._enter_state(self.NORMAL, 0.0)

    def _enter_state(self, new_state: str, error_rate: float) -> None:
        prev_state = self._state
        self._state = new_state
        if new_state == self.NORMAL:
            self.min_delay, self.max_delay = self._base_min, self._base_max
        elif new_state == self.CAUTIOUS:
            self.min_delay = self._base_min * 2
            self.max_delay = self._base_max * 2
        else:  # COOLOFF
            self.min_delay = self._base_min * 4
            self.max_delay = self._base_max * 4
        log.info(
            "[{src}] throttle state {prev}→{new} (error_rate={er:.0%}) "
            "delays={lo:.1f}-{hi:.1f}s",
            src=self.source, prev=prev_state, new=new_state,
            er=error_rate, lo=self.min_delay, hi=self.max_delay,
        )

    def _purge_old_timestamps(self) -> None:
        cutoff = time.monotonic() - 60.0
        self._request_times = [t for t in self._request_times if t > cutoff]

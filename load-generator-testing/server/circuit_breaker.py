"""Circuit breaker state machine for overload protection."""

import asyncio
import time
from enum import Enum


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker with three states: CLOSED, OPEN, HALF_OPEN.

    - CLOSED: All requests pass through. If the error rate exceeds
      `error_threshold` within the sliding window (and at least
      `min_requests` have been recorded), the breaker trips to OPEN.
    - OPEN: All requests are rejected. After `cooldown_secs` the
      breaker transitions to HALF_OPEN.
    - HALF_OPEN: Requests are allowed through as probes. If
      `recovery_successes` consecutive successes are recorded the
      breaker returns to CLOSED. Any single failure sends it back
      to OPEN.
    """

    def __init__(
        self,
        error_threshold: float = 0.5,
        min_requests: int = 100,
        cooldown_secs: float = 10.0,
        recovery_successes: int = 5,
        window_secs: float = 60.0,
    ) -> None:
        self.error_threshold = error_threshold
        self.min_requests = min_requests
        self.cooldown_secs = cooldown_secs
        self.recovery_successes = recovery_successes
        self.window_secs = window_secs

        self.state = CircuitState.CLOSED
        self._requests: list[tuple[float, bool]] = []  # (timestamp, is_success)
        self._open_since: float = 0.0
        self._half_open_successes: int = 0
        self._lock = asyncio.Lock()

    async def check(self) -> bool:
        """Check if a request should be allowed. Returns True if allowed."""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.monotonic() - self._open_since >= self.cooldown_secs:
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
                    return True
                return False
            else:  # HALF_OPEN
                return True

    async def record_success(self) -> None:
        """Record a successful request."""
        async with self._lock:
            now = time.monotonic()
            self._requests.append((now, True))
            self._prune_window(now)

            if self.state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.recovery_successes:
                    self.state = CircuitState.CLOSED

    async def record_failure(self) -> None:
        """Record a failed request."""
        async with self._lock:
            now = time.monotonic()
            self._requests.append((now, False))
            self._prune_window(now)

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self._open_since = now
            elif self.state == CircuitState.CLOSED:
                self._check_threshold()

    def _prune_window(self, now: float) -> None:
        """Remove entries older than the sliding window."""
        cutoff = now - self.window_secs
        self._requests = [(t, s) for t, s in self._requests if t >= cutoff]

    def _check_threshold(self) -> None:
        """Trip the breaker to OPEN if the error rate exceeds the threshold."""
        if len(self._requests) < self.min_requests:
            return
        failures = sum(1 for _, s in self._requests if not s)
        error_rate = failures / len(self._requests)
        if error_rate >= self.error_threshold:
            self.state = CircuitState.OPEN
            self._open_since = time.monotonic()

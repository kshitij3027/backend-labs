"""Per-IP rate limiting using fixed-window counters."""

import time
import threading


class TokenBucket:
    """Fixed-window counter for a single client."""

    def __init__(self, max_requests: int, window_seconds: int, time_func=None):
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._time_func = time_func or time.monotonic
        self._count = 0
        self._window_start = self._time_func()

    def allow(self) -> bool:
        """Return True if the request is allowed, False if rate-limited."""
        now = self._time_func()
        if now - self._window_start >= self._window_seconds:
            self._window_start = now
            self._count = 0

        self._count += 1
        return self._count <= self._max_requests


class RateLimiter:
    """Manages per-IP rate limiting with thread-safe bucket access."""

    def __init__(self, enabled: bool, max_requests: int, window_seconds: int,
                 time_func=None):
        self._enabled = enabled
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._time_func = time_func
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def allow(self, client_ip: str) -> bool:
        """Check if a request from client_ip is allowed."""
        if not self._enabled:
            return True

        with self._lock:
            if client_ip not in self._buckets:
                self._buckets[client_ip] = TokenBucket(
                    self._max_requests,
                    self._window_seconds,
                    self._time_func,
                )
            return self._buckets[client_ip].allow()

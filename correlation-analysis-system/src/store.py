"""Graceful-degradation Redis wrapper: the pipeline's shared-state mirror.

Redis is an *enhancement*, never a hard dependency: every operation is wrapped so
a dead or unreachable Redis degrades to a no-op (returning ``None``/``False``)
instead of raising — the pipeline and the API keep serving from in-memory state.
Failures log ONE warning per outage (and one info line on recovery), never one
per call, so a long Redis outage cannot flood the logs at 1000 events/sec.

Key schema (everything carries the ``corr:`` prefix; the registry below is the
single source of truth and later commits extend it with correlation, pattern and
alert keys):

    corr:events:recent   LIST — newest-first LogEvent JSON, capped at 1000
"""

from __future__ import annotations

import logging
import time

import redis

from src.models import LogEvent

logger = logging.getLogger(__name__)

# --- Key registry (C4+ adds correlation/pattern/alert keys here) --------------
KEY_EVENTS_RECENT = "corr:events:recent"

#: Hard cap on the mirrored recent-events list (the LTRIM bound).
EVENTS_RECENT_MAX = 1000

#: How long :attr:`RedisStore.available` trusts the last probe before pinging
#: Redis again (avoids a ping per /health request).
_AVAILABILITY_TTL_SECONDS = 5.0


class RedisStore:
    """Thin pipelined Redis client that can never crash the app.

    Every public method is a best-effort write/probe: on any Redis/socket error
    it records the outage, logs once per failure burst, and returns a harmless
    default. Short socket timeouts (2s) bound how long a dead Redis can stall
    the pipeline tick.
    """

    def __init__(self, redis_url: str) -> None:
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        # Optimistic start: the first failed op flips this and logs the warning.
        self._healthy = True
        #: time.monotonic() of the last ping probe (0.0 forces the first probe).
        self._last_ping = 0.0

    # --- Availability -----------------------------------------------------------
    @property
    def available(self) -> bool:
        """Last known availability, re-probed lazily at most every 5 seconds."""
        if time.monotonic() - self._last_ping >= _AVAILABILITY_TTL_SECONDS:
            self.ping()
        return self._healthy

    def ping(self) -> bool:
        """True when Redis answers PING; False on any failure (never raises)."""
        self._last_ping = time.monotonic()
        try:
            ok = bool(self._client.ping())
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("ping", exc)
            return False
        self._mark_success()
        return ok

    # --- Writes -------------------------------------------------------------------
    def push_recent_logs(self, events: list[LogEvent]) -> None:
        """Mirror newly parsed events into the capped ``corr:events:recent`` list.

        One pipelined round-trip per tick: a single LPUSH of the whole batch
        (arguments in chronological order, so the newest event lands at index 0)
        followed by an LTRIM to the 1000-entry cap.
        """
        if not events:
            return
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.lpush(KEY_EVENTS_RECENT, *(ev.model_dump_json() for ev in events))
            pipe.ltrim(KEY_EVENTS_RECENT, 0, EVENTS_RECENT_MAX - 1)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("push_recent_logs", exc)
            return
        self._mark_success()

    # --- Outage bookkeeping (one log line per burst, not per op) -------------------
    def _mark_failure(self, op: str, exc: Exception) -> None:
        if self._healthy:
            logger.warning(
                "redis unavailable (%s: %s) — degrading to in-memory only", op, exc
            )
        self._healthy = False

    def _mark_success(self) -> None:
        if not self._healthy:
            logger.info("redis connection recovered")
        self._healthy = True

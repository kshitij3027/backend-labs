"""Graceful-degradation Redis wrapper: the pipeline's shared-state mirror.

Redis is an *enhancement*, never a hard dependency: every operation is wrapped so
a dead or unreachable Redis degrades to a no-op (returning ``None``/``False``)
instead of raising — the pipeline and the API keep serving from in-memory state.
Failures log ONE warning per outage (and one info line on recovery), never one
per call, so a long Redis outage cannot flood the logs at 1000 events/sec.

Key schema (everything carries the ``corr:`` prefix; the registry below is the
single source of truth and later commits extend it with pattern and alert keys):

    corr:events:recent                LIST — newest-first LogEvent JSON, capped at 1000
    corr:correlations:recent          LIST — newest-first Correlation JSON, capped at 2000
    corr:correlations:by_type:{type}  LIST — newest-first per-type Correlation JSON, capped at 500
    corr:stats                        HASH — total, per-type ``type:{t}`` counts, strength_sum
    corr:alerts:recent                LIST — newest-first Alert JSON, capped at 200
    corr:alerts:channel               PUB/SUB — every fresh Alert JSON, fanned out live
"""

from __future__ import annotations

import logging
import time

import redis

from src.models import Alert, Correlation, LogEvent

logger = logging.getLogger(__name__)

# --- Key registry (C7 adds pattern keys here) ----------------------------------
KEY_EVENTS_RECENT = "corr:events:recent"
KEY_CORRELATIONS_RECENT = "corr:correlations:recent"
#: Template — ``.format(type=...)`` with a CorrelationType value.
KEY_CORRELATIONS_BY_TYPE = "corr:correlations:by_type:{type}"
KEY_STATS = "corr:stats"
KEY_ALERTS_RECENT = "corr:alerts:recent"
#: Pub/sub channel every fresh alert is PUBLISHed to (live subscribers see
#: alerts the moment they fire, without polling the list).
CHANNEL_ALERTS = "corr:alerts:channel"

#: Hard cap on the mirrored recent-events list (the LTRIM bound).
EVENTS_RECENT_MAX = 1000
#: Hard cap on the mirrored recent-correlations list (matches the engine deque).
CORRELATIONS_RECENT_MAX = 2000
#: Hard cap on each per-type correlation list.
CORRELATIONS_BY_TYPE_MAX = 500
#: Hard cap on the mirrored recent-alerts list (matches the AlertManager deque).
ALERTS_RECENT_MAX = 200

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

    def push_correlations(self, corrs: list[Correlation]) -> None:
        """Mirror newly detected correlations into the capped Redis lists.

        One pipelined round-trip per detection cycle: LPUSH + LTRIM on the
        global ``corr:correlations:recent`` list (cap 2000), plus one
        LPUSH + LTRIM per correlation type present in the batch
        (``corr:correlations:by_type:{type}``, cap 500 each). Each correlation
        is serialized exactly once and pushed in chronological order, so the
        newest lands at index 0 everywhere.
        """
        if not corrs:
            return
        payloads = [(corr.correlation_type.value, corr.model_dump_json()) for corr in corrs]
        by_type: dict[str, list[str]] = {}
        for type_value, payload in payloads:
            by_type.setdefault(type_value, []).append(payload)
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.lpush(KEY_CORRELATIONS_RECENT, *(payload for _, payload in payloads))
            pipe.ltrim(KEY_CORRELATIONS_RECENT, 0, CORRELATIONS_RECENT_MAX - 1)
            for type_value, items in by_type.items():
                key = KEY_CORRELATIONS_BY_TYPE.format(type=type_value)
                pipe.lpush(key, *items)
                pipe.ltrim(key, 0, CORRELATIONS_BY_TYPE_MAX - 1)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("push_correlations", exc)
            return
        self._mark_success()

    def incr_stats(self, corrs: list[Correlation]) -> None:
        """Advance the ``corr:stats`` hash counters for a batch of correlations.

        Fields: ``total`` (HINCRBY), one ``type:{t}`` count per type present
        (HINCRBY), and ``strength_sum`` (HINCRBYFLOAT) — enough for readers to
        rebuild ``avg_strength`` without replaying the lists. One pipelined
        round-trip, same graceful degradation as every other write.
        """
        if not corrs:
            return
        type_counts: dict[str, int] = {}
        for corr in corrs:
            type_value = corr.correlation_type.value
            type_counts[type_value] = type_counts.get(type_value, 0) + 1
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.hincrby(KEY_STATS, "total", len(corrs))
            for type_value, count in type_counts.items():
                pipe.hincrby(KEY_STATS, f"type:{type_value}", count)
            pipe.hincrbyfloat(KEY_STATS, "strength_sum", float(sum(c.strength for c in corrs)))
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("incr_stats", exc)
            return
        self._mark_success()

    def push_alerts(self, alerts: list[Alert]) -> None:
        """Mirror fresh alerts into the capped list and PUBLISH each to the channel.

        One pipelined round-trip per alert batch: a single LPUSH of the whole
        batch (chronological order, newest at index 0) + LTRIM to the 200-entry
        cap on ``corr:alerts:recent``, then one PUBLISH per alert on
        ``corr:alerts:channel`` so live subscribers (tests, future dashboards)
        receive each alert the moment it fires. Each alert is serialized
        exactly once; same graceful degradation as every other write.
        """
        if not alerts:
            return
        payloads = [alert.model_dump_json() for alert in alerts]
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.lpush(KEY_ALERTS_RECENT, *payloads)
            pipe.ltrim(KEY_ALERTS_RECENT, 0, ALERTS_RECENT_MAX - 1)
            for payload in payloads:
                pipe.publish(CHANNEL_ALERTS, payload)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("push_alerts", exc)
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

"""Graceful-degradation Redis wrapper: the pipeline's shared-state mirror.

Redis is an *enhancement*, never a hard dependency: every operation is wrapped so
a dead or unreachable Redis degrades to a no-op (returning ``None``/``False``)
instead of raising — the pipeline and the API keep serving from in-memory state.
Failures log ONE warning per outage (and one info line on recovery), never one
per call, so a long Redis outage cannot flood the logs at 1000 events/sec.

Key schema (everything carries the ``corr:`` prefix; the registry below is the
single source of truth):

    corr:events:recent                LIST — newest-first LogEvent JSON, capped at 1000
    corr:correlations:recent          LIST — newest-first Correlation JSON, capped at 2000
    corr:correlations:by_type:{type}  LIST — newest-first per-type Correlation JSON, capped at 500
    corr:stats                        HASH — total, per-type ``type:{t}`` counts, strength_sum
    corr:stats:minute:{minute}        HASH — per-minute total + ``type:{t}`` counts,
                                      1 h TTL (durability belt-and-braces; the API
                                      never reads it)
    corr:pattern:{type}:{a}:{b}       HASH — count, strength_sum, strength_sqsum,
                                      first_seen, last_seen. One PatternLearner
                                      baseline per pattern; {a}/{b} are the sorted
                                      normalized endpoints (source values, or
                                      metric names for metric_based)
    corr:pattern:index                ZSET — member = the full pattern hash key,
                                      score = observation count (hot patterns first)
    corr:alerts:recent                LIST — newest-first Alert JSON, capped at 200
    corr:alerts:channel               PUB/SUB — every fresh Alert JSON, fanned out live
"""

from __future__ import annotations

import logging
import time

import redis

from src.models import Alert, Correlation, LogEvent

logger = logging.getLogger(__name__)

# --- Key registry ----------------------------------------------------------------
KEY_EVENTS_RECENT = "corr:events:recent"
KEY_CORRELATIONS_RECENT = "corr:correlations:recent"
#: Template — ``.format(type=...)`` with a CorrelationType value.
KEY_CORRELATIONS_BY_TYPE = "corr:correlations:by_type:{type}"
KEY_STATS = "corr:stats"
#: Template — ``.format(minute=int(now // 60))``: per-minute detection counters.
KEY_STATS_MINUTE = "corr:stats:minute:{minute}"
#: Template — ``.format(type=..., a=..., b=...)``: one baseline hash per learned
#: pattern; {a}/{b} are the pattern's sorted normalized endpoints.
KEY_PATTERN = "corr:pattern:{type}:{a}:{b}"
#: ZSET index of every pattern hash key, scored by observation count.
KEY_PATTERN_INDEX = "corr:pattern:index"
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
#: TTL on the per-minute stats hashes — durability breadcrumbs, not API state.
MINUTE_STATS_TTL_SECONDS = 3600

#: How long :attr:`RedisStore.available` trusts the last probe before pinging
#: Redis again (avoids a ping per /health request).
_AVAILABILITY_TTL_SECONDS = 5.0

#: SCAN page size when hydrating pattern baselines (one startup-time sweep).
_PATTERN_SCAN_COUNT = 500


def _parse_int(value: str | None, default: int = 0) -> int:
    """Defensive int parse for hydrated hash fields (garbage -> ``default``)."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _parse_float(value: str | None, default: float = 0.0) -> float:
    """Defensive float parse for hydrated hash fields (garbage -> ``default``)."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


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

    @property
    def healthy(self) -> bool:
        """Last observed health with ZERO I/O (no probe, unlike :attr:`available`).

        Kept fresh by the pipeline's once-a-second writes; optimistically True
        before the first operation. The dashboard endpoint reads this so its
        request path never performs a Redis operation, not even a ping.
        """
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

    def incr_minute_stats(self, corrs: list[Correlation], now: float) -> None:
        """Advance the per-minute durability counters (never read by the API).

        HINCRBYs ``total`` plus one ``type:{t}`` count per type present onto
        ``corr:stats:minute:{minute}`` (minute = ``int(now // 60)``) and
        refreshes the 1-hour TTL — belt-and-braces history that survives
        restarts without feeding any request path. One pipelined round-trip,
        same graceful degradation as every other write.
        """
        if not corrs:
            return
        key = KEY_STATS_MINUTE.format(minute=int(now // 60))
        type_counts: dict[str, int] = {}
        for corr in corrs:
            type_value = corr.correlation_type.value
            type_counts[type_value] = type_counts.get(type_value, 0) + 1
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.hincrby(key, "total", len(corrs))
            for type_value, count in type_counts.items():
                pipe.hincrby(key, f"type:{type_value}", count)
            pipe.expire(key, MINUTE_STATS_TTL_SECONDS)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("incr_minute_stats", exc)
            return
        self._mark_success()

    # --- Pattern baselines (C7) -----------------------------------------------------
    def record_patterns(
        self, updates: list[tuple[tuple[str, str, str], float, float]]
    ) -> None:
        """Advance one baseline hash per update ((type, a, b), strength, now).

        One pipelined round-trip per detection cycle; per update: HINCRBY
        ``count``, HINCRBYFLOAT ``strength_sum`` / ``strength_sqsum``
        (strength squared), HSETNX ``first_seen`` (only the first observation
        wins), HSET ``last_seen``, and a ZINCRBY on ``corr:pattern:index``
        keyed by the full pattern hash key — so the hottest patterns sort
        first. Same graceful degradation as every other write.
        """
        if not updates:
            return
        try:
            pipe = self._client.pipeline(transaction=False)
            for (type_value, endpoint_a, endpoint_b), strength, now in updates:
                key = KEY_PATTERN.format(type=type_value, a=endpoint_a, b=endpoint_b)
                pipe.hincrby(key, "count", 1)
                pipe.hincrbyfloat(key, "strength_sum", float(strength))
                pipe.hincrbyfloat(key, "strength_sqsum", float(strength) * float(strength))
                pipe.hsetnx(key, "first_seen", now)
                pipe.hset(key, "last_seen", now)
                pipe.zincrby(KEY_PATTERN_INDEX, 1, key)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("record_patterns", exc)
            return
        self._mark_success()

    # --- Reads (startup hydration only — never on a request path) --------------------
    def load_patterns(self) -> dict[tuple[str, str, str], dict[str, float]]:
        """Hydrate every pattern baseline: {(type, a, b): fields} ({} on failure).

        One SCAN sweep (MATCH ``corr:pattern:*``, COUNT 500) collects the hash
        keys — skipping the index zset, which shares the prefix — then a single
        pipelined HGETALL round-trip fetches them all. Field values are parsed
        defensively: a malformed value falls back to 0 instead of poisoning the
        learner, and a malformed key is skipped entirely.
        """
        try:
            keys = [
                key
                for key in self._client.scan_iter(
                    match="corr:pattern:*", count=_PATTERN_SCAN_COUNT
                )
                if key != KEY_PATTERN_INDEX
            ]
            if not keys:
                self._mark_success()
                return {}
            pipe = self._client.pipeline(transaction=False)
            for key in keys:
                pipe.hgetall(key)
            rows = pipe.execute()
        except Exception as exc:  # noqa: BLE001 — degradation contract: never raise
            self._mark_failure("load_patterns", exc)
            return {}
        self._mark_success()

        out: dict[tuple[str, str, str], dict[str, float]] = {}
        prefix_len = len("corr:pattern:")
        for key, row in zip(keys, rows):
            parts = key[prefix_len:].split(":")
            if len(parts) != 3 or not isinstance(row, dict) or not row:
                continue  # defensive: a foreign key shape under our prefix
            out[(parts[0], parts[1], parts[2])] = {
                "count": _parse_int(row.get("count")),
                "strength_sum": _parse_float(row.get("strength_sum")),
                "strength_sqsum": _parse_float(row.get("strength_sqsum")),
                "first_seen": _parse_float(row.get("first_seen")),
                "last_seen": _parse_float(row.get("last_seen")),
            }
        return out

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

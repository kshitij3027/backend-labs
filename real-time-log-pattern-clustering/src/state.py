"""Persistence / state layer for the Real-Time Log Pattern Clustering engine.

:class:`StateStore` is a backend-agnostic store for the three pieces of live engine
state the dashboard reads back:

* **stats**    — the latest :class:`~src.schemas.StatsSnapshot` (a single JSON object),
* **patterns** — the current list of :class:`~src.schemas.PatternRecord` (a JSON list),
* **anomalies**— a recent-first, capped history of :class:`~src.schemas.AnomalyAlert`.

Plus a generic namespaced JSON key/value space (:meth:`set_json` / :meth:`get_json`).

The store exposes the **same API regardless of backend**. When constructed with a
reachable Redis client it persists to Redis (surviving restarts and visible across
processes); otherwise it transparently uses an in-process dict + deque. Callers pass
plain JSON-able values (typically ``model.model_dump(mode="json")`` dicts), so the store
itself stays decoupled from the Pydantic schemas.

Every method is **exception-safe**: a Redis op that throws mid-flight is logged and
swallowed (reads degrade to ``None`` / ``[]``, writes become no-ops) so a transient
Redis failure can never crash the engine. The engine keeps running — it just stops
persisting until Redis recovers.

Use :func:`create_state_store` as the entry point; it wires up a Redis client (or the
in-memory fallback) for you.
"""

from __future__ import annotations

import collections
import json
import logging
from typing import Any, Optional

from src.clients.redis import make_redis_client, redis_available
from src.config import AppConfig

logger = logging.getLogger(__name__)

#: Namespace prefix for every key this store owns. ``clear()`` scopes its delete to it.
KEY_PREFIX = "rtlpc:"
#: Fixed keys for the three first-class collections.
STATS_KEY = f"{KEY_PREFIX}stats"
PATTERNS_KEY = f"{KEY_PREFIX}patterns"
ANOMALIES_KEY = f"{KEY_PREFIX}anomalies"
#: Prefix for the generic JSON namespace exposed via set_json / get_json.
KV_PREFIX = f"{KEY_PREFIX}kv:"

#: Default upper bound on retained anomalies (matches push_anomaly's ``cap`` default).
DEFAULT_ANOMALY_CAP = 200


class StateStore:
    """Backend-agnostic store for engine stats, patterns, and anomaly history.

    Construct with a live Redis client to persist to Redis, or with ``None`` (or an
    unreachable client) to use the in-memory fallback. Prefer :func:`create_state_store`
    which performs the client wiring. The public API is identical for both backends.
    """

    def __init__(
        self,
        redis_client: Any = None,
        *,
        anomaly_cap: int = DEFAULT_ANOMALY_CAP,
    ) -> None:
        """Initialise the store, selecting the Redis backend only if the client pings.

        Args:
            redis_client: A :class:`redis.Redis` instance, or ``None`` for in-memory.
                A non-``None`` client that fails its ``PING`` is rejected and the store
                falls back to memory (so a dead client never silently drops writes).
            anomaly_cap: Capacity of the in-memory anomaly deque. The per-call ``cap``
                on :meth:`push_anomaly` governs trimming for both backends; this only
                sizes the memory deque's hard ceiling.
        """
        self._anomaly_cap = anomaly_cap
        if redis_client is not None and redis_available(redis_client):
            self._redis = redis_client
            self._backend = "redis"
            self._mem: Optional[dict[str, Any]] = None
            self._anomalies: Optional[collections.deque[str]] = None
        else:
            self._redis = None
            self._backend = "memory"
            self._mem = {}
            # Stored as JSON strings (mirroring Redis) so round-trips are identical.
            self._anomalies = collections.deque(maxlen=anomaly_cap)

    # ------------------------------------------------------------------ status

    @property
    def backend(self) -> str:
        """Return the active backend: ``"redis"`` or ``"memory"``."""
        return self._backend

    def available(self) -> bool:
        """Return whether the store can currently serve reads/writes.

        The in-memory backend is always available; the Redis backend is available only
        while its client responds to ``PING``.
        """
        if self._backend == "memory":
            return True
        return redis_available(self._redis)

    # -------------------------------------------------------------- stats

    def save_stats(self, stats: dict) -> None:
        """Persist the latest stats snapshot (overwrites any previous value).

        Args:
            stats: A JSON-able dict (typically ``StatsSnapshot.model_dump(mode="json")``).
        """
        if self._backend == "redis":
            self._safe_set(STATS_KEY, stats)
        else:
            assert self._mem is not None
            self._mem[STATS_KEY] = stats

    def load_stats(self) -> Optional[dict]:
        """Return the last-saved stats dict, or ``None`` if nothing was saved yet."""
        if self._backend == "redis":
            return self._safe_get(STATS_KEY)
        assert self._mem is not None
        return self._mem.get(STATS_KEY)

    # ------------------------------------------------------------ patterns

    def save_patterns(self, patterns: list[dict]) -> None:
        """Persist the current list of pattern records (overwrites the previous list).

        Args:
            patterns: A JSON-able list of dicts (``PatternRecord.model_dump(...)`` each).
        """
        if self._backend == "redis":
            self._safe_set(PATTERNS_KEY, list(patterns))
        else:
            assert self._mem is not None
            self._mem[PATTERNS_KEY] = list(patterns)

    def load_patterns(self) -> list[dict]:
        """Return the saved pattern list, or ``[]`` when none has been saved."""
        if self._backend == "redis":
            value = self._safe_get(PATTERNS_KEY)
        else:
            assert self._mem is not None
            value = self._mem.get(PATTERNS_KEY)
        return value if isinstance(value, list) else []

    # ----------------------------------------------------------- anomalies

    def push_anomaly(self, alert: dict, cap: int = DEFAULT_ANOMALY_CAP) -> None:
        """Prepend an anomaly to the recent-first history, trimming to ``cap`` entries.

        Args:
            alert: A JSON-able dict (``AnomalyAlert.model_dump(mode="json")``).
            cap: Maximum number of anomalies to retain (most-recent kept).
        """
        try:
            payload = json.dumps(alert)
        except (TypeError, ValueError):
            logger.warning("push_anomaly: alert is not JSON-serialisable; dropping")
            return

        if self._backend == "redis":
            try:
                pipe = self._redis.pipeline()
                pipe.lpush(ANOMALIES_KEY, payload)
                if cap > 0:
                    pipe.ltrim(ANOMALIES_KEY, 0, cap - 1)
                pipe.execute()
            except Exception as exc:
                logger.warning("Redis push_anomaly failed (%s); skipping persist", exc)
        else:
            assert self._anomalies is not None
            self._anomalies.appendleft(payload)
            # Honour a per-call cap tighter than the deque's maxlen.
            while cap >= 0 and len(self._anomalies) > cap:
                self._anomalies.pop()

    def recent_anomalies(self, limit: int = 50) -> list[dict]:
        """Return up to ``limit`` most-recent anomalies, newest first.

        Args:
            limit: Maximum number of anomalies to return.

        Returns:
            A list of anomaly dicts (most-recent first); ``[]`` if none or on error.
        """
        if limit <= 0:
            return []
        if self._backend == "redis":
            try:
                raw = self._redis.lrange(ANOMALIES_KEY, 0, limit - 1)
            except Exception as exc:
                logger.warning("Redis recent_anomalies failed (%s); returning []", exc)
                return []
        else:
            assert self._anomalies is not None
            raw = list(self._anomalies)[:limit]
        return self._decode_list(raw)

    # ------------------------------------------------------- generic JSON kv

    def set_json(self, key: str, value: Any) -> None:
        """Store an arbitrary JSON-able value under the namespaced key ``rtlpc:kv:<key>``.

        Args:
            key: Caller-facing key (namespaced internally; need not include the prefix).
            value: Any JSON-serialisable value.
        """
        full = self._kv_key(key)
        if self._backend == "redis":
            self._safe_set(full, value)
        else:
            assert self._mem is not None
            self._mem[full] = value

    def get_json(self, key: str) -> Any:
        """Return the value previously stored via :meth:`set_json`, or ``None``."""
        full = self._kv_key(key)
        if self._backend == "redis":
            return self._safe_get(full)
        assert self._mem is not None
        return self._mem.get(full)

    # --------------------------------------------------------- lifecycle

    def clear(self) -> None:
        """Delete every ``rtlpc:*`` key (Redis) / reset all in-memory state.

        Intended for tests and for a clean re-bootstrap. Scoped strictly to this store's
        namespace so it never touches unrelated Redis keys.
        """
        if self._backend == "redis":
            try:
                keys = list(self._redis.scan_iter(match=f"{KEY_PREFIX}*", count=500))
                if keys:
                    self._redis.delete(*keys)
            except Exception as exc:
                logger.warning("Redis clear failed (%s); leaving keys in place", exc)
        else:
            assert self._mem is not None and self._anomalies is not None
            self._mem.clear()
            self._anomalies.clear()

    def close(self) -> None:
        """Close the underlying Redis client if present; a no-op for in-memory.

        Safe to call multiple times and safe if the client is already closed.
        """
        if self._redis is not None:
            try:
                self._redis.close()
            except Exception as exc:  # pragma: no cover - close errors are non-fatal
                logger.debug("Redis client close failed (%s); ignoring", exc)

    # ----------------------------------------------------------- internals

    @staticmethod
    def _kv_key(key: str) -> str:
        """Map a caller key to its namespaced ``rtlpc:kv:<key>`` form (idempotent)."""
        return key if key.startswith(KV_PREFIX) else f"{KV_PREFIX}{key}"

    def _safe_set(self, key: str, value: Any) -> None:
        """SET ``key`` to ``json.dumps(value)`` on Redis, swallowing any failure."""
        try:
            self._redis.set(key, json.dumps(value))
        except (TypeError, ValueError) as exc:
            logger.warning("Refusing to persist non-JSON value at %s (%s)", key, exc)
        except Exception as exc:
            logger.warning("Redis SET %s failed (%s); skipping persist", key, exc)

    def _safe_get(self, key: str) -> Any:
        """GET ``key`` from Redis and JSON-decode it; ``None`` on miss or any failure."""
        try:
            raw = self._redis.get(key)
        except Exception as exc:
            logger.warning("Redis GET %s failed (%s); returning None", key, exc)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError) as exc:
            logger.warning("Corrupt JSON at %s (%s); returning None", key, exc)
            return None

    @staticmethod
    def _decode_list(raw: list[str]) -> list[dict]:
        """JSON-decode a list of stored strings, skipping any that fail to parse."""
        out: list[dict] = []
        for item in raw:
            try:
                out.append(json.loads(item))
            except (TypeError, ValueError):
                logger.warning("Skipping corrupt anomaly entry in history")
        return out


def create_state_store(config: Optional[AppConfig] = None) -> StateStore:
    """Build a :class:`StateStore`, wiring a Redis client (or the in-memory fallback).

    This is the entry point the app/engine should use. It attempts a Redis connection
    via :func:`~src.clients.redis.make_redis_client` (honouring ``REDIS_HOST`` etc.) and
    hands the result to :class:`StateStore`; an unreachable Redis transparently yields a
    memory-backed store.

    Args:
        config: Optional :class:`AppConfig`; loaded from defaults/YAML/env when ``None``.

    Returns:
        A ready-to-use :class:`StateStore` (Redis-backed if reachable, else in-memory).
    """
    client = make_redis_client(config)
    return StateStore(client)

"""Async Redis cache-aside helpers with graceful fallback.

Responsibilities:

* Own the Redis client lifecycle (``connect`` / ``ping``).
* Expose ``make_key`` — a deterministic SHA1-based cache-key builder
  so the same filter payload always maps to the same key regardless
  of Python dict iteration order.
* Expose ``get_or_compute`` — cache-aside around an awaitable
  ``compute`` callable. If Redis is unreachable we log at WARNING,
  bump the ``errors`` counter, and invoke ``compute`` directly. We
  NEVER raise from here: the caller must always get a value back.
* Expose ``get_facet_values_cached`` — a small helper specific to
  the ``SELECT DISTINCT <dim>`` lookup used by the stats endpoint.

Cache counters (``hits`` / ``misses`` / ``errors``) live as
module-level dataclass fields so ``/api/stats`` can read them
without extra machinery.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from typing import Any, Awaitable, Callable, List, Optional, Tuple, Union

import aiosqlite
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

import hashlib

logger = logging.getLogger(__name__)


# Redis-connection errors we treat as "Redis down, fall back to
# direct compute". We include ``OSError`` because the underlying
# socket layer sometimes surfaces OS-level failures (DNS, refused
# connection) before the Redis client wraps them.
_REDIS_CONNECTION_ERRORS: Tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    asyncio.TimeoutError,
    OSError,
)


# ---------------------------------------------------------------------------
# Module-level cache counters.
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class CacheStats:
    """Plain counters bumped by ``get_or_compute``.

    Kept as a dataclass instance so the ``/api/stats`` endpoint can
    read all three fields in one shot and reset them in tests if
    needed. Process-local: one counter per Python process.
    """

    hits: int = 0
    misses: int = 0
    errors: int = 0

    def reset(self) -> None:
        self.hits = 0
        self.misses = 0
        self.errors = 0


# Single shared instance — import this in ``src/api/stats.py`` and
# wherever else you need to read counters.
stats = CacheStats()


# ---------------------------------------------------------------------------
# Single-flight dedup for concurrent cache misses.
#
# Without this, 100 workers hitting an uncached key all compute the
# SAME search — 100x the DB load, 100x the pool queue pressure, and
# a catastrophic p95. We keep a per-process map of in-flight futures
# keyed by cache key so a second caller with the same key just awaits
# the first one's result.
# ---------------------------------------------------------------------------

_inflight: dict[str, asyncio.Future[Any]] = {}


# ---------------------------------------------------------------------------
# Key derivation + client lifecycle.
# ---------------------------------------------------------------------------

def make_key(prefix: str, payload: Any) -> str:
    """Build a deterministic cache key from ``prefix`` + ``payload``.

    ``payload`` is typically a dict (e.g. ``request.model_dump()`` or
    a flat query-param mapping); ``sort_keys=True`` + ``default=str``
    guarantee the same logical payload always hashes to the same
    digest regardless of Python dict iteration order.
    """
    blob = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha1(blob.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


async def connect(url: str) -> aioredis.Redis:
    """Open a Redis client (does NOT ping).

    Short socket timeouts keep failing requests snappy when Redis is
    down — ``get_or_compute`` will catch the resulting error and fall
    through to the compute path. Call ``ping`` separately if you want
    a real reachability check at startup.
    """
    client = aioredis.from_url(
        url,
        socket_connect_timeout=0.5,
        socket_timeout=1.0,
        decode_responses=True,
    )
    logger.info("redis client created url=%s", url)
    return client


async def ping(client: Optional[aioredis.Redis]) -> bool:
    """Return True iff the client responds to PING.

    Swallows connection-level errors (Redis down, DNS failure, etc.)
    and returns False — callers use this to expose a ``redis_reachable``
    flag in ``/health`` and ``/api/stats`` without cascading failures.
    """
    if client is None:
        return False
    try:
        return bool(await client.ping())
    except _REDIS_CONNECTION_ERRORS as exc:
        logger.warning("redis ping failed: %s", exc)
        return False
    except RedisError as exc:
        logger.warning("redis ping redis-error: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Cache-aside core.
# ---------------------------------------------------------------------------

ComputeFn = Callable[[], Awaitable[Any]]


async def _compute_with_singleflight(
    key: str,
    compute: ComputeFn,
) -> Any:
    """Run ``compute`` at most once per in-flight ``key``.

    Concurrent callers with the same key wait for the first caller's
    future instead of racing into their own computes. This is the
    single biggest p95 lever under load: without it, 100 workers all
    missing the same cache key will each grab a reader from the pool
    and do the full 40-80ms facet scan, multiplying DB pressure 100x.
    """
    # Fast path: an in-flight future already exists. Awaiting it
    # piggybacks on the first caller's work and does not touch the DB.
    inflight = _inflight.get(key)
    if inflight is not None:
        return await inflight

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()
    # Race check: another task may have inserted a future between our
    # .get above and here. Since this coroutine hasn't awaited anything
    # in between, under asyncio's cooperative scheduling no other task
    # can have run, but we defensively look again to make the code
    # safe under future refactors.
    existing = _inflight.setdefault(key, fut)
    if existing is not fut:
        # Another caller won the race. Drop ours, await theirs.
        return await existing

    try:
        value = await compute()
        fut.set_result(value)
        return value
    except BaseException as exc:  # noqa: BLE001 - propagate to all waiters
        fut.set_exception(exc)
        raise
    finally:
        # Remove ourselves from the inflight map only if we're still
        # the registered future. Safe even if another coroutine has
        # already replaced it (defensive; can't happen today).
        if _inflight.get(key) is fut:
            _inflight.pop(key, None)


async def get_or_compute(
    client: Optional[aioredis.Redis],
    key: str,
    compute: ComputeFn,
    ttl: int,
) -> Tuple[Any, bool]:
    """Cache-aside around ``compute()`` with Redis fallback.

    Returns ``(value, was_hit)``:

    * On cache hit: ``(json.loads(cached), True)`` and ``stats.hits`` bumped.
    * On cache miss: ``value = await compute()``, store for ``ttl``
      seconds, return ``(value, False)`` and ``stats.misses`` bumped.
    * On Redis connection error: log WARNING, bump ``stats.errors``,
      return ``(await compute(), False)``. We never raise.

    Concurrent callers with the same ``key`` share one compute via a
    per-process in-flight map — without that, 100 workers missing on
    the same key would each run the full facet scan and crush the
    read pool.

    ``compute`` is called at most once per key per in-flight window.
    Serialization uses ``json.dumps(..., default=str)`` so any
    Pydantic-emitted non-JSON-native types still round-trip.
    """
    # Redis unreachable from the outset — skip the lookup entirely
    # and just compute. Counters still get bumped as an "error".
    if client is None:
        stats.errors += 1
        value = await _compute_with_singleflight(key, compute)
        return value, False

    # --- Try GET ---
    try:
        cached = await client.get(key)
    except _REDIS_CONNECTION_ERRORS as exc:
        logger.warning("redis get failed key=%s err=%s", key, exc)
        stats.errors += 1
        value = await _compute_with_singleflight(key, compute)
        return value, False
    except RedisError as exc:
        # Non-connection Redis errors (e.g. OOM, auth) — treat as
        # cache miss with error bump; request must still succeed.
        logger.warning("redis get redis-error key=%s err=%s", key, exc)
        stats.errors += 1
        value = await _compute_with_singleflight(key, compute)
        return value, False

    if cached is not None:
        # Cache hit. Decode and return. If the cached blob is somehow
        # malformed (shouldn't happen, but be defensive), fall through
        # to the compute path so the caller isn't blocked by a bad
        # cached value.
        try:
            value = json.loads(cached)
        except (TypeError, ValueError) as exc:
            logger.warning("redis cache decode failed key=%s err=%s", key, exc)
            stats.errors += 1
            value = await _compute_with_singleflight(key, compute)
            return value, False
        stats.hits += 1
        return value, True

    # --- Cache miss: compute + setex ---
    stats.misses += 1
    value = await _compute_with_singleflight(key, compute)
    try:
        await client.setex(key, ttl, json.dumps(value, default=str))
    except _REDIS_CONNECTION_ERRORS as exc:
        logger.warning("redis setex failed key=%s err=%s", key, exc)
        stats.errors += 1
    except RedisError as exc:
        logger.warning("redis setex redis-error key=%s err=%s", key, exc)
        stats.errors += 1
    return value, False


# ---------------------------------------------------------------------------
# Facet-values cache (for stats / future sidebar pre-fill).
# ---------------------------------------------------------------------------

async def get_facet_values_cached(
    client: Optional[aioredis.Redis],
    dim: str,
    db: aiosqlite.Connection,
    ttl: int,
) -> List[Union[str, int]]:
    """Return distinct values for ``dim`` with Redis cache-aside.

    Key is ``facet_values:<dim>``. Value list is pulled straight
    from SQLite via ``SELECT DISTINCT <dim> FROM logs ORDER BY <dim>``.
    Redis outage falls through to the direct query — same semantics
    as ``get_or_compute``. Never raises.
    """
    key = f"facet_values:{dim}"

    async def _compute() -> List[Union[str, int]]:
        # NOTE: ``dim`` is trusted — callers supply it from our own
        # FACET_DIMS literal. It never flows in from user input.
        sql = f"SELECT DISTINCT {dim} FROM logs ORDER BY {dim}"
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
        return [row[0] for row in rows if row[0] is not None]

    value, _ = await get_or_compute(client, key, _compute, ttl)
    return value

"""L2 distributed cache tier backed by Redis (async).

The L2 tier sits between the in-process L1 cache and the slow Postgres-backed
L3 store. It uses ``redis.asyncio`` with a bounded connection pool and stores
**binary** values: cache payloads are serialized (and optionally zstd-compressed)
by :mod:`src.compression`, so the client is created with
``decode_responses=False`` — decoding bytes to ``str`` would corrupt compressed
blobs.

Graceful degradation is a hard requirement (project §5): **every** Redis
operation is wrapped so that a connection failure, timeout, or protocol error
results in a cache miss / no-op and flips :attr:`L2Redis.degraded` to ``True``
instead of propagating an exception. The caller (the cache manager) can then
fall through to L3 / the backend and keep serving requests even when Redis is
down or unreachable.

Invalidation never uses the blocking ``KEYS`` command — pattern invalidation
streams matching keys with ``SCAN`` (via ``scan_iter``), and tag invalidation
uses Redis sets (``tag:<tag>`` -> member keys).
"""
from __future__ import annotations

import asyncio
from typing import Any, Iterable

import redis.asyncio
import redis.exceptions

from src.compression import decode_value, encode_value

# Exceptions that must NEVER escape a public L2 method. Catching all of these
# and degrading gracefully is what keeps the system serving when Redis is down.
_FAILSOFT_EXCEPTIONS = (
    redis.exceptions.RedisError,
    asyncio.TimeoutError,
    OSError,
    ConnectionError,
)


class L2Redis:
    """Async Redis-backed L2 cache tier with fail-soft graceful degradation.

    Args:
        url: Redis connection URL (e.g. ``redis://redis:6379/0``).
        ttl_seconds: default TTL applied to ``set`` when no per-call TTL given.
        timeout: per-operation timeout (seconds) for connect, socket, and the
            ``asyncio.wait_for`` guard around each command.
        compress: default compression flag passed to
            :func:`src.compression.encode_value` on ``set``.

    The underlying client is created lazily in :meth:`connect` and is not
    forced to open a socket until the first command runs.
    """

    def __init__(
        self,
        url: str,
        *,
        ttl_seconds: int = 600,
        timeout: float = 2.0,
        compress: bool = True,
    ) -> None:
        self.url = url
        self.ttl_seconds = ttl_seconds
        self.timeout = timeout
        self.compress = compress

        self._client: redis.asyncio.Redis | None = None

        # Observability counters / state.
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.degraded = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        """Create the pooled async client (lazy — does not open a socket).

        Safe to call once during application/test startup. The pool is bounded
        at 20 connections and every socket op inherits ``self.timeout``.
        """
        self._client = redis.asyncio.from_url(
            self.url,
            decode_responses=False,  # store/return raw bytes (binary zstd blobs)
            max_connections=20,
            socket_connect_timeout=self.timeout,
            socket_timeout=self.timeout,
            health_check_interval=30,
        )

    async def close(self) -> None:
        """Close the client and release its connection pool, if connected."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except _FAILSOFT_EXCEPTIONS:
                # Closing is best-effort; never raise from teardown.
                pass
            self._client = None

    # ------------------------------------------------------------------ #
    # Core read / write
    # ------------------------------------------------------------------ #
    async def get(self, key: str) -> Any | None:
        """Return the decoded value for ``key``, or ``None`` on miss/failure.

        Increments :attr:`hits` on a hit, :attr:`misses` on a clean miss, and
        :attr:`errors` (plus sets :attr:`degraded`) on any Redis failure. Never
        raises — a failure is reported to the caller as a miss.
        """
        if self._client is None:
            self.misses += 1
            return None
        try:
            raw = await asyncio.wait_for(self._client.get(key), self.timeout)
            if raw is None:
                self.misses += 1
                return None
            value = decode_value(raw)
            self.hits += 1
            return value
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return None

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: int | None = None,
        tags: Iterable[str] | None = None,
        compress: bool | None = None,
    ) -> bool:
        """Store ``value`` under ``key`` with a TTL; optionally tag it.

        The value is encoded (and optionally compressed) via
        :func:`src.compression.encode_value`. When ``tags`` is given, each tag
        ``t`` gets ``key`` added to its ``tag:<t>`` set and that set is given
        the same TTL (so tag bookkeeping expires with the data).

        Returns ``True`` on success (and clears :attr:`degraded`), ``False`` on
        any failure (and sets :attr:`degraded`). Never raises.
        """
        if self._client is None:
            self.degraded = True
            return False

        effective_ttl = ttl if ttl is not None else self.ttl_seconds
        use_compress = self.compress if compress is None else compress
        try:
            body = encode_value(value, compress=use_compress)
            await asyncio.wait_for(
                self._client.set(key, body, ex=effective_ttl), self.timeout
            )
            if tags:
                pipe = self._client.pipeline(transaction=False)
                for tag in tags:
                    tag_set = f"tag:{tag}"
                    pipe.sadd(tag_set, key)
                    pipe.expire(tag_set, effective_ttl)
                await asyncio.wait_for(pipe.execute(), self.timeout)
            self.degraded = False
            return True
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return False

    async def delete(self, key: str) -> int:
        """Delete ``key``; return the number of keys removed (0 on failure)."""
        if self._client is None:
            self.degraded = True
            return 0
        try:
            removed = await asyncio.wait_for(self._client.delete(key), self.timeout)
            return int(removed or 0)
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return 0

    # ------------------------------------------------------------------ #
    # Invalidation
    # ------------------------------------------------------------------ #
    async def invalidate_pattern(self, pattern: str) -> int:
        """Delete every key matching ``pattern`` (glob) and return the count.

        Streams matches with ``SCAN`` (never the blocking ``KEYS``) and deletes
        in batches. Fail-soft: returns the number deleted so far (0 on early
        failure) and never raises.
        """
        if self._client is None:
            self.degraded = True
            return 0

        deleted = 0
        batch: list[Any] = []
        try:
            async for raw_key in self._client.scan_iter(match=pattern, count=200):
                batch.append(raw_key)
                if len(batch) >= 200:
                    removed = await asyncio.wait_for(
                        self._client.delete(*batch), self.timeout
                    )
                    deleted += int(removed or 0)
                    batch.clear()
            if batch:
                removed = await asyncio.wait_for(
                    self._client.delete(*batch), self.timeout
                )
                deleted += int(removed or 0)
            return deleted
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return deleted

    async def tag_members(self, tag: str) -> list[str]:
        """Return the member keys recorded under ``tag:<tag>`` as ``str``.

        Reads the tag set with ``SMEMBERS`` (members come back as ``bytes`` from
        the ``decode_responses=False`` client and are decoded to ``str`` here).
        Fail-soft: returns ``[]`` on a missing tag or any Redis failure (and
        flips :attr:`degraded` on failure). Never raises.
        """
        if self._client is None:
            self.degraded = True
            return []

        tag_set = f"tag:{tag}"
        try:
            members = await asyncio.wait_for(
                self._client.smembers(tag_set), self.timeout
            )
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return []

        result: list[str] = []
        for member in members or ():
            if isinstance(member, bytes):
                result.append(member.decode())
            else:
                result.append(str(member))
        return result

    async def invalidate_tag(self, tag: str) -> int:
        """Delete all keys tagged ``tag`` plus the ``tag:<tag>`` set itself.

        Returns the number of member keys deleted (0 on failure). Member keys
        come back as ``bytes`` from ``SMEMBERS``; they are passed straight to
        ``DEL`` (binary-safe). Fail-soft — never raises.
        """
        if self._client is None:
            self.degraded = True
            return 0

        tag_set = f"tag:{tag}"
        try:
            members = await asyncio.wait_for(
                self._client.smembers(tag_set), self.timeout
            )
            deleted = 0
            if members:
                # members are bytes; DEL accepts bytes keys directly.
                removed = await asyncio.wait_for(
                    self._client.delete(*members), self.timeout
                )
                deleted = int(removed or 0)
            # Drop the tag set regardless so stale bookkeeping doesn't linger.
            await asyncio.wait_for(self._client.delete(tag_set), self.timeout)
            return deleted
        except _FAILSOFT_EXCEPTIONS:
            self.errors += 1
            self.degraded = True
            return 0

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    async def mem_used_bytes(self) -> int | None:
        """Best-effort ``used_memory`` from Redis ``INFO memory`` (None on fail)."""
        if self._client is None:
            return None
        try:
            info = await asyncio.wait_for(
                self._client.info("memory"), self.timeout
            )
            used = info.get("used_memory") if isinstance(info, dict) else None
            return int(used) if used is not None else None
        except _FAILSOFT_EXCEPTIONS:
            return None

    def stats(self) -> dict:
        """Return a snapshot of counters + derived hit rate for this tier."""
        total = self.hits + self.misses
        hit_rate = (self.hits / total) if total else 0.0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "degraded": self.degraded,
            "hit_rate": hit_rate,
        }

    @property
    def raw(self) -> redis.asyncio.Redis | None:
        """The underlying async client (for tests / advanced callers)."""
        return self._client

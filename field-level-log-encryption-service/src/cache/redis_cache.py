"""Redis-backed :class:`CacheProvider`.

Uses the official ``redis-py`` client. Two design choices worth flagging:

* ``decode_responses=False`` — we store arbitrary bytes for the
  ``set`` / ``get`` namespace, so disabling auto-decode is mandatory
  (auto-decode would try to interpret ciphertext bytes as UTF-8 and
  blow up). For the counter namespace ``INCR`` always returns an int
  regardless of this flag, and ``GET`` on an integer key returns the
  ASCII bytes representation which we explicitly ``int(...)`` in
  :meth:`get_counter`.
* Short connection + socket timeouts (1 second). Without these the
  :func:`~src.cache.factory.build_cache` fallback would block for
  ``redis-py``'s 60-second default before falling back to in-memory,
  defeating the whole point of having a fallback. 1 second is long
  enough to absorb a brief DNS hiccup in compose (where ``redis`` is
  the service name) and short enough that startup latency stays under
  a second on every CI run.

Constructor behaviour: we ``ping()`` on construction. A network failure
there is raised as :class:`~src.cache.provider.CacheUnavailable` with
the underlying exception chained, so the factory can catch it and
fall back without having to know about ``redis.ConnectionError``
specifically.

We do NOT use ``KEYS`` for prefix scans — ``KEYS *`` is O(N) blocking
which would freeze a busy Redis instance. ``SCAN_ITER`` is the
production-safe cursor-based alternative.
"""
from __future__ import annotations

import logging

import redis

from .provider import CacheProvider, CacheUnavailable

logger = logging.getLogger(__name__)


class RedisCache(CacheProvider):
    """Networked :class:`CacheProvider` backed by Redis.

    Parameters
    ----------
    host : str
        Redis host (typically the docker-compose service name
        ``redis``).
    port : int
        Redis TCP port (default 6379 in production).
    socket_connect_timeout_s : float
        Max time to wait for the initial TCP handshake. Defaults to 1
        second — short enough that the in-memory fallback kicks in
        quickly when Redis is genuinely down.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        socket_connect_timeout_s: float = 1.0,
    ) -> None:
        # Build the client. ``decode_responses=False`` is critical so
        # binary values (ciphertext bytes in future use cases) round-trip
        # cleanly. The same flag means counter ``GET`` returns bytes
        # like ``b"42"``; :meth:`get_counter` does the int() conversion.
        self._client = redis.Redis(
            host=host,
            port=port,
            socket_connect_timeout=socket_connect_timeout_s,
            socket_timeout=1.0,
            decode_responses=False,
        )

        # Ping verifies the connection is actually live — without this
        # check the constructor would succeed even with a wrong host,
        # and the first ``incr`` would throw deep inside a hot path.
        # Re-raise as :class:`CacheUnavailable` so the factory's catch
        # block can recognise the failure mode without depending on
        # redis-py internals.
        try:
            self._client.ping()
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            raise CacheUnavailable(
                f"redis at {host}:{port} unreachable: {exc}"
            ) from exc
        logger.info("RedisCache connected to %s:%d", host, port)

    # ------------------------------------------------------------------
    # CacheProvider implementation
    # ------------------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        """Return the bytes stored at ``key`` or ``None`` if absent."""
        return self._client.get(key)

    def set(self, key: str, value: bytes, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key`` with optional TTL.

        ``redis.Redis.set(..., ex=ttl_sec)`` is atomic: the key is
        written with the TTL in a single round-trip. Passing
        ``ex=None`` is the canonical "no expiry" signal.
        """
        # `ex=None` means "no expiry" in redis-py — exactly the semantic
        # we want when the caller omits ``ttl_sec``.
        self._client.set(key, value, ex=ttl_sec)

    def incr(self, key: str) -> int:
        """Atomically increment the counter at ``key`` and return the new value.

        Redis ``INCR`` creates the key at 0 then increments to 1 if
        missing — exactly the contract our ABC documents. Atomicity is
        guaranteed by Redis's single-threaded command loop.
        """
        return int(self._client.incr(key))

    def get_counter(self, key: str) -> int:
        """Return the current counter at ``key`` or 0 if missing.

        ``GET`` returns ``None`` for a missing key — we coerce to 0
        before the int() so the symmetric "no usage yet → 0 count"
        semantics match :class:`~src.cache.in_memory.InMemoryCache`.
        """
        raw = self._client.get(key)
        if raw is None:
            return 0
        # ``raw`` is bytes like ``b"42"`` because decode_responses=False.
        return int(raw)

    def keys_with_prefix(self, prefix: str) -> list[str]:
        """Return every counter key whose name starts with ``prefix``.

        Implemented via ``SCAN_ITER`` — cursor-based, non-blocking,
        production-safe. ``KEYS *`` would also work but is O(N)
        blocking and freezes Redis for the duration; we never use it.

        Each key from ``scan_iter`` is bytes (decode_responses=False);
        we decode to str so callers see a uniform ``list[str]`` type
        regardless of which backend is live.
        """
        pattern = f"{prefix}*"
        # ``scan_iter`` returns a generator; we materialise into a list
        # so the caller gets the standard interface contract.
        return [
            key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else key
            for key in self._client.scan_iter(match=pattern)
        ]

    def close(self) -> None:
        """Close the underlying client (releases the connection pool).

        Idempotent — redis-py's ``close()`` is safe to call multiple
        times. We swallow any cleanup exception because shutdown
        should never crash the app even if the network is half-broken.
        """
        try:
            self._client.close()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("RedisCache close failed (ignored): %s", exc)

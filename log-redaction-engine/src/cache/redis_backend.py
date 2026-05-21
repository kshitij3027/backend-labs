"""Redis-backed :class:`Backend` implementation.

Uses the official ``redis-py`` client. Two design choices worth flagging:

* ``decode_responses=True`` — the Protocol surfaces ``str`` values, not
  ``bytes``. Enabling auto-decode pushes that responsibility into the
  client and keeps our higher-level code free of decode bookkeeping.
  This is the only difference from the reference field-encryption
  service's RedisCache (which stores ciphertext bytes).
* Short connection timeout (1 second by default). Without it the C10
  fallback path would block for ``redis-py``'s 60-second default before
  the lifespan could fall back to ``InMemoryBackend``, defeating the
  whole point of graceful degradation. 1 s is long enough to absorb a
  brief DNS hiccup in compose (where ``redis`` is the service name)
  and short enough that startup stays sub-second on every CI run.

Constructor behaviour
---------------------
We ``ping()`` on construction. If Redis is unreachable, the ``ping`` raises
``redis.ConnectionError`` (or a related exception) and we let it
propagate — the C10 lifespan catches it under a broad ``except Exception``
and falls back to :class:`InMemoryBackend`. We DON'T wrap it in a
custom exception because the caller doesn't need to distinguish
"unreachable" from "auth failed" from "DNS failed" — every failure
mode triggers the same fallback.

Prefix scans use ``SCAN_ITER``
------------------------------
``KEYS *`` is O(N) blocking, which would freeze a busy Redis instance
for the duration. ``SCAN_ITER`` is the cursor-based, production-safe
alternative — non-blocking and resumable. We pass ``count=1000`` as a
hint to the server about batch size; the contract is unchanged but
big result sets paginate more efficiently.
"""
from __future__ import annotations

import logging

import redis

logger = logging.getLogger(__name__)


class RedisBackend:
    """Networked :class:`Backend` implementation backed by Redis.

    Parameters
    ----------
    host : str
        Redis host (typically the docker-compose service name ``redis``).
    port : int
        Redis TCP port (default 6379 in production).
    socket_connect_timeout : float
        Max time to wait for the initial TCP handshake. Defaults to 1
        second so the in-memory fallback kicks in quickly when Redis
        is genuinely down.
    """

    # Class-level identifier surfaced in startup logs so an operator
    # can tell which backend the lifespan selected.
    name = "redis"

    def __init__(
        self,
        host: str,
        port: int,
        socket_connect_timeout: float = 1.0,
    ) -> None:
        # Build the client. ``decode_responses=True`` aligns with the
        # Backend Protocol's ``str`` value type — no bytes leak through.
        self._client = redis.Redis(
            host=host,
            port=port,
            socket_connect_timeout=socket_connect_timeout,
            decode_responses=True,
        )
        # Ping verifies the connection is actually live — without this
        # check the constructor would succeed even with a wrong host, and
        # the first real call would throw deep inside the hot path. The
        # exception (``redis.ConnectionError`` and friends) propagates so
        # the lifespan's try/except can fall back to InMemoryBackend.
        self._client.ping()
        logger.info("redis backend connected to %s:%s", host, port)

    # ------------------------------------------------------------------
    # Backend implementation
    # ------------------------------------------------------------------

    def get(self, key: str) -> str | None:
        """Return the str stored at ``key`` or ``None`` if absent."""
        return self._client.get(key)

    def set(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key`` with optional TTL in seconds.

        ``redis.Redis.set(..., ex=ttl_sec)`` is atomic: the key is written
        with the TTL in a single round-trip. ``ex=None`` is the canonical
        "no expiry" signal in redis-py.
        """
        self._client.set(key, value, ex=ttl_sec)

    def incr(self, key: str) -> int:
        """Atomically increment the counter at ``key`` and return the new value.

        Redis ``INCR`` auto-creates the key at 0 and increments to 1 if
        missing — exactly the contract our Protocol documents. Atomicity
        is guaranteed by Redis's single-threaded command loop.
        """
        return self._client.incr(key)

    def keys(self, prefix: str = "") -> list[str]:
        """Return every key whose name starts with ``prefix``.

        Implemented via ``SCAN_ITER`` — cursor-based, non-blocking, and
        production-safe. ``KEYS *`` would also work but is O(N) blocking
        and freezes Redis for the duration; we never use it.

        The ``count=1000`` hint asks the server to batch in chunks of
        ~1000 keys per cursor step — a sweet spot between round-trip
        overhead (smaller batches) and per-call latency (larger batches).
        Result correctness is independent of this hint.
        """
        return [k for k in self._client.scan_iter(match=f"{prefix}*", count=1000)]

    def close(self) -> None:
        """Close the underlying client (releases the connection pool).

        redis-py's ``close()`` is idempotent — safe to call multiple
        times. We don't swallow exceptions here because shutdown failures
        are themselves observable signal; the caller's ``finally`` block
        in the lifespan is the right place to handle them.
        """
        self._client.close()

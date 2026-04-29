"""Async Redis wrapper used by every part of the failover system.

This is the only module in the codebase that should reach for
``redis.asyncio`` directly. Everyone else goes through ``RedisClient``.

Three Redis keys are managed here:

* ``leader:lock``       — string, value = node_id of the current primary;
  acquired with ``SET NX EX``, renewed with a Lua script that checks the
  value before extending the TTL.
* ``heartbeat:primary`` — string, value = orjson-encoded HeartbeatMessage;
  written by the primary every HEARTBEAT_INTERVAL seconds with a TTL.
* ``state:snapshot``    — string, value = orjson-encoded StateSnapshot;
  rewritten by the primary every STATE_SYNC_INTERVAL seconds.
"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# --- Redis keys (single source of truth) ----------------------------------
LEADER_LOCK_KEY = "leader:lock"
HEARTBEAT_KEY = "heartbeat:primary"
SNAPSHOT_KEY = "state:snapshot"

# --- Lua scripts ----------------------------------------------------------
# Renew only if the lock value still matches our node_id. PEXPIRE keeps the
# semantics millisecond-precise even though we accept a seconds TTL on the
# Python side.
RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
"""

# Release only if we still hold the lock — never blow away someone else's.
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""


class RedisClient:
    """Thin async wrapper around ``redis.asyncio.Redis`` with the keys we use.

    All methods catch ``RedisError`` and log + return a safe default
    (``False`` / ``None`` / ``-2``). Retries are deliberately not done here
    — callers know more about whether a particular operation should be
    retried, backed off, or treated as a hard failure.
    """

    def __init__(self, host: str, port: int, node_id: str) -> None:
        self.host = host
        self.port = port
        self.node_id = node_id
        self._redis: Optional[aioredis.Redis] = None

    # --- lifecycle ---
    async def connect(self) -> None:
        """Open the underlying connection pool. Idempotent."""
        if self._redis is None:
            # decode_responses=False keeps everything in bytes — orjson gives
            # us bytes on the way out and we want byte-equality on round-trips.
            self._redis = aioredis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=False,
            )

    async def close(self) -> None:
        """Close the connection pool, releasing any sockets."""
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except RedisError as exc:
                logger.warning("redis close failed: %s", exc)
            finally:
                self._redis = None

    # --- internal helper ---
    def _client(self) -> aioredis.Redis:
        """Return the underlying ``Redis`` client, asserting we've connected."""
        if self._redis is None:
            raise RuntimeError("RedisClient.connect() must be awaited first")
        return self._redis

    # --- leader lock ---
    async def acquire_lock(self, ttl: int) -> bool:
        """Try to acquire ``leader:lock`` for this node via ``SET NX EX``."""
        try:
            result = await self._client().set(
                LEADER_LOCK_KEY,
                self.node_id.encode("utf-8"),
                nx=True,
                ex=ttl,
            )
            return bool(result)
        except RedisError as exc:
            logger.warning("acquire_lock failed: %s", exc)
            return False

    async def renew_lock(self, ttl: int) -> bool:
        """Atomically extend our lock's TTL iff we still own it.

        ``ttl`` is in seconds; we hand the Lua script milliseconds.
        """
        try:
            result = await self._client().eval(
                RENEW_LUA,
                1,
                LEADER_LOCK_KEY,
                self.node_id.encode("utf-8"),
                ttl * 1000,
            )
            return result == 1
        except RedisError as exc:
            logger.warning("renew_lock failed: %s", exc)
            return False

    async def release_lock(self) -> bool:
        """Atomically release the lock iff we still own it."""
        try:
            result = await self._client().eval(
                RELEASE_LUA,
                1,
                LEADER_LOCK_KEY,
                self.node_id.encode("utf-8"),
            )
            return result == 1
        except RedisError as exc:
            logger.warning("release_lock failed: %s", exc)
            return False

    async def read_lock_holder(self) -> Optional[str]:
        """Return the node_id currently holding ``leader:lock``, or None."""
        try:
            value = await self._client().get(LEADER_LOCK_KEY)
            if value is None:
                return None
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return str(value)
        except RedisError as exc:
            logger.warning("read_lock_holder failed: %s", exc)
            return None

    # --- heartbeat ---
    async def write_heartbeat(self, payload: bytes, ttl: int = 6) -> None:
        """Write the primary heartbeat with a fresh TTL."""
        try:
            await self._client().set(HEARTBEAT_KEY, payload, ex=ttl)
        except RedisError as exc:
            logger.warning("write_heartbeat failed: %s", exc)

    async def read_heartbeat(self) -> Optional[bytes]:
        """Return the latest heartbeat payload, or None if missing/expired."""
        try:
            value = await self._client().get(HEARTBEAT_KEY)
            if value is None:
                return None
            return value if isinstance(value, bytes) else bytes(value, "utf-8")
        except RedisError as exc:
            logger.warning("read_heartbeat failed: %s", exc)
            return None

    async def heartbeat_ttl(self) -> int:
        """Return PTTL of the heartbeat key in milliseconds.

        ``-2`` means the key is missing (Redis convention); ``-1`` means it
        exists but has no expiry (shouldn't happen for our usage).
        """
        try:
            ttl = await self._client().pttl(HEARTBEAT_KEY)
            return int(ttl)
        except RedisError as exc:
            logger.warning("heartbeat_ttl failed: %s", exc)
            return -2

    # --- state snapshot ---
    async def put_snapshot(self, payload: bytes) -> None:
        """Atomically replace the state snapshot."""
        try:
            await self._client().set(SNAPSHOT_KEY, payload)
        except RedisError as exc:
            logger.warning("put_snapshot failed: %s", exc)

    async def get_snapshot(self) -> Optional[bytes]:
        """Return the latest snapshot payload, or None if missing."""
        try:
            value = await self._client().get(SNAPSHOT_KEY)
            if value is None:
                return None
            return value if isinstance(value, bytes) else bytes(value, "utf-8")
        except RedisError as exc:
            logger.warning("get_snapshot failed: %s", exc)
            return None

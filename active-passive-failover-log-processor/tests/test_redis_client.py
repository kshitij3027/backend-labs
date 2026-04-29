"""Tests for src/redis_client.py — exercises real wrapper code over fakeredis."""

from __future__ import annotations

import asyncio

import pytest

from src.redis_client import (
    HEARTBEAT_KEY,
    LEADER_LOCK_KEY,
    SNAPSHOT_KEY,
    RedisClient,
)


# --- lock semantics -----------------------------------------------------


async def test_acquire_lock_first_succeeds_second_fails(
    fake_redis_client: RedisClient,
) -> None:
    assert await fake_redis_client.acquire_lock(ttl=6) is True
    # NX semantics: the second attempt by the same node must still fail
    # because the key already exists.
    assert await fake_redis_client.acquire_lock(ttl=6) is False


async def test_renew_lock_when_self_holds(fake_redis_client: RedisClient) -> None:
    assert await fake_redis_client.acquire_lock(ttl=6) is True
    assert await fake_redis_client.renew_lock(ttl=6) is True


async def test_renew_lock_when_other_holds(fake_redis_client: RedisClient) -> None:
    # Manually inject a different owner of the lock.
    await fake_redis_client._client().set(LEADER_LOCK_KEY, b"other-node")
    assert await fake_redis_client.renew_lock(ttl=6) is False


async def test_release_lock_when_self_holds(fake_redis_client: RedisClient) -> None:
    assert await fake_redis_client.acquire_lock(ttl=6) is True
    assert await fake_redis_client.release_lock() is True
    # Lock is gone after release.
    assert await fake_redis_client.read_lock_holder() is None


async def test_release_lock_no_op_when_other_holds(
    fake_redis_client: RedisClient,
) -> None:
    await fake_redis_client._client().set(LEADER_LOCK_KEY, b"other-node")
    assert await fake_redis_client.release_lock() is False
    # The other holder's value must still be there.
    assert await fake_redis_client.read_lock_holder() == "other-node"


async def test_read_lock_holder_returns_value(
    fake_redis_client: RedisClient,
) -> None:
    assert await fake_redis_client.read_lock_holder() is None
    await fake_redis_client.acquire_lock(ttl=6)
    assert await fake_redis_client.read_lock_holder() == "node-test"


# --- heartbeat ----------------------------------------------------------


async def test_heartbeat_round_trip(fake_redis_client: RedisClient) -> None:
    payload = b'{"node_id":"node-1","timestamp":1.0}'
    await fake_redis_client.write_heartbeat(payload, ttl=6)
    got = await fake_redis_client.read_heartbeat()
    assert got == payload


async def test_heartbeat_ttl_positive_after_write(
    fake_redis_client: RedisClient,
) -> None:
    await fake_redis_client.write_heartbeat(b"payload", ttl=6)
    ttl = await fake_redis_client.heartbeat_ttl()
    assert ttl > 0


async def test_heartbeat_ttl_minus_two_after_expiry(
    fake_redis_client: RedisClient,
) -> None:
    # Use a 1-second TTL on the underlying client so the test stays fast.
    await fake_redis_client._client().set(HEARTBEAT_KEY, b"payload", ex=1)
    await asyncio.sleep(1.1)
    ttl = await fake_redis_client.heartbeat_ttl()
    assert ttl == -2


async def test_heartbeat_ttl_minus_two_when_missing(
    fake_redis_client: RedisClient,
) -> None:
    ttl = await fake_redis_client.heartbeat_ttl()
    assert ttl == -2


# --- snapshot -----------------------------------------------------------


async def test_snapshot_round_trip(fake_redis_client: RedisClient) -> None:
    payload = b'{"version":1,"log_count":5}'
    await fake_redis_client.put_snapshot(payload)
    got = await fake_redis_client.get_snapshot()
    assert got == payload


async def test_snapshot_get_returns_none_when_missing(
    fake_redis_client: RedisClient,
) -> None:
    assert await fake_redis_client.get_snapshot() is None


# --- key constants are wired correctly ----------------------------------


async def test_keys_match_constants(fake_redis_client: RedisClient) -> None:
    """Sanity check: the wrapper writes to exactly the documented keys."""
    await fake_redis_client.acquire_lock(ttl=6)
    await fake_redis_client.write_heartbeat(b"hb", ttl=6)
    await fake_redis_client.put_snapshot(b"snap")
    keys = await fake_redis_client._client().keys("*")
    keyset = {k.decode("utf-8") if isinstance(k, bytes) else k for k in keys}
    assert LEADER_LOCK_KEY in keyset
    assert HEARTBEAT_KEY in keyset
    assert SNAPSHOT_KEY in keyset

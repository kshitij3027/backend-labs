"""Shared pytest fixtures for the active-passive failover suite."""

from __future__ import annotations

from typing import AsyncIterator

import fakeredis.aioredis as fake_aioredis
import pytest
import pytest_asyncio

from src.config import NodeConfig
from src.redis_client import RedisClient


@pytest.fixture
def node_config(monkeypatch: pytest.MonkeyPatch) -> NodeConfig:
    """A NodeConfig instance with NODE_ID forced via env."""
    # Wipe any other env that might have been set by the test runner so we
    # genuinely exercise defaults.
    for var in (
        "IS_PRIMARY",
        "PORT",
        "REDIS_HOST",
        "REDIS_PORT",
        "HEARTBEAT_INTERVAL",
        "HEARTBEAT_TIMEOUT",
        "ELECTION_TIMEOUT",
        "STATE_SYNC_INTERVAL",
        "LOCK_TTL",
        "PEER_NODES",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NODE_ID", "node-test")
    return NodeConfig()


@pytest_asyncio.fixture
async def fake_redis_client() -> AsyncIterator[RedisClient]:
    """Yield a RedisClient backed by an in-process fakeredis.

    We construct the client normally and then swap the underlying
    ``redis.asyncio.Redis`` for a ``FakeRedis`` so the rest of the wrapper
    code path (including Lua eval) is exercised.
    """
    client = RedisClient(host="localhost", port=6379, node_id="node-test")
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    client._redis = fake  # type: ignore[attr-defined]
    try:
        yield client
    finally:
        try:
            await fake.flushall()
        finally:
            await fake.aclose()
            client._redis = None  # type: ignore[attr-defined]

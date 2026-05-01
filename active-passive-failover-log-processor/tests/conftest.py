"""Shared pytest fixtures for the active-passive failover suite."""

from __future__ import annotations

from typing import AsyncIterator, Callable

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


@pytest_asyncio.fixture
async def shared_fakeredis_factory() -> AsyncIterator[Callable[[str], RedisClient]]:
    """Yield a factory that creates RedisClients sharing a fakeredis backend.

    Use this for multi-node integration tests where two nodes need to see
    the same Redis state without each spinning up a separate fake. In a
    real cluster every node has its own ``RedisClient`` connecting to the
    same Redis server; here we mimic that by sharing the underlying
    ``FakeRedis`` instance across multiple wrappers.

    The factory accepts a ``node_id`` and returns a fresh
    :class:`RedisClient` whose underlying ``_redis`` attribute is the
    shared fake. Each created client's per-node mutation
    (``acquire_lock``, ``release_lock``) flows through the shared backend
    and is visible to every other client constructed by the same factory.

    On teardown, every constructed client has its underlying handle
    detached (so ``close()`` becomes a no-op) and the shared backend is
    closed exactly once.
    """
    backend = fake_aioredis.FakeRedis(decode_responses=False)
    clients: list[RedisClient] = []

    def _make(node_id: str) -> RedisClient:
        client = RedisClient(host="fake", port=0, node_id=node_id)
        client._redis = backend  # type: ignore[assignment]
        clients.append(client)
        return client

    try:
        yield _make
    finally:
        # Detach every client so individual close() calls don't try to
        # close the shared backend twice.
        for c in clients:
            try:
                c._redis = None  # type: ignore[assignment]
            except Exception:
                pass
        try:
            await backend.flushall()
        finally:
            await backend.aclose()

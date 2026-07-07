"""Integration fixtures: a real Redis client, or a fast skip when none answers.

Inside the compose `test` service REDIS_URL points at the redis container; on a
bare host the localhost default is tried and every test using these fixtures
skips (quickly, thanks to the 2s socket timeouts) when nothing is listening.
"""

import os

import pytest
import redis


@pytest.fixture()
def redis_url() -> str:
    """The Redis URL integration tests (and their RedisStore instances) target."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture()
def redis_client(redis_url):
    """A verified-live Redis client over a clean database (flushed before use)."""
    client = redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    try:
        client.ping()
    except Exception:  # noqa: BLE001 — any failure means "no redis here"
        pytest.skip("redis unavailable")
    client.flushdb()
    yield client
    client.close()

import pytest
import fakeredis.aioredis

from src.config import Settings
from src.redis_client import RedisClient


@pytest.fixture
async def fake_redis():
    """Provide a fake async Redis client for testing."""
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def redis_client():
    """Provide a RedisClient backed by fakeredis (no real Redis needed)."""
    server = fakeredis.FakeServer()
    fr = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    client = RedisClient(Settings())
    client._redis = fr  # bypass connect()
    return client

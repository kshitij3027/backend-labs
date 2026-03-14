import pytest
import fakeredis.aioredis


@pytest.fixture
async def fake_redis():
    """Provide a fake async Redis client for testing."""
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server)
    yield client
    await client.aclose()

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.coordinator.app import app
from src.db import close_db, init_db
from src.redis_client import close_redis, init_redis


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def setup_services():
    """Initialize DB and Redis once for the entire test session."""
    await init_db()
    await init_redis()
    yield
    await close_redis()
    await close_db()


@pytest_asyncio.fixture(loop_scope="session")
async def test_client(setup_services):
    """Async HTTP client bound to the FastAPI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

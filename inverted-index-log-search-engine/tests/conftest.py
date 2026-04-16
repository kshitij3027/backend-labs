"""Shared test fixtures."""

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from backend.main import app


@pytest.fixture
async def client():
    """Async HTTP client wired to the FastAPI app via ASGI transport.

    Uses ``LifespanManager`` to ensure the application lifespan events
    (startup / shutdown) are triggered so that ``app.state`` is populated
    before any endpoint test runs.
    """
    async with LifespanManager(app) as manager:
        transport = ASGITransport(app=manager.app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

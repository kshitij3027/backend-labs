"""Integration tests for the FastAPI app surface.

These drive the app **through its lifespan** (via
``app.router.lifespan_context``) so ``app.state.settings`` is populated exactly
as it would be in production, then issue requests over an in-process
``httpx.ASGITransport`` — no network, no Redis, no Postgres required.

``pytest.ini`` sets ``asyncio_mode = auto``, so plain ``async def test_*``
functions run without an explicit ``@pytest.mark.asyncio`` decorator.
"""

from __future__ import annotations

import httpx

from src.main import app


async def test_health_returns_healthy() -> None:
    """GET /health returns 200 with the exact ``{"status": "healthy"}`` body."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


async def test_lifespan_populates_settings() -> None:
    """The lifespan attaches a Settings object to ``app.state``."""
    async with app.router.lifespan_context(app):
        assert app.state.settings is not None
        # api_port is a core Settings field; confirm the wiring is real.
        assert isinstance(app.state.settings.api_port, int)

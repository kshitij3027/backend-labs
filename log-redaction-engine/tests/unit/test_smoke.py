"""Smoke test for the C1 bootstrap.

Confirms the FastAPI app boots and ``/api/health`` returns the documented
JSON body. This is the only test that must pass for C1; richer endpoint
tests are layered on in later commits.

We drive the app via ``httpx.AsyncClient`` over an ``ASGITransport`` wrapped
in ``asgi_lifespan.LifespanManager`` so the FastAPI ``lifespan`` (none
declared yet in C1, but a no-op manager keeps the test forward-compatible
when one is added) runs end-to-end.
"""
from __future__ import annotations

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.main import app
from src.settings import get_settings


@pytest.mark.asyncio
async def test_health_endpoint_returns_expected_payload() -> None:
    """GET /api/health returns 200 with the documented service marker."""
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "log-redaction-engine",
    }


def test_settings_loads_with_test_salt() -> None:
    """``Settings()`` constructs cleanly with the conftest-injected salt.

    This also exercises the ``src.settings`` module so the coverage gate
    sees both files in the ``src`` package executed during the smoke run.
    """
    settings = get_settings()
    assert settings.REDACTION_HASH_SALT  # truthy 64-char hex from conftest
    assert settings.PORT == 8000
    assert settings.REDACTION_PRESET == "general"

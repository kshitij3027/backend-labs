"""Smoke test for the C1 bootstrap.

Confirms the FastAPI app boots and `/api/health` returns the documented
JSON body. This is the only test that must pass for C1; everything else
gets layered on in later commits.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_health_endpoint_returns_expected_payload() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "field-encryption-service",
    }

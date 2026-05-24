"""Integration smoke test: hit GET /api/health via an in-process ASGI client.

Uses ``httpx.ASGITransport`` (the supported pattern in httpx 0.27+; the
older ``AsyncClient(app=app)`` form is deprecated) so this test runs
without binding a real port.
"""
from __future__ import annotations

import httpx
import pytest

from src.main import app


@pytest.mark.asyncio
async def test_health_returns_healthy_with_int_timestamp():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert isinstance(body["timestamp"], int)
    assert body["timestamp"] > 0

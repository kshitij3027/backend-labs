from __future__ import annotations

import os

import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.skipif(
    os.getenv("API_URL") is None,
    reason="set API_URL env var",
)


async def test_live_health_basic(live_url: str) -> None:
    async with AsyncClient(base_url=live_url, timeout=10.0) as client:
        response = await client.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "timestamp" in body


async def test_live_health_detailed(live_url: str) -> None:
    async with AsyncClient(base_url=live_url, timeout=10.0) as client:
        response = await client.get("/api/v1/health/detailed")
    assert response.status_code == 200
    body = response.json()
    assert "dependencies" in body
    assert "elasticsearch" in body["dependencies"]
    assert "redis" in body["dependencies"]

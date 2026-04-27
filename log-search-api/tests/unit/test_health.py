from __future__ import annotations

from httpx import AsyncClient


async def test_health_basic_returns_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "timestamp" in body


async def test_health_detailed_returns_placeholder_dependencies(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/health/detailed")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "timestamp" in body
    assert body["dependencies"]["elasticsearch"] == "unknown"
    assert body["dependencies"]["redis"] == "unknown"

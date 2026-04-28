from __future__ import annotations

from httpx import AsyncClient


async def test_health_basic_returns_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "timestamp" in body


async def test_health_detailed_reports_dependency_statuses(async_client: AsyncClient) -> None:
    """Uses dependency_overrides in conftest to inject fake AsyncMock ES + Redis clients."""
    response = await async_client.get("/api/v1/health/detailed")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "timestamp" in body
    assert body["dependencies"]["elasticsearch"] == "ok"
    assert body["dependencies"]["redis"] == "ok"

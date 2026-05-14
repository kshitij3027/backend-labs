"""Smoke test for /health — also exercises the build_app() factory + lifespan."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_endpoint_returns_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_root_path_is_not_health(async_client: AsyncClient) -> None:
    """Sanity: the / path is NOT health (FastAPI returns 404 for undefined routes)."""
    response = await async_client.get("/")
    assert response.status_code == 404

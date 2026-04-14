"""Tests for the FastAPI endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


class TestHealthEndpoint:
    """Verify GET /health returns expected shape and values."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_status_healthy(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        data = resp.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_health_redis_connected(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        data = resp.json()
        # fakeredis always responds to ping, so this should be True.
        assert data["redis_connected"] is True


class TestDashboardEndpoint:
    """Verify GET / serves HTML content."""

    @pytest.mark.asyncio
    async def test_dashboard_returns_200(self, client: AsyncClient) -> None:
        resp = await client.get("/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_dashboard_returns_html(self, client: AsyncClient) -> None:
        resp = await client.get("/")
        assert "text/html" in resp.headers.get("content-type", "")

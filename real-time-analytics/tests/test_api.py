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


class TestServicesEndpoint:
    """Verify GET /api/services returns service list."""

    @pytest.mark.asyncio
    async def test_get_services(self, client: AsyncClient) -> None:
        # Generate sample data first so at least one service exists
        await client.post("/api/generate-sample-data?service=web-api&count=5")
        resp = await client.get("/api/services")
        assert resp.status_code == 200
        data = resp.json()
        assert "services" in data
        assert isinstance(data["services"], list)
        assert len(data["services"]) > 0
        assert "web-api" in data["services"]


class TestExportEndpoint:
    """Verify GET /api/export returns CSV and JSON exports."""

    @pytest.mark.asyncio
    async def test_export_csv(self, client: AsyncClient) -> None:
        # Generate sample data first
        await client.post("/api/generate-sample-data?service=web-api&count=10")
        resp = await client.get(
            "/api/export",
            params={"service": "web-api", "metric_name": "response_time", "minutes": 60, "format": "csv"},
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers.get("content-type", "")
        assert "attachment" in resp.headers.get("content-disposition", "")
        assert ".csv" in resp.headers.get("content-disposition", "")
        # Verify CSV content has header row
        content = resp.text
        lines = content.strip().split("\n")
        assert len(lines) >= 1
        assert "timestamp" in lines[0]
        assert "value" in lines[0]

    @pytest.mark.asyncio
    async def test_export_json(self, client: AsyncClient) -> None:
        # Generate sample data first
        await client.post("/api/generate-sample-data?service=web-api&count=10")
        resp = await client.get(
            "/api/export",
            params={"service": "web-api", "metric_name": "response_time", "minutes": 60, "format": "json"},
        )
        assert resp.status_code == 200
        assert "application/json" in resp.headers.get("content-type", "")
        assert "attachment" in resp.headers.get("content-disposition", "")
        data = resp.json()
        assert "service" in data
        assert "data_points" in data
        assert data["service"] == "web-api"
        assert data["metric_name"] == "response_time"

    @pytest.mark.asyncio
    async def test_export_missing_params(self, client: AsyncClient) -> None:
        # Missing both service and metric_name
        resp = await client.get("/api/export")
        assert resp.status_code == 422

        # Missing metric_name
        resp = await client.get("/api/export", params={"service": "web-api"})
        assert resp.status_code == 422

        # Missing service
        resp = await client.get("/api/export", params={"metric_name": "response_time"})
        assert resp.status_code == 422

"""Tests for FastAPI API endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import ConsumerStats, LogEntry


@pytest.fixture
async def setup_app():
    """Set up app with mocked dependencies (no real Redis)."""
    import src.app as app_module
    from src.config import Config
    from src.health import HealthMonitor
    from src.metrics import MetricsAggregator

    # Create real metrics + health, mock consumer manager
    config = Config()
    metrics = MetricsAggregator(window_sec=300)

    mock_manager = MagicMock()
    mock_manager.get_consumer_stats.return_value = [
        ConsumerStats(
            consumer_id="test-worker-0",
            processed_count=10,
            error_count=0,
            success_rate=1.0,
            last_active=datetime.now(timezone.utc),
        )
    ]

    health_monitor = HealthMonitor(mock_manager.get_consumer_stats)

    # Override module globals
    app_module.config = config
    app_module.metrics = metrics
    app_module.consumer_manager = mock_manager
    app_module.health_monitor = health_monitor

    # Seed some metrics data
    entry = LogEntry(
        ip="10.0.0.1",
        method="GET",
        path="/api/users",
        status_code=200,
        response_size=1234,
        response_time_ms=45.0,
        raw="test",
    )
    await metrics.record(entry)

    yield app_module.app, metrics, mock_manager


@pytest.fixture
async def client(setup_app):
    """Create async test client that bypasses lifespan."""
    app, _, _ = setup_app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_stats_endpoint(client):
    resp = await client.get("/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_processed"] >= 1
    assert "consumers" in data
    assert "endpoints" in data


@pytest.mark.asyncio
async def test_requests_endpoint(client):
    resp = await client.get("/api/stats/requests")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_processed" in data
    assert "requests_per_second" in data


@pytest.mark.asyncio
async def test_status_codes_endpoint(client):
    resp = await client.get("/api/stats/status-codes")
    assert resp.status_code == 200
    data = resp.json()
    assert "status_codes" in data


@pytest.mark.asyncio
async def test_top_paths_endpoint(client):
    resp = await client.get("/api/stats/top-paths")
    assert resp.status_code == 200
    data = resp.json()
    assert "top_paths" in data


@pytest.mark.asyncio
async def test_top_ips_endpoint(client):
    resp = await client.get("/api/stats/top-ips")
    assert resp.status_code == 200
    data = resp.json()
    assert "top_ips" in data


@pytest.mark.asyncio
async def test_latency_endpoint(client):
    resp = await client.get("/api/stats/latency")
    assert resp.status_code == 200
    data = resp.json()
    assert "latency_percentiles" in data


@pytest.mark.asyncio
async def test_errors_endpoint(client):
    resp = await client.get("/api/stats/errors")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_errors" in data
    assert "error_rate" in data

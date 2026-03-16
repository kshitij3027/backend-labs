"""Tests for the FastAPI dashboard endpoints."""

import pytest
from unittest.mock import MagicMock, patch

from httpx import AsyncClient, ASGITransport

import src.dashboard as dashboard_module
from src.dashboard import create_app
from src.log_generator import LogGenerator
from src.websocket_manager import ConnectionManager


@pytest.fixture
async def client(tmp_path, monkeypatch):
    """Create an async test client with a mocked Kafka producer.

    ASGITransport does not trigger ASGI lifespan events, so we set
    the module-level state that the lifespan would normally populate.
    """
    # Build a mock producer
    mock_producer = MagicMock()
    mock_producer.stats = {
        "total_sent": 5,
        "total_failed": 0,
        "topic_counts": {"logs-application": 3, "logs-errors": 2},
        "partition_counts": {"logs-application-0": 3},
        "success_rate": 100.0,
        "metrics": {
            "total_sent": 5,
            "total_failed": 0,
            "topic_counts": {"logs-application": 3, "logs-errors": 2},
            "throughput": 2.5,
            "error_counts": {},
            "error_rate": 0.0,
        },
    }
    mock_producer.send_logs_batch.return_value = {"sent": 10, "failed": 0}
    mock_producer._closed = False
    mock_producer.close = MagicMock()
    mock_producer.flush = MagicMock(return_value=0)

    # Inject module-level state directly
    dashboard_module._producer = mock_producer
    dashboard_module._generator = LogGenerator()
    dashboard_module._manager = ConnectionManager()

    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    # Cleanup
    dashboard_module._producer = None
    dashboard_module._generator = None
    dashboard_module._manager = None
    dashboard_module._config = None


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    """GET /health returns 200 with status ok."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["producer_connected"] is True


@pytest.mark.asyncio
async def test_dashboard_page(client: AsyncClient):
    """GET / returns 200 with HTML containing the dashboard title."""
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "Kafka Log Producer" in resp.text


@pytest.mark.asyncio
async def test_send_sample(client: AsyncClient):
    """POST /api/send-sample returns 200 with logs_sent field."""
    resp = await client.post("/api/send-sample")
    assert resp.status_code == 200
    body = resp.json()
    assert body["logs_sent"] == 10


@pytest.mark.asyncio
async def test_send_error_burst(client: AsyncClient):
    """POST /api/send-error-burst returns 200."""
    resp = await client.post("/api/send-error-burst")
    assert resp.status_code == 200
    body = resp.json()
    assert body["logs_sent"] == 5


@pytest.mark.asyncio
async def test_stats_endpoint(client: AsyncClient):
    """GET /api/stats returns 200 with expected metrics fields."""
    resp = await client.get("/api/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_sent"] == 5
    assert body["total_failed"] == 0
    assert "topic_counts" in body
    assert "metrics" in body

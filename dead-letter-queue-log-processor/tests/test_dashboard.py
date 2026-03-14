"""Tests for the aiohttp web dashboard."""

import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from src.config import Settings
from src.dashboard import Dashboard
from src.models import FailedMessage, FailureType, LogLevel, LogMessage


@pytest.fixture
async def client(redis_client):
    """Create an aiohttp test client backed by fake Redis."""
    settings = Settings()
    dashboard = Dashboard(redis_client, settings)
    server = TestServer(dashboard.app)
    tc = TestClient(server)
    await tc.start_server()
    yield tc
    await tc.close()


# ------------------------------------------------------------------
# Health & Index
# ------------------------------------------------------------------


async def test_health(client):
    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"
    assert "timestamp" in data


async def test_index(client):
    resp = await client.get("/")
    assert resp.status == 200
    text = await resp.text()
    assert "Dead Letter Queue" in text


# ------------------------------------------------------------------
# API: Stats
# ------------------------------------------------------------------


async def test_api_stats(client):
    resp = await client.get("/api/stats")
    assert resp.status == 200
    data = await resp.json()
    assert "dlq_size" in data
    assert "queue_length" in data


# ------------------------------------------------------------------
# API: DLQ (empty)
# ------------------------------------------------------------------


async def test_api_dlq_empty(client):
    resp = await client.get("/api/dlq")
    assert resp.status == 200
    data = await resp.json()
    assert data == []


async def test_api_dlq_analysis_empty(client):
    resp = await client.get("/api/dlq/analysis")
    assert resp.status == 200
    data = await resp.json()
    assert data["total"] == 0


# ------------------------------------------------------------------
# API: DLQ with messages
# ------------------------------------------------------------------


async def test_api_dlq_with_messages(client, redis_client):
    """Seed a DLQ message and verify it shows up."""
    msg = FailedMessage(
        original_message=LogMessage(
            id="test-1",
            source="api-gw",
            level=LogLevel.ERROR,
            message="boom",
        ),
        failure_type=FailureType.NETWORK,
        error_details="Connection refused",
        retry_count=3,
    )
    await redis_client.move_to_dlq(msg.to_json())

    resp = await client.get("/api/dlq")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["failure_type"] == "NETWORK"
    assert data[0]["original_message"]["source"] == "api-gw"


# ------------------------------------------------------------------
# API: DLQ Actions
# ------------------------------------------------------------------


async def test_api_purge(client):
    resp = await client.post("/api/dlq/purge")
    assert resp.status == 200
    data = await resp.json()
    assert "purged" in data


async def test_api_reprocess(client):
    resp = await client.post("/api/dlq/reprocess")
    assert resp.status == 200
    data = await resp.json()
    assert "reprocessed" in data


async def test_api_reprocess_by_valid_type(client, redis_client):
    """Seed a message and reprocess by its failure type."""
    msg = FailedMessage(
        original_message=LogMessage(id="r-1", source="svc", message="err"),
        failure_type=FailureType.PARSING,
        error_details="bad format",
        retry_count=3,
    )
    await redis_client.move_to_dlq(msg.to_json())

    resp = await client.post("/api/dlq/reprocess/parsing")
    assert resp.status == 200
    data = await resp.json()
    assert data["reprocessed"] == 1
    assert data["failure_type"] == "PARSING"


async def test_api_reprocess_invalid_type(client):
    resp = await client.post("/api/dlq/reprocess/INVALID")
    assert resp.status == 400
    data = await resp.json()
    assert "error" in data


# ------------------------------------------------------------------
# API: Trends & Alerts
# ------------------------------------------------------------------


async def test_api_trends(client):
    resp = await client.get("/api/trends")
    assert resp.status == 200
    data = await resp.json()
    assert "total_failures" in data
    assert "by_type" in data


async def test_api_trends_custom_window(client):
    resp = await client.get("/api/trends?window=60")
    assert resp.status == 200
    data = await resp.json()
    assert data["window_seconds"] == 60.0


async def test_api_alerts(client):
    resp = await client.get("/api/alerts")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)

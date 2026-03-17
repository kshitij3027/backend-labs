"""Tests for the FastAPI dashboard."""
import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.analytics import AnalyticsEngine
from src.batch_processor import BatchProcessor
from src.config import Settings
from src.consumer import LogConsumer
from src.dashboard import create_app
from src.redis_store import RedisStore
from src.websocket_manager import ConnectionManager


@pytest.fixture
def mock_consumer():
    consumer = MagicMock(spec=LogConsumer)
    consumer.is_running = True
    consumer.stats = {
        "is_running": True,
        "total_consumed": 100,
        "total_committed": 100,
        "total_errors": 0,
        "batches_processed": 2,
        "uptime_seconds": 30.0,
        "throughput": 3.3,
        "assigned_partitions": 9,
        "current_batch_size": 0,
    }
    consumer.assigned_partitions = []
    return consumer


@pytest.fixture
def mock_redis():
    store = MagicMock(spec=RedisStore)
    store.is_connected = True
    store.ping.return_value = True
    return store


@pytest.fixture
def app(mock_consumer, mock_redis):
    settings = Settings(
        bootstrap_servers="localhost:9092",
        redis_host="localhost",
    )
    analytics = AnalyticsEngine()
    processor = BatchProcessor(analytics=analytics)
    app = create_app(settings, analytics, mock_consumer, processor, mock_redis)

    # Set module-level state directly (ASGITransport doesn't trigger lifespan)
    import src.dashboard as dashboard_module
    dashboard_module._settings = settings
    dashboard_module._analytics = analytics
    dashboard_module._consumer = mock_consumer
    dashboard_module._processor = processor
    dashboard_module._redis_store = mock_redis
    dashboard_module._manager = ConnectionManager()

    return app


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
class TestHealthEndpoint:
    async def test_health_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "consumer_running" in body
        assert "redis_connected" in body


@pytest.mark.asyncio
class TestStatsEndpoint:
    async def test_stats(self, client):
        resp = await client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "consumer" in body
        assert "processor" in body
        assert "analytics" in body


@pytest.mark.asyncio
class TestAnalyticsEndpoint:
    async def test_analytics(self, client):
        resp = await client.get("/api/analytics")
        assert resp.status_code == 200
        body = resp.json()
        assert "percentiles" in body
        assert "endpoints" in body
        assert "geo_distribution" in body


@pytest.mark.asyncio
class TestMetricsEndpoint:
    async def test_metrics(self, client):
        resp = await client.get("/api/metrics")
        assert resp.status_code == 200
        body = resp.json()
        assert "throughput_history" in body
        assert "total_messages" in body


@pytest.mark.asyncio
class TestDashboardPage:
    async def test_dashboard(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "Kafka Log Consumer" in resp.text


@pytest.mark.asyncio
class TestWebSocket:
    async def test_websocket_connects(self, app):
        from starlette.testclient import TestClient
        with TestClient(app) as tc:
            with tc.websocket_connect("/ws") as ws:
                # WebSocket should connect successfully
                # Send a keepalive and expect data from broadcast
                pass  # Connection itself is the test

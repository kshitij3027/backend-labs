"""API integration tests using fakeredis."""

from __future__ import annotations

from datetime import datetime, timezone

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient

from src.aggregator import Aggregator
from src.api import app
from src.config import AppConfig, WindowTypeConfig
from src.timestamp_parser import TimestampParser
from src.websocket import ConnectionManager
from src.window_manager import WindowManager
from src.window_rotator import WindowRotator


@pytest_asyncio.fixture
async def client():
    """Provide an async HTTP client with fakeredis-backed app state."""
    fake_redis = fakeredis.aioredis.FakeRedis()
    config = AppConfig(
        redis_host="localhost",
        redis_port=6379,
        log_level="DEBUG",
        window_types=[
            WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600),
            WindowTypeConfig(name="1h", size_seconds=3600, grace_period_seconds=300, retention_seconds=86400),
            WindowTypeConfig(name="order_5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600),
            WindowTypeConfig(name="revenue_1h", size_seconds=3600, grace_period_seconds=300, retention_seconds=86400),
        ],
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Override app state after lifespan has run
        app.state.config = config
        app.state.redis = fake_redis
        app.state.window_manager = WindowManager(fake_redis, config)
        app.state.aggregator = Aggregator(fake_redis)
        app.state.rotator = WindowRotator(fake_redis, config)
        app.state.ts_parser = TimestampParser()
        app.state.start_time = int(datetime.now(timezone.utc).timestamp())
        app.state.ws_manager = ConnectionManager()
        yield ac

    await fake_redis.aclose()


def _make_event(level: str = "INFO", source: str = "test-svc", response_time: float | None = None) -> dict:
    """Build a log event dict with current timestamp."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "source": source,
        "message": f"Test log message at {level}",
        "response_time": response_time,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_check(client: AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_ingest_single_event(client: AsyncClient) -> None:
    event = _make_event(level="WARN", response_time=42.5)
    r = await client.post("/api/v1/logs", json=event)
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] >= 1
    assert body["errors"] == []


@pytest.mark.asyncio
async def test_ingest_batch(client: AsyncClient) -> None:
    events = [_make_event(level=lv, source=f"svc-{i}") for i, lv in enumerate(["INFO", "WARN", "ERROR", "DEBUG", "INFO"])]
    r = await client.post("/api/v1/logs/batch", json={"events": events})
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 5
    assert body["accepted"] >= 5


@pytest.mark.asyncio
async def test_ingest_invalid_timestamp(client: AsyncClient) -> None:
    event = {
        "timestamp": "not-a-real-timestamp!!!",
        "level": "INFO",
        "source": "test",
        "message": "bad ts",
    }
    r = await client.post("/api/v1/logs", json=event)
    assert r.status_code == 200
    body = r.json()
    assert len(body["errors"]) > 0


@pytest.mark.asyncio
async def test_get_windows_5m(client: AsyncClient) -> None:
    # Ingest 3 events
    for _ in range(3):
        await client.post("/api/v1/logs", json=_make_event())

    r = await client.get("/api/v1/windows/5m")
    assert r.status_code == 200
    body = r.json()
    assert body["window_type"] == "5m"
    assert body["count"] >= 1
    # The window should have at least 3 events
    total_count = sum(w["metrics"]["count"] for w in body["windows"] if w.get("metrics"))
    assert total_count >= 3


@pytest.mark.asyncio
async def test_get_windows_invalid_type(client: AsyncClient) -> None:
    r = await client.get("/api/v1/windows/invalid")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_stats(client: AsyncClient) -> None:
    # Ingest a couple of events first
    await client.post("/api/v1/logs", json=_make_event())
    await client.post("/api/v1/logs", json=_make_event(level="ERROR"))

    r = await client.get("/api/v1/stats")
    assert r.status_code == 200
    body = r.json()
    assert "uptime_seconds" in body
    assert "total_events" in body
    assert "active_windows" in body
    assert "window_types" in body
    assert body["total_events"] >= 2


@pytest.mark.asyncio
async def test_dashboard_serves_html(client: AsyncClient) -> None:
    r = await client.get("/dashboard")
    assert r.status_code == 200
    text = r.text.lower()
    assert "chart.js" in text or "dashboard" in text


@pytest.mark.asyncio
async def test_ecommerce_order_tracking(client: AsyncClient) -> None:
    """Ingest events with order_id and order_value, verify e-commerce metrics."""
    for i in range(5):
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "source": "order-svc",
            "message": f"Order placed #{i}",
            "order_id": f"ORD-{i:04d}",
            "order_value": 25.50 + i * 10,
            "order_status": "placed",
        }
        r = await client.post("/api/v1/logs", json=event)
        assert r.status_code == 200
        assert r.json()["accepted"] >= 1

    # Check order_5m e-commerce endpoint
    r = await client.get("/api/v1/windows/order_5m/ecommerce")
    assert r.status_code == 200
    body = r.json()
    assert body["window_type"] == "order_5m"
    assert body["count"] >= 1
    total_orders = sum(w["order_count"] for w in body["windows"])
    assert total_orders >= 5
    total_revenue = sum(w["total_revenue"] for w in body["windows"])
    assert total_revenue > 0


@pytest.mark.asyncio
async def test_replay_endpoint(client: AsyncClient) -> None:
    """POST /api/v1/replay with historical events, verify response."""
    events = [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "source": "replay-svc",
            "message": f"Replay event #{i}",
        }
        for i in range(5)
    ]
    r = await client.post("/api/v1/replay", json={
        "start_time": "2024-01-01T00:00:00Z",
        "end_time": "2024-01-01T01:00:00Z",
        "events": events,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["events_processed"] == 5
    assert body["windows_created"] >= 1
    assert body["errors"] == []


@pytest.mark.asyncio
async def test_replay_invalid_event(client: AsyncClient) -> None:
    """Replay with bad timestamp, verify error in response."""
    events = [
        {
            "timestamp": "not-a-timestamp!!!",
            "level": "INFO",
            "source": "replay-svc",
            "message": "Bad event",
        },
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "source": "replay-svc",
            "message": "Good event",
        },
    ]
    r = await client.post("/api/v1/replay", json={
        "start_time": "2024-01-01T00:00:00Z",
        "end_time": "2024-01-01T01:00:00Z",
        "events": events,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["events_processed"] == 1
    assert len(body["errors"]) == 1


def test_websocket_connection() -> None:
    """Test WebSocket connection via Starlette sync TestClient."""
    # Set up app state for sync test client
    fake_redis = fakeredis.aioredis.FakeRedis()
    config = AppConfig(
        redis_host="localhost",
        redis_port=6379,
        log_level="DEBUG",
        window_types=[
            WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600),
            WindowTypeConfig(name="order_5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600),
            WindowTypeConfig(name="revenue_1h", size_seconds=3600, grace_period_seconds=300, retention_seconds=86400),
        ],
    )
    app.state.config = config
    app.state.redis = fake_redis
    app.state.window_manager = WindowManager(fake_redis, config)
    app.state.aggregator = Aggregator(fake_redis)
    app.state.rotator = WindowRotator(fake_redis, config)
    app.state.ts_parser = TimestampParser()
    app.state.start_time = int(datetime.now(timezone.utc).timestamp())
    app.state.ws_manager = ConnectionManager()

    sync_client = TestClient(app)
    with sync_client.websocket_connect("/ws/dashboard") as ws:
        # Connection accepted — send a ping and verify no crash
        ws.send_text("ping")
        # If we get here without exception, the connection works
        assert True

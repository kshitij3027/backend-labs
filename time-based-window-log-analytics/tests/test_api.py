"""API integration tests using fakeredis."""

from __future__ import annotations

from datetime import datetime, timezone

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from src.aggregator import Aggregator
from src.api import app
from src.config import AppConfig, WindowTypeConfig
from src.timestamp_parser import TimestampParser
from src.window_manager import WindowManager
from src.window_rotator import WindowRotator


@pytest.fixture
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

"""Tests for the Aggregator class."""

from __future__ import annotations

from datetime import datetime, timezone

import fakeredis.aioredis
import pytest

from src.aggregator import Aggregator
from src.models import LogEvent


def _make_event(
    level: str = "INFO",
    source: str = "test-svc",
    response_time: float | None = None,
) -> LogEvent:
    return LogEvent(
        timestamp="2026-03-24T10:00:00Z",
        level=level,
        source=source,
        message="test message",
        response_time=response_time,
    )


WINDOW_KEY = "window:5m:300:1742810400"
PARSED_TS = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def redis_client():
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def aggregator(redis_client):
    return Aggregator(redis_client)


async def _seed_window(redis_client, key: str = WINDOW_KEY) -> None:
    """Pre-create a minimal window hash so HINCRBY works on existing fields."""
    await redis_client.hset(key, mapping={
        "count": 0,
        "error_count": 0,
        "total_response_time": 0,
        "sum_response_time_sq": 0,
        "levels": "{}",
        "services": "{}",
    })


class TestRecordEvent:
    @pytest.mark.asyncio
    async def test_record_event_increments_count(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(), PARSED_TS)
        count = await redis_client.hget(WINDOW_KEY, "count")
        assert int(count) == 1

    @pytest.mark.asyncio
    async def test_record_multiple_events(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        for _ in range(5):
            await aggregator.record_event(WINDOW_KEY, _make_event(), PARSED_TS)
        count = await redis_client.hget(WINDOW_KEY, "count")
        assert int(count) == 5

    @pytest.mark.asyncio
    async def test_error_event_tracking(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(level="ERROR"), PARSED_TS)
        error_count = await redis_client.hget(WINDOW_KEY, "error_count")
        assert int(error_count) == 1

    @pytest.mark.asyncio
    async def test_non_error_not_counted(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(level="INFO"), PARSED_TS)
        error_count = await redis_client.hget(WINDOW_KEY, "error_count")
        assert int(error_count) == 0

    @pytest.mark.asyncio
    async def test_response_time_aggregation(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(response_time=100.0), PARSED_TS)
        await aggregator.record_event(WINDOW_KEY, _make_event(response_time=200.0), PARSED_TS)

        total_rt = float(await redis_client.hget(WINDOW_KEY, "total_response_time"))
        assert total_rt == pytest.approx(300.0)

        metrics = await aggregator.get_window_metrics(WINDOW_KEY, 300)
        assert metrics is not None
        assert metrics.avg_response_time == pytest.approx(150.0)


class TestFieldTracking:
    @pytest.mark.asyncio
    async def test_service_tracking(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(source="api"), PARSED_TS)
        await aggregator.record_event(WINDOW_KEY, _make_event(source="api"), PARSED_TS)
        await aggregator.record_event(WINDOW_KEY, _make_event(source="worker"), PARSED_TS)

        metrics = await aggregator.get_window_metrics(WINDOW_KEY, 300)
        assert metrics is not None
        assert metrics.services == {"api": 2, "worker": 1}

    @pytest.mark.asyncio
    async def test_level_tracking(self, redis_client, aggregator: Aggregator) -> None:
        await _seed_window(redis_client)
        await aggregator.record_event(WINDOW_KEY, _make_event(level="INFO"), PARSED_TS)
        await aggregator.record_event(WINDOW_KEY, _make_event(level="ERROR"), PARSED_TS)
        await aggregator.record_event(WINDOW_KEY, _make_event(level="INFO"), PARSED_TS)

        metrics = await aggregator.get_window_metrics(WINDOW_KEY, 300)
        assert metrics is not None
        assert metrics.levels == {"INFO": 2, "ERROR": 1}


class TestGetMetrics:
    @pytest.mark.asyncio
    async def test_get_window_metrics_empty(self, aggregator: Aggregator) -> None:
        result = await aggregator.get_window_metrics("nonexistent:key", 300)
        assert result is None


class TestActiveWindows:
    @pytest.mark.asyncio
    async def test_get_active_windows(self, redis_client, aggregator: Aggregator) -> None:
        await redis_client.zadd("windows:active:5m", {"window:5m:300:100": 100, "window:5m:300:400": 400})
        windows = await aggregator.get_active_windows("5m")
        assert len(windows) == 2
        assert "window:5m:300:100" in windows
        assert "window:5m:300:400" in windows

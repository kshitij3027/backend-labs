"""Tests for the WindowManager class."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from src.config import AppConfig, WindowTypeConfig
from src.models import LogEvent, WindowState
from src.window_manager import WindowManager


def _make_event(
    ts: str = "2026-03-24T10:03:45Z",
    level: str = "INFO",
    source: str = "test-svc",
    message: str = "test message",
    response_time: float | None = None,
) -> LogEvent:
    return LogEvent(
        timestamp=ts, level=level, source=source, message=message, response_time=response_time
    )


def _make_config(window_types: list[WindowTypeConfig] | None = None) -> AppConfig:
    if window_types is None:
        window_types = [
            WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600),
            WindowTypeConfig(name="1h", size_seconds=3600, grace_period_seconds=300, retention_seconds=86400),
            WindowTypeConfig(name="1d", size_seconds=86400, grace_period_seconds=600, retention_seconds=604800),
        ]
    return AppConfig(window_types=window_types)


@pytest.fixture
def redis_client():
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def wm(redis_client):
    return WindowManager(redis_client, _make_config())


class TestAlignToWindow:
    def test_align_to_window_5min(self, wm: WindowManager) -> None:
        """10:03:45 should align to 10:00:00 for a 300s window."""
        ts = datetime(2026, 3, 24, 10, 3, 45, tzinfo=timezone.utc)
        aligned = wm.align_to_window(ts, 300)
        expected = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)
        assert aligned == int(expected.timestamp())

    def test_align_to_window_1hour(self, wm: WindowManager) -> None:
        """10:23:00 should align to 10:00:00 for a 3600s window."""
        ts = datetime(2026, 3, 24, 10, 23, 0, tzinfo=timezone.utc)
        aligned = wm.align_to_window(ts, 3600)
        expected = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)
        assert aligned == int(expected.timestamp())

    def test_align_to_window_exact_boundary(self, wm: WindowManager) -> None:
        """Timestamp exactly on a boundary should stay the same."""
        ts = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)
        aligned = wm.align_to_window(ts, 300)
        assert aligned == int(ts.timestamp())


class TestWindowKey:
    def test_window_key_format(self, wm: WindowManager) -> None:
        key = wm.get_window_key("5m", 300, 1742810400)
        assert key == "window:5m:300:1742810400"


class TestWindowState:
    def test_window_state_active(self) -> None:
        """Window that started 100s ago with size 300 should be ACTIVE."""
        now = int(time.time())
        start_ts = now - 100
        wt = WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600)
        wm = WindowManager(None, _make_config())
        assert wm.get_window_state(start_ts, wt) == WindowState.ACTIVE

    def test_window_state_grace(self) -> None:
        """Window ended 30s ago with grace=60 should be GRACE."""
        now = int(time.time())
        # end_ts = start_ts + 300, so start_ts = now - 300 - 30
        start_ts = now - 300 - 30
        wt = WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600)
        wm = WindowManager(None, _make_config())
        assert wm.get_window_state(start_ts, wt) == WindowState.GRACE

    def test_window_state_closed(self) -> None:
        """Window ended 200s ago with grace=60 should be CLOSED."""
        now = int(time.time())
        start_ts = now - 300 - 200
        wt = WindowTypeConfig(name="5m", size_seconds=300, grace_period_seconds=60, retention_seconds=3600)
        wm = WindowManager(None, _make_config())
        assert wm.get_window_state(start_ts, wt) == WindowState.CLOSED


class TestAssignEvent:
    @pytest.mark.asyncio
    async def test_assign_event_creates_windows(self, redis_client) -> None:
        """Ingesting an event with a current timestamp should create windows for all 3 types."""
        now = datetime.now(tz=timezone.utc)
        event = _make_event(ts=now.isoformat())
        config = _make_config()
        wm = WindowManager(redis_client, config)

        results = await wm.assign_event(event, now)
        assert len(results) == 3
        for r in results:
            assert r["accepted"] is True
            assert r["state"] in (WindowState.ACTIVE, WindowState.GRACE)
            # Verify the window hash exists in Redis
            exists = await redis_client.exists(r["window_key"])
            assert exists

    @pytest.mark.asyncio
    async def test_assign_event_rejects_closed(self, redis_client) -> None:
        """An event with a very old timestamp should be rejected by all windows."""
        old_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        event = _make_event(ts=old_ts.isoformat())
        config = _make_config()
        wm = WindowManager(redis_client, config)

        results = await wm.assign_event(event, old_ts)
        assert len(results) == 3
        for r in results:
            assert r["accepted"] is False
            assert r["state"] == WindowState.CLOSED

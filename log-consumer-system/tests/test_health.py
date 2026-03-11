"""Tests for src.health — HealthMonitor status checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.health import HealthMonitor
from src.models import ConsumerStats


def _now():
    return datetime.now(timezone.utc)


def _make_stats(
    consumer_id: str = "worker-0",
    processed: int = 100,
    errors: int = 0,
    success_rate: float = 1.0,
    last_active: datetime | None = None,
) -> ConsumerStats:
    """Helper to build a ConsumerStats instance."""
    return ConsumerStats(
        consumer_id=consumer_id,
        processed_count=processed,
        error_count=errors,
        success_rate=success_rate,
        last_active=last_active if last_active is not None else _now(),
    )


class TestHealthyStatus:
    """All consumers active and healthy."""

    def test_all_consumers_active(self):
        stats = [
            _make_stats("w-0", processed=50, errors=0, success_rate=1.0),
            _make_stats("w-1", processed=40, errors=1, success_rate=0.975),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        assert result["status"] == "healthy"
        assert result["reason"] == "all consumers operating normally"
        assert len(result["consumers"]) == 2
        assert all(c["status"] == "active" for c in result["consumers"])


class TestDegradedStatus:
    """Some consumers have issues but not the majority."""

    def test_one_consumer_stale(self):
        stale_time = _now() - timedelta(seconds=60)
        stats = [
            _make_stats("w-0", processed=50, last_active=_now()),
            _make_stats("w-1", processed=40, last_active=stale_time),
            _make_stats("w-2", processed=30, last_active=_now()),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        assert result["status"] == "degraded"
        assert "1 stale" in result["reason"]
        # The stale consumer should be marked
        stale_consumers = [c for c in result["consumers"] if c["status"] == "stale"]
        assert len(stale_consumers) == 1
        assert stale_consumers[0]["consumer_id"] == "w-1"

    def test_one_consumer_degraded_error_rate(self):
        """One consumer with error rate > 0.1 but < 0.5."""
        stats = [
            _make_stats("w-0", processed=80, errors=20, success_rate=0.8),
            _make_stats("w-1", processed=100, errors=0, success_rate=1.0),
            _make_stats("w-2", processed=100, errors=0, success_rate=1.0),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        assert result["status"] == "degraded"
        # w-0 has error_rate = 0.2 which exceeds the degraded threshold
        degraded = [c for c in result["consumers"] if c["status"] == "degraded"]
        assert len(degraded) == 1
        assert degraded[0]["consumer_id"] == "w-0"


class TestUnhealthyStatus:
    """Serious issues: no consumers or majority have problems."""

    def test_no_consumers(self):
        monitor = HealthMonitor(lambda: [])
        result = monitor.status()
        assert result["status"] == "unhealthy"
        assert result["reason"] == "no consumers running"
        assert result["consumers"] == []

    def test_majority_high_error_rate(self):
        """More than half the consumers have high error rates."""
        stats = [
            _make_stats("w-0", processed=50, errors=50, success_rate=0.5),
            _make_stats("w-1", processed=40, errors=60, success_rate=0.4),
            _make_stats("w-2", processed=100, errors=0, success_rate=1.0),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        assert result["status"] == "unhealthy"
        assert result["reason"] == "majority of consumers have issues"

    def test_all_consumers_stale(self):
        """All consumers are stale -> unhealthy."""
        stale_time = _now() - timedelta(seconds=120)
        stats = [
            _make_stats("w-0", processed=10, last_active=stale_time),
            _make_stats("w-1", processed=20, last_active=stale_time),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        assert result["status"] == "unhealthy"


class TestConsumerStarting:
    """Consumer that has never processed anything shows as starting."""

    def test_starting_consumer(self):
        stats = [
            ConsumerStats(
                consumer_id="w-0",
                processed_count=0,
                error_count=0,
                success_rate=1.0,
                last_active=None,
            ),
            _make_stats("w-1", processed=50, last_active=_now()),
        ]
        monitor = HealthMonitor(lambda: stats)
        result = monitor.status()
        starting = [c for c in result["consumers"] if c["status"] == "starting"]
        assert len(starting) == 1
        assert starting[0]["consumer_id"] == "w-0"

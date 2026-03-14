"""Tests for StatsTracker."""

import json
import time

import pytest

from src.config import Settings
from src.models import FailureType
from src.stats import StatsTracker


def _make_tracker(redis_client, **overrides) -> StatsTracker:
    """Helper to build a StatsTracker with sensible defaults."""
    settings = Settings(**overrides)
    return StatsTracker(redis_client, settings)


class TestIncrementAndGetStats:
    """Incrementing counters and retrieving them."""

    @pytest.mark.asyncio
    async def test_increment_and_get_stats(self, redis_client):
        tracker = _make_tracker(redis_client)

        for _ in range(5):
            await tracker.increment("processed")
        for _ in range(2):
            await tracker.increment("failed")

        stats = await tracker.get_stats()
        assert stats["processed"] == 5
        assert stats["failed"] == 2


class TestGetStatsEmpty:
    """Getting stats when nothing has been recorded."""

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, redis_client):
        tracker = _make_tracker(redis_client)

        stats = await tracker.get_stats()
        assert stats == {}


class TestRecordFailure:
    """Recording failures updates history and counter."""

    @pytest.mark.asyncio
    async def test_record_failure(self, redis_client):
        tracker = _make_tracker(redis_client)

        await tracker.record_failure(FailureType.PARSING, "api-gateway", "invalid JSON")
        await tracker.record_failure(FailureType.NETWORK, "auth-service", "connection refused")
        await tracker.record_failure(FailureType.RESOURCE, "log-ingest", "out of memory")

        # Failure history should have 3 entries
        history = await redis_client.get_failure_history(count=100)
        assert len(history) == 3

        # 'failed' counter should be 3
        stats = await tracker.get_stats()
        assert stats["failed"] == 3

        # Verify entries are valid JSON with expected fields
        entry = json.loads(history[0])
        assert "timestamp" in entry
        assert "failure_type" in entry
        assert "source" in entry
        assert "error" in entry


class TestGetFailureTrends:
    """Analyzing failure trends over a time window."""

    @pytest.mark.asyncio
    async def test_get_failure_trends(self, redis_client):
        tracker = _make_tracker(redis_client)

        # Record failures with different types and sources
        await tracker.record_failure(FailureType.PARSING, "api-gateway", "invalid JSON")
        await tracker.record_failure(FailureType.PARSING, "api-gateway", "invalid JSON")
        await tracker.record_failure(FailureType.NETWORK, "auth-service", "connection refused")
        await tracker.record_failure(FailureType.RESOURCE, "log-ingest", "out of memory")

        trends = await tracker.get_failure_trends(window_seconds=300.0)

        assert trends["window_seconds"] == 300.0
        assert trends["total_failures"] == 4
        assert trends["by_type"]["PARSING"] == 2
        assert trends["by_type"]["NETWORK"] == 1
        assert trends["by_type"]["RESOURCE"] == 1
        assert trends["by_source"]["api-gateway"] == 2
        assert trends["by_source"]["auth-service"] == 1
        assert trends["by_source"]["log-ingest"] == 1

        # Top errors should include our error messages
        error_messages = [e["error"] for e in trends["top_errors"]]
        assert "invalid JSON" in error_messages
        assert "connection refused" in error_messages
        assert "out of memory" in error_messages

        # The most frequent error should be first
        assert trends["top_errors"][0]["error"] == "invalid JSON"
        assert trends["top_errors"][0]["count"] == 2


class TestGetFailureTrendsFiltersOld:
    """Old entries outside the time window are excluded."""

    @pytest.mark.asyncio
    async def test_get_failure_trends_filters_old(self, redis_client):
        tracker = _make_tracker(redis_client)

        # Record a recent failure via the normal API
        await tracker.record_failure(FailureType.PARSING, "api-gateway", "recent error")

        # Manually inject an old-timestamp entry directly into Redis
        old_entry = json.dumps({
            "timestamp": time.time() - 600,  # 10 minutes ago
            "failure_type": "NETWORK",
            "source": "old-service",
            "error": "old error",
        })
        await redis_client.push_failure_history(old_entry)

        # With a 300-second window, only the recent failure should appear
        trends = await tracker.get_failure_trends(window_seconds=300.0)

        assert trends["total_failures"] == 1
        assert trends["by_type"] == {"PARSING": 1}
        assert trends["by_source"] == {"api-gateway": 1}


class TestGetDlqGrowthRate:
    """Calculating DLQ growth rate from failure history."""

    @pytest.mark.asyncio
    async def test_get_dlq_growth_rate(self, redis_client):
        tracker = _make_tracker(redis_client)

        # Record 6 failures (all within the window since they happen now)
        for i in range(6):
            await tracker.record_failure(FailureType.PARSING, "svc", f"error-{i}")

        # Over 60 seconds window, 6 failures -> rate = 6/60 = 0.1
        rate = await tracker.get_dlq_growth_rate(window_seconds=60.0)
        assert rate == pytest.approx(6 / 60.0)

        # Over 6 seconds window, still 6 failures -> rate = 6/6 = 1.0
        rate = await tracker.get_dlq_growth_rate(window_seconds=6.0)
        assert rate == pytest.approx(6 / 6.0)


class TestCheckAlertsDlqSize:
    """Alert fires when DLQ size exceeds threshold."""

    @pytest.mark.asyncio
    async def test_check_alerts_dlq_size(self, redis_client):
        tracker = _make_tracker(redis_client, dlq_alert_threshold=2)

        # Pre-populate DLQ with 3 messages
        for i in range(3):
            await redis_client.move_to_dlq(f"msg-{i}")

        alerts = await tracker.check_alerts()

        dlq_alerts = [a for a in alerts if a["type"] == "dlq_size"]
        assert len(dlq_alerts) == 1
        assert dlq_alerts[0]["severity"] == "high"
        assert "3" in dlq_alerts[0]["message"]
        assert "2" in dlq_alerts[0]["message"]


class TestCheckAlertsDominantFailure:
    """Alert fires when one failure type dominates (>80%)."""

    @pytest.mark.asyncio
    async def test_check_alerts_dominant_failure(self, redis_client):
        tracker = _make_tracker(redis_client)

        # Record 10 PARSING failures and 1 NETWORK failure
        for i in range(10):
            await tracker.record_failure(FailureType.PARSING, "api-gateway", f"parse error {i}")
        await tracker.record_failure(FailureType.NETWORK, "auth-service", "timeout")

        alerts = await tracker.check_alerts()

        dominant_alerts = [a for a in alerts if a["type"] == "dominant_failure"]
        assert len(dominant_alerts) == 1
        assert dominant_alerts[0]["severity"] == "high"
        assert "PARSING" in dominant_alerts[0]["message"]
        assert "api-gateway" in dominant_alerts[0]["message"]


class TestCheckAlertsNoAlerts:
    """No alerts in a clean state."""

    @pytest.mark.asyncio
    async def test_check_alerts_no_alerts(self, redis_client):
        tracker = _make_tracker(redis_client)

        alerts = await tracker.check_alerts()
        assert alerts == []

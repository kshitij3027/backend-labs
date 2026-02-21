import pytest
from datetime import datetime, timezone
from analytics import AnalyticsEngine


def make_log(service="auth-service", level="INFO", minutes_ago=0, user_id=None, processing_time_ms=None):
    """Helper to create a log entry with a specific time offset."""
    ts = datetime.now(timezone.utc)
    if minutes_ago:
        from datetime import timedelta
        ts = ts - timedelta(minutes=minutes_ago)
    log = {
        "timestamp": ts.isoformat(),
        "level": level,
        "service": service,
        "message": "test message",
    }
    if user_id:
        log["user_id"] = user_id
    if processing_time_ms is not None:
        log["metadata"] = {"processing_time_ms": processing_time_ms}
    return log


class TestAnalyticsEngine:
    def test_record_and_summary(self):
        engine = AnalyticsEngine()
        engine.record(make_log())
        summary = engine.get_summary()
        assert summary["total_logs"] == 1
        assert summary["active_services"] == 1

    def test_error_tracking(self):
        engine = AnalyticsEngine()
        engine.record(make_log(level="ERROR"))
        engine.record(make_log(level="CRITICAL"))
        engine.record(make_log(level="INFO"))
        summary = engine.get_summary()
        assert summary["total_errors"] == 2
        assert abs(summary["error_rate"] - 2/3) < 0.01

    def test_time_series(self):
        engine = AnalyticsEngine()
        for _ in range(5):
            engine.record(make_log())
        ts = engine.get_time_series(minutes=10)
        assert len(ts) >= 1
        assert ts[0]["total"] == 5

    def test_service_stats(self):
        engine = AnalyticsEngine()
        for _ in range(3):
            engine.record(make_log(service="svc-a"))
        for _ in range(2):
            engine.record(make_log(service="svc-b"))
        stats = engine.get_service_stats()
        assert stats["svc-a"]["total"] == 3
        assert stats["svc-b"]["total"] == 2

    def test_error_rate(self):
        engine = AnalyticsEngine()
        engine.record(make_log(level="ERROR"))
        engine.record(make_log(level="INFO"))
        rate = engine.get_error_rate(minutes=5)
        assert abs(rate - 0.5) < 0.01

    def test_error_trends(self):
        engine = AnalyticsEngine()
        engine.record(make_log(level="ERROR"))
        engine.record(make_log(level="INFO"))
        trends = engine.get_error_trends()
        assert len(trends) >= 1
        assert trends[0]["error_rate"] == 0.5

    def test_service_health(self):
        engine = AnalyticsEngine()
        engine.record(make_log(service="healthy-svc"))
        health = engine.get_service_health()
        assert "healthy-svc" in health
        assert health["healthy-svc"]["status"] == "healthy"

    def test_most_active_services(self):
        engine = AnalyticsEngine()
        for _ in range(10):
            engine.record(make_log(service="busy-svc"))
        for _ in range(3):
            engine.record(make_log(service="quiet-svc"))
        active = engine.get_most_active_services(limit=2)
        assert active[0]["service"] == "busy-svc"
        assert active[0]["count"] == 10

    def test_user_activity(self):
        engine = AnalyticsEngine()
        for _ in range(5):
            engine.record(make_log(user_id="user-1"))
        for _ in range(2):
            engine.record(make_log(user_id="user-2"))
        activity = engine.get_user_activity(limit=2)
        assert activity[0]["user_id"] == "user-1"
        assert activity[0]["count"] == 5

    def test_bucket_cleanup(self):
        engine = AnalyticsEngine(max_buckets=3)
        # Record logs with different minute offsets to create multiple buckets
        engine.record(make_log(minutes_ago=5))
        engine.record(make_log(minutes_ago=4))
        engine.record(make_log(minutes_ago=3))
        engine.record(make_log(minutes_ago=2))
        # Should have trimmed down to 3 buckets max
        ts = engine.get_time_series(minutes=60)
        assert len(ts) <= 3

    def test_get_error_rate_no_logs(self):
        engine = AnalyticsEngine()
        assert engine.get_error_rate() == 0.0

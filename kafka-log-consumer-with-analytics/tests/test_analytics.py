"""Tests for the analytics engine."""
import time

import pytest

from src.analytics import AnalyticsEngine
from src.models import WebAccessLog, AppLog, ErrorLog


class TestThroughput:
    def test_single_message(self):
        engine = AnalyticsEngine(window_seconds=60)
        log = WebAccessLog(endpoint="/api/test", response_time_ms=50.0)
        engine.record_message(log)
        stats = engine.get_stats()
        assert stats["total_messages"] == 1
        assert stats["throughput_per_sec"] > 0

    def test_multiple_messages(self):
        engine = AnalyticsEngine(window_seconds=60)
        for i in range(100):
            log = WebAccessLog(endpoint="/api/test", response_time_ms=50.0)
            engine.record_message(log)
        stats = engine.get_stats()
        assert stats["total_messages"] == 100


class TestPercentiles:
    def test_response_time_percentiles(self):
        engine = AnalyticsEngine()
        # Create 100 messages with response times 1-100ms
        for i in range(1, 101):
            log = WebAccessLog(endpoint="/api/test", response_time_ms=float(i))
            engine.record_message(log)

        analytics = engine.get_analytics()
        p = analytics["percentiles"]
        # P50 should be around 50ms
        assert 45 <= p["p50"] <= 55
        # P95 should be around 95ms
        assert 90 <= p["p95"] <= 100
        # P99 should be around 99ms
        assert 95 <= p["p99"] <= 100

    def test_empty_percentiles(self):
        engine = AnalyticsEngine()
        analytics = engine.get_analytics()
        assert analytics["percentiles"]["p50"] == 0.0
        assert analytics["percentiles"]["p95"] == 0.0


class TestPerEndpointStats:
    def test_endpoint_counting(self):
        engine = AnalyticsEngine()
        for _ in range(5):
            engine.record_message(WebAccessLog(endpoint="/api/users", response_time_ms=30.0, status_code=200))
        for _ in range(3):
            engine.record_message(WebAccessLog(endpoint="/api/orders", response_time_ms=60.0, status_code=200))

        analytics = engine.get_analytics()
        assert analytics["endpoints"]["/api/users"]["request_count"] == 5
        assert analytics["endpoints"]["/api/orders"]["request_count"] == 3

    def test_endpoint_error_rate(self):
        engine = AnalyticsEngine()
        # 8 success + 2 errors = 20% error rate
        for _ in range(8):
            engine.record_message(WebAccessLog(endpoint="/api/test", status_code=200, response_time_ms=10.0))
        for _ in range(2):
            engine.record_message(WebAccessLog(endpoint="/api/test", status_code=500, response_time_ms=10.0))

        analytics = engine.get_analytics()
        assert analytics["endpoints"]["/api/test"]["error_rate"] == 20.0

    def test_endpoint_avg_response_time(self):
        engine = AnalyticsEngine()
        engine.record_message(WebAccessLog(endpoint="/api/test", response_time_ms=20.0))
        engine.record_message(WebAccessLog(endpoint="/api/test", response_time_ms=40.0))
        engine.record_message(WebAccessLog(endpoint="/api/test", response_time_ms=60.0))

        analytics = engine.get_analytics()
        assert analytics["endpoints"]["/api/test"]["avg_response_time_ms"] == pytest.approx(40.0, abs=0.1)


class TestGeoDistribution:
    def test_geo_counting(self):
        engine = AnalyticsEngine()
        engine.record_message(WebAccessLog(geo="us-east", response_time_ms=10.0))
        engine.record_message(WebAccessLog(geo="us-east", response_time_ms=10.0))
        engine.record_message(WebAccessLog(geo="eu-west", response_time_ms=10.0))

        analytics = engine.get_analytics()
        assert analytics["geo_distribution"]["us-east"] == 2
        assert analytics["geo_distribution"]["eu-west"] == 1


class TestErrorTracking:
    def test_error_rate(self):
        engine = AnalyticsEngine()
        for _ in range(9):
            engine.record_message(WebAccessLog(status_code=200, response_time_ms=10.0))
        engine.record_message(WebAccessLog(status_code=500, response_time_ms=10.0))

        stats = engine.get_stats()
        assert stats["error_rate"] == 10.0

    def test_error_log_counted(self):
        engine = AnalyticsEngine()
        engine.record_message(ErrorLog(error_type="NPE", endpoint="/api/crash"))
        stats = engine.get_stats()
        assert stats["total_errors"] == 1

    def test_zero_errors(self):
        engine = AnalyticsEngine()
        engine.record_message(AppLog(service="auth", level="INFO"))
        stats = engine.get_stats()
        assert stats["error_rate"] == 0.0


class TestMetrics:
    def test_metrics_structure(self):
        engine = AnalyticsEngine()
        engine.record_message(WebAccessLog(response_time_ms=50.0))
        metrics = engine.get_metrics()
        assert "throughput_history" in metrics
        assert "total_messages" in metrics
        assert "response_time_percentiles" in metrics
        assert metrics["total_messages"] == 1


class TestWindowExpiry:
    def test_buckets_expire(self):
        engine = AnalyticsEngine(window_seconds=5)
        # Manually insert an old bucket
        old_time = int(time.time()) - 10
        with engine._lock:
            engine._throughput_buckets.append((old_time, 50))
        # Record a new message to trigger expiry
        engine.record_message(WebAccessLog(response_time_ms=10.0))
        stats = engine.get_stats()
        # Old bucket should have been expired, total_messages should be 1
        assert stats["total_messages"] == 1

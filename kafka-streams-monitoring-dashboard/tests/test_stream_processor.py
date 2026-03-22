"""Tests for src.stream_processor module."""

import logging
import time

from src.metrics_store import MetricsStore
from src.stream_processor import StreamProcessor


class TestStreamProcessor:
    """Verify message routing by topic."""

    def test_process_log_event(self, stream_processor, metrics_store, sample_log_event):
        stream_processor.process_message("log-events", "key-1", sample_log_event)
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1
        assert metrics["per_topic_counts"].get("log-events") == 1

    def test_process_error_event(self, stream_processor, metrics_store, sample_error_event):
        stream_processor.process_message("error-events", "key-2", sample_error_event)
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1
        assert metrics["per_topic_counts"].get("error-events") == 1

    def test_process_user_event(self, stream_processor, metrics_store, sample_user_event):
        stream_processor.process_message("user-events", "key-3", sample_user_event)
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1
        assert metrics["per_topic_counts"].get("user-events") == 1

    def test_unknown_topic(self, stream_processor, metrics_store, caplog):
        with caplog.at_level(logging.WARNING):
            stream_processor.process_message("unknown-topic", "key-4", {"foo": "bar"})
        assert "Unknown topic" in caplog.text
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 0

    def test_malformed_data(self, stream_processor, metrics_store):
        """Handler should not crash on data missing expected fields."""
        stream_processor.process_message("log-events", None, {})
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1


class TestBusinessMetricsIntegration:
    """Verify business metrics are tracked when processing messages."""

    def test_log_event_tracks_api_version(self, stream_processor, business_metrics):
        event = {
            "path": "/api/v2/orders",
            "method": "GET",
            "status_code": 200,
            "response_time": 50,
            "timestamp": time.time(),
        }
        stream_processor.process_message("log-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["api_versions"].get("v2") == 1

    def test_log_event_no_path_skips_api_version(self, stream_processor, business_metrics):
        event = {"method": "GET", "status_code": 200, "timestamp": time.time()}
        stream_processor.process_message("log-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["api_versions"] == {}

    def test_user_event_tracks_funnel(self, stream_processor, business_metrics):
        event = {
            "user_id": "u1",
            "action": "page_view",
            "path": "/products",
            "timestamp": time.time(),
        }
        stream_processor.process_message("user-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["funnel"]["browse"] >= 1

    def test_user_event_tracks_auth(self, stream_processor, business_metrics):
        event = {
            "user_id": "u1",
            "action": "login",
            "path": "/login",
            "timestamp": time.time(),
        }
        stream_processor.process_message("user-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["auth"]["success"] == 1

    def test_user_event_signup_tracks_auth(self, stream_processor, business_metrics):
        event = {
            "user_id": "u1",
            "action": "signup",
            "path": "/signup",
            "timestamp": time.time(),
        }
        stream_processor.process_message("user-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["auth"]["success"] == 1

    def test_user_event_non_auth_action(self, stream_processor, business_metrics):
        event = {
            "user_id": "u1",
            "action": "page_view",
            "path": "/about",
            "timestamp": time.time(),
        }
        stream_processor.process_message("user-events", "key-1", event)
        bm = business_metrics.get_business_metrics()
        assert bm["auth"]["success"] == 0
        assert bm["auth"]["failure"] == 0


class TestGeoIntegration:
    """Verify geo analysis is triggered when processing log events."""

    def test_log_event_with_ip_triggers_geo(self, stream_processor, geo_analyzer):
        event = {
            "path": "/api/v1/users",
            "ip_address": "8.8.8.8",
            "response_time": 120.0,
            "timestamp": time.time(),
        }
        stream_processor.process_message("log-events", "key-1", event)
        geo = geo_analyzer.get_geo_metrics()
        assert sum(geo["traffic_by_region"].values()) >= 1
        assert len(geo["latency_by_region"]) >= 1

    def test_log_event_without_ip_skips_geo(self, stream_processor, geo_analyzer):
        event = {
            "path": "/api/v1/users",
            "response_time": 120.0,
            "timestamp": time.time(),
        }
        stream_processor.process_message("log-events", "key-1", event)
        geo = geo_analyzer.get_geo_metrics()
        assert sum(geo["traffic_by_region"].values()) == 0

    def test_log_event_ip_but_no_response_time(self, stream_processor, geo_analyzer):
        event = {
            "path": "/api/v1/users",
            "ip_address": "8.8.8.8",
            "timestamp": time.time(),
        }
        stream_processor.process_message("log-events", "key-1", event)
        geo = geo_analyzer.get_geo_metrics()
        # Traffic is counted but no latency recorded
        assert sum(geo["traffic_by_region"].values()) >= 1
        assert len(geo["latency_by_region"]) == 0

    def test_error_event_does_not_trigger_geo(self, stream_processor, geo_analyzer):
        event = {
            "error_type": "TimeoutError",
            "ip_address": "8.8.8.8",
            "timestamp": time.time(),
        }
        stream_processor.process_message("error-events", "key-1", event)
        geo = geo_analyzer.get_geo_metrics()
        assert sum(geo["traffic_by_region"].values()) == 0


class TestProcessorWithoutOptionalComponents:
    """Verify processor works when business_metrics/geo_analyzer are None."""

    def test_log_event_without_business_metrics(self, metrics_store):
        processor = StreamProcessor(metrics_store, business_metrics=None, geo_analyzer=None)
        event = {
            "path": "/api/v1/users",
            "ip_address": "8.8.8.8",
            "response_time": 50,
            "timestamp": time.time(),
        }
        processor.process_message("log-events", "key-1", event)
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1

    def test_user_event_without_business_metrics(self, metrics_store):
        processor = StreamProcessor(metrics_store, business_metrics=None, geo_analyzer=None)
        event = {
            "user_id": "u1",
            "action": "login",
            "path": "/login",
            "timestamp": time.time(),
        }
        processor.process_message("user-events", "key-1", event)
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1

    def test_handler_exception_logged(self, metrics_store, caplog):
        """If handler raises, it should be caught and logged."""
        processor = StreamProcessor(metrics_store)
        # Temporarily break the metrics store to cause an exception
        processor._metrics_store = None
        with caplog.at_level(logging.ERROR):
            processor.process_message("log-events", "key-1", {"timestamp": time.time()})
        assert "Error processing message" in caplog.text

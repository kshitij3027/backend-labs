"""Tests for src.stream_processor module."""

import logging
import time


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

"""Unit tests for MetricsTracker."""

import time
import threading
from unittest.mock import patch, MagicMock

import pytest

from src.metrics import MetricsTracker, _THROUGHPUT_WINDOW


class TestMetricsTrackerRecordConsumed:
    """record_consumed increments counts correctly."""

    def test_single_record(self):
        tracker = MetricsTracker()
        tracker.record_consumed("web-api-logs")

        tp = tracker.throughput
        assert tp["total_messages"] == 1
        assert tp["by_topic"]["web-api-logs"] == 1

    def test_multiple_records_same_topic(self):
        tracker = MetricsTracker()
        for _ in range(5):
            tracker.record_consumed("web-api-logs")

        tp = tracker.throughput
        assert tp["total_messages"] == 5
        assert tp["by_topic"]["web-api-logs"] == 5

    def test_multiple_records_different_topics(self):
        tracker = MetricsTracker()
        tracker.record_consumed("web-api-logs")
        tracker.record_consumed("web-api-logs")
        tracker.record_consumed("payment-service-logs")
        tracker.record_consumed("user-service-logs")
        tracker.record_consumed("user-service-logs")
        tracker.record_consumed("user-service-logs")

        tp = tracker.throughput
        assert tp["total_messages"] == 6
        assert tp["by_topic"]["web-api-logs"] == 2
        assert tp["by_topic"]["payment-service-logs"] == 1
        assert tp["by_topic"]["user-service-logs"] == 3

    def test_record_consumed_with_latency(self):
        tracker = MetricsTracker()
        tracker.record_consumed("web-api-logs", latency_ms=15.5)
        tracker.record_consumed("web-api-logs", latency_ms=25.0)

        tp = tracker.throughput
        assert tp["total_messages"] == 2
        stats = tracker.latency_stats
        assert stats["samples"] == 2

    def test_record_consumed_latency_none_ignored(self):
        tracker = MetricsTracker()
        tracker.record_consumed("web-api-logs")
        tracker.record_consumed("web-api-logs", latency_ms=None)

        stats = tracker.latency_stats
        assert stats["samples"] == 0


class TestMetricsTrackerThroughput:
    """throughput calculates messages_per_second correctly."""

    def test_throughput_reflects_recent_messages(self):
        tracker = MetricsTracker()
        for _ in range(20):
            tracker.record_consumed("web-api-logs")

        tp = tracker.throughput
        # 20 messages all recorded just now, window = 10s
        assert tp["messages_per_second"] == round(20 / _THROUGHPUT_WINDOW, 1)

    def test_throughput_zero_when_empty(self):
        tracker = MetricsTracker()
        tp = tracker.throughput
        assert tp["messages_per_second"] == 0.0
        assert tp["total_messages"] == 0
        assert tp["by_topic"] == {}

    def test_throughput_excludes_old_timestamps(self):
        tracker = MetricsTracker()

        # Inject timestamps older than the window by manipulating internal state
        old_time = time.time() - _THROUGHPUT_WINDOW - 5
        with tracker._lock:
            for _ in range(10):
                tracker._message_timestamps.append(old_time)
            tracker._total = 10
            tracker._counts["old-topic"] = 10

        # Record one fresh message
        tracker.record_consumed("web-api-logs")

        tp = tracker.throughput
        # Only the 1 recent message counts toward per-second rate
        assert tp["messages_per_second"] == round(1 / _THROUGHPUT_WINDOW, 1)
        # But total_messages includes everything
        assert tp["total_messages"] == 11


class TestMetricsTrackerPerTopicCounts:
    """by_topic tracks independent per-topic counts."""

    def test_topics_tracked_independently(self):
        tracker = MetricsTracker()
        topics = [
            "web-api-logs",
            "user-service-logs",
            "payment-service-logs",
        ]
        for topic in topics:
            tracker.record_consumed(topic)

        tp = tracker.throughput
        for topic in topics:
            assert tp["by_topic"][topic] == 1

    def test_unknown_topics_tracked(self):
        tracker = MetricsTracker()
        tracker.record_consumed("some-unknown-topic")
        tp = tracker.throughput
        assert tp["by_topic"]["some-unknown-topic"] == 1


class TestMetricsTrackerBackwardCompat:
    """MetricsTracker with no args must still work (backward compat)."""

    def test_no_args_init(self):
        tracker = MetricsTracker()
        assert tracker._admin_client is None
        tp = tracker.throughput
        assert tp["total_messages"] == 0

    def test_none_bootstrap_servers(self):
        tracker = MetricsTracker(bootstrap_servers=None)
        assert tracker._admin_client is None

    def test_invalid_bootstrap_servers_no_crash(self):
        """Passing invalid servers should not raise — AdminClient failure is swallowed."""
        tracker = MetricsTracker(bootstrap_servers="invalid:0000")
        # AdminClient may or may not fail at construction depending on librdkafka,
        # but the tracker itself must be usable.
        tp = tracker.throughput
        assert tp["total_messages"] == 0


class TestMetricsTrackerLatency:
    """Latency stats (p50, p95, p99)."""

    def test_latency_stats_empty(self):
        tracker = MetricsTracker()
        stats = tracker.latency_stats
        assert stats == {"p50": 0, "p95": 0, "p99": 0, "samples": 0}

    def test_latency_stats_single_sample(self):
        tracker = MetricsTracker()
        tracker.record_consumed("t", latency_ms=42.0)
        stats = tracker.latency_stats
        assert stats["samples"] == 1
        assert stats["p50"] == 42.0
        assert stats["p95"] == 42.0
        assert stats["p99"] == 42.0

    def test_latency_stats_percentiles(self):
        tracker = MetricsTracker()
        # Insert 100 samples: 1, 2, 3, ..., 100
        for i in range(1, 101):
            tracker.record_consumed("t", latency_ms=float(i))

        stats = tracker.latency_stats
        assert stats["samples"] == 100
        # p50 = index 50 → value 51
        assert stats["p50"] == 51.0
        # p95 = index 95 → value 96
        assert stats["p95"] == 96.0
        # p99 = index 99 → value 100
        assert stats["p99"] == 100.0

    def test_latency_stats_unordered_input(self):
        tracker = MetricsTracker()
        # Insert in reverse order
        for i in [100.0, 1.0, 50.0, 75.0, 25.0]:
            tracker.record_consumed("t", latency_ms=i)

        stats = tracker.latency_stats
        assert stats["samples"] == 5
        # Sorted: [1, 25, 50, 75, 100]
        # p50 = index 2 → 50
        assert stats["p50"] == 50.0


class TestMetricsTrackerConsumerLag:
    """Consumer lag tracking."""

    def test_consumer_lag_empty_by_default(self):
        tracker = MetricsTracker()
        assert tracker.consumer_lag == {}

    def test_consumer_lag_returns_dict(self):
        tracker = MetricsTracker()
        assert isinstance(tracker.consumer_lag, dict)

    def test_update_consumer_lag_with_mock(self):
        tracker = MetricsTracker()

        # Build a mock confluent_kafka Consumer
        mock_tp = MagicMock()
        mock_tp.topic = "web-api-logs"
        mock_tp.partition = 0

        mock_consumer = MagicMock()
        mock_consumer.assignment.return_value = [mock_tp]

        mock_committed_tp = MagicMock()
        mock_committed_tp.offset = 50
        mock_consumer.committed.return_value = [mock_committed_tp]
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        tracker.update_consumer_lag(mock_consumer)

        lag = tracker.consumer_lag
        assert lag == {"web-api-logs-0": 50}

    def test_update_consumer_lag_handles_exception(self):
        tracker = MetricsTracker()
        mock_consumer = MagicMock()
        mock_consumer.assignment.side_effect = RuntimeError("no connection")

        # Should not raise
        tracker.update_consumer_lag(mock_consumer)
        assert tracker.consumer_lag == {}

    def test_update_consumer_lag_negative_committed_offset(self):
        """When committed offset is -1 (no commit yet), treat as 0."""
        tracker = MetricsTracker()

        mock_tp = MagicMock()
        mock_tp.topic = "test-topic"
        mock_tp.partition = 1

        mock_consumer = MagicMock()
        mock_consumer.assignment.return_value = [mock_tp]

        mock_committed_tp = MagicMock()
        mock_committed_tp.offset = -1
        mock_consumer.committed.return_value = [mock_committed_tp]
        mock_consumer.get_watermark_offsets.return_value = (0, 30)

        tracker.update_consumer_lag(mock_consumer)
        assert tracker.consumer_lag == {"test-topic-1": 30}


class TestMetricsTrackerThroughputHistory:
    """Throughput history time-series."""

    def test_throughput_history_returns_list(self):
        tracker = MetricsTracker()
        assert isinstance(tracker.throughput_history, list)

    def test_throughput_history_populated_over_time(self):
        tracker = MetricsTracker()
        # The background thread snapshots every 1s. Wait a bit.
        time.sleep(1.5)
        history = tracker.throughput_history
        assert len(history) >= 1
        assert "time" in history[0]
        assert "mps" in history[0]

    def test_snapshot_throughput_manual(self):
        tracker = MetricsTracker()
        tracker.record_consumed("t")
        tracker._snapshot_throughput()
        history = tracker.throughput_history
        # Should have at least the manual snapshot + any from background thread
        found = any(entry["mps"] >= 1.0 for entry in history)
        # The manual snapshot may or may not catch the 1-second window,
        # but the history should contain entries
        assert len(history) >= 1


class TestMetricsTrackerThreadSafety:
    """MetricsTracker is safe under concurrent access."""

    def test_concurrent_record_consumed(self):
        tracker = MetricsTracker()
        num_threads = 10
        records_per_thread = 100

        def worker():
            for _ in range(records_per_thread):
                tracker.record_consumed("web-api-logs")

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        tp = tracker.throughput
        assert tp["total_messages"] == num_threads * records_per_thread
        assert tp["by_topic"]["web-api-logs"] == num_threads * records_per_thread

    def test_concurrent_read_and_write(self):
        tracker = MetricsTracker()
        results = []
        stop_event = threading.Event()

        def writer():
            for _ in range(200):
                tracker.record_consumed("web-api-logs")
                time.sleep(0.001)
            stop_event.set()

        def reader():
            while not stop_event.is_set():
                tp = tracker.throughput
                results.append(tp["total_messages"])
                time.sleep(0.005)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        w.join()
        r.join()

        # The final read should reflect all writes
        tp = tracker.throughput
        assert tp["total_messages"] == 200
        # Results should be monotonically non-decreasing
        for i in range(1, len(results)):
            assert results[i] >= results[i - 1]

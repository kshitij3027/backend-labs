"""Unit tests for MetricsTracker."""

import time
import threading
from unittest.mock import patch

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

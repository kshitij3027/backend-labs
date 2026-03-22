"""Tests for src.metrics_store module."""

import threading
import time

from src.metrics_store import MetricsStore


class TestAddEvent:
    """Verify events are stored correctly."""

    def test_add_event(self, metrics_store):
        metrics_store.add_event("log-events", {"msg": "hello", "timestamp": time.time()})
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1

    def test_deque_max_length(self):
        store = MetricsStore(max_length=5)
        now = time.time()
        for i in range(10):
            store.add_event("log-events", {"i": i, "timestamp": now})
        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 5


class TestGetWindowedMetrics:
    """Verify windowed metric aggregations."""

    def test_get_windowed_metrics_empty(self, metrics_store):
        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 0
        assert metrics["error_rate"] == 0.0
        assert metrics["avg_response_time"] == 0.0
        assert metrics["p95_response_time"] == 0.0
        assert metrics["events_per_second"] == 0.0

    def test_get_windowed_metrics_with_data(self, metrics_store):
        now = time.time()
        metrics_store.add_event("log-events", {"response_time": 50, "timestamp": now})
        metrics_store.add_event("error-events", {"response_time": 100, "timestamp": now})
        metrics_store.add_event("user-events", {"timestamp": now})

        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 3
        assert metrics["per_topic_counts"]["log-events"] == 1
        assert metrics["per_topic_counts"]["error-events"] == 1
        assert metrics["per_topic_counts"]["user-events"] == 1

    def test_windowed_metrics_excludes_old_events(self, metrics_store):
        old_time = time.time() - 120  # 2 minutes ago
        now = time.time()
        metrics_store.add_event("log-events", {"timestamp": old_time})
        metrics_store.add_event("log-events", {"timestamp": now})

        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 1

    def test_error_rate_calculation(self, metrics_store):
        now = time.time()
        # 2 errors out of 10 total = 20%
        for _ in range(8):
            metrics_store.add_event("log-events", {"timestamp": now})
        for _ in range(2):
            metrics_store.add_event("error-events", {"timestamp": now})

        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["error_rate"] == 20.0

    def test_response_time_stats(self, metrics_store):
        now = time.time()
        # Add enough events for p95 calculation (>= 20)
        for i in range(25):
            metrics_store.add_event(
                "log-events", {"response_time": float(i + 1), "timestamp": now}
            )

        metrics = metrics_store.get_windowed_metrics(window_seconds=60)
        assert metrics["avg_response_time"] == 13.0  # mean of 1..25
        # p95 index: int(25 * 0.95) = 23 -> sorted_rt[23] = 24.0
        assert metrics["p95_response_time"] == 24.0


class TestGetHistorical:
    """Verify historical bucket output."""

    def test_get_historical(self, metrics_store):
        now = time.time()
        # Add events in the most recent bucket
        for _ in range(5):
            metrics_store.add_event(
                "log-events", {"response_time": 10, "timestamp": now - 1}
            )

        result = metrics_store.get_historical(points=10, bucket_seconds=10)
        assert "labels" in result
        assert "events" in result
        assert "error_rate" in result
        assert "response_times" in result
        assert len(result["labels"]) == 10
        # At least one bucket should have events
        assert sum(result["events"]) >= 5

    def test_get_historical_empty(self, metrics_store):
        result = metrics_store.get_historical(points=5, bucket_seconds=10)
        assert len(result["labels"]) == 5
        assert all(e == 0 for e in result["events"])
        assert all(r == 0 for r in result["response_times"])


class TestThreadSafety:
    """Verify concurrent access does not raise errors."""

    def test_thread_safety(self):
        store = MetricsStore(max_length=500)
        errors = []

        def writer(topic, count):
            try:
                now = time.time()
                for i in range(count):
                    store.add_event(topic, {"i": i, "timestamp": now})
            except Exception as e:
                errors.append(e)

        def reader(count):
            try:
                for _ in range(count):
                    store.get_windowed_metrics(window_seconds=60)
                    store.get_historical(points=5, bucket_seconds=10)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, args=("log-events", 100)),
            threading.Thread(target=writer, args=("error-events", 100)),
            threading.Thread(target=reader, args=(50,)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []

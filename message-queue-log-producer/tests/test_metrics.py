"""Tests for the MetricsCollector class."""

import threading
from unittest.mock import patch

from src.metrics import MetricsCollector


class TestMetricsCollector:
    """Unit tests for MetricsCollector."""

    def test_initial_zeros(self):
        mc = MetricsCollector()
        snap = mc.snapshot()
        assert snap["messages_received"] == 0
        assert snap["messages_published"] == 0
        assert snap["batches_flushed"] == 0
        assert snap["publish_errors"] == 0
        assert snap["fallback_writes"] == 0
        assert snap["fallback_drained"] == 0

    def test_record_received(self):
        mc = MetricsCollector()
        mc.record_received(5)
        snap = mc.snapshot()
        assert snap["messages_received"] == 5

    def test_record_published(self):
        mc = MetricsCollector()
        mc.record_published(3)
        snap = mc.snapshot()
        assert snap["messages_published"] == 3

    def test_record_batch_flushed(self):
        mc = MetricsCollector()
        mc.record_batch_flushed()
        mc.record_batch_flushed()
        snap = mc.snapshot()
        assert snap["batches_flushed"] == 2

    def test_record_publish_error(self):
        mc = MetricsCollector()
        mc.record_publish_error()
        snap = mc.snapshot()
        assert snap["publish_errors"] == 1

    def test_record_fallback_write(self):
        mc = MetricsCollector()
        mc.record_fallback_write(10)
        snap = mc.snapshot()
        assert snap["fallback_writes"] == 10

    def test_record_fallback_drained(self):
        mc = MetricsCollector()
        mc.record_fallback_drained(5)
        snap = mc.snapshot()
        assert snap["fallback_drained"] == 5

    def test_latency_p95(self):
        mc = MetricsCollector()
        latencies = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        for lat in latencies:
            mc.record_latency(lat)
        p95 = mc.get_latency_p95()
        assert 95 <= p95 <= 100

    def test_latency_p95_empty(self):
        mc = MetricsCollector()
        assert mc.get_latency_p95() == 0.0

    @patch("src.metrics.time")
    def test_throughput(self, mock_time):
        mock_time.monotonic.return_value = 0.0
        mc = MetricsCollector()
        mc.record_published(100)

        # Simulate 10 seconds elapsed
        mock_time.monotonic.return_value = 10.0
        throughput = mc.get_throughput()
        assert throughput == 10.0

    def test_snapshot_keys(self):
        mc = MetricsCollector()
        snap = mc.snapshot()
        expected_keys = {
            "messages_received",
            "messages_published",
            "batches_flushed",
            "publish_errors",
            "fallback_writes",
            "fallback_drained",
            "throughput",
            "latency_p95",
            "uptime_seconds",
        }
        assert set(snap.keys()) == expected_keys

    def test_thread_safety(self):
        mc = MetricsCollector()
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: mc.record_received(100))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        snap = mc.snapshot()
        assert snap["messages_received"] == 1000

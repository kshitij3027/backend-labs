"""Tests for ShipperMetrics and ReceiverMetrics."""

import threading

import pytest

from src.metrics import ShipperMetrics, ReceiverMetrics


class TestShipperMetrics:
    """Tests for ShipperMetrics."""

    def test_initial_snapshot(self):
        m = ShipperMetrics()
        snap = m.snapshot()
        assert snap["logs_sent"] == 0
        assert snap["batches_sent"] == 0
        assert snap["failed_sends"] == 0
        assert snap["avg_compression_ratio"] == 0.0
        assert snap["total_compression_time_ms"] == 0.0

    def test_record_send_updates_counts(self):
        m = ShipperMetrics()
        m.record_send(logs_count=10, compression_ratio=3.0, compression_time_ms=1.5)
        snap = m.snapshot()
        assert snap["logs_sent"] == 10
        assert snap["batches_sent"] == 1
        assert snap["avg_compression_ratio"] == 3.0
        assert snap["total_compression_time_ms"] == 1.5

    def test_record_send_multiple(self):
        m = ShipperMetrics()
        m.record_send(logs_count=10, compression_ratio=4.0, compression_time_ms=2.0)
        m.record_send(logs_count=20, compression_ratio=2.0, compression_time_ms=3.0)
        snap = m.snapshot()
        assert snap["logs_sent"] == 30
        assert snap["batches_sent"] == 2
        # Average ratio: (4.0 + 2.0) / 2 = 3.0
        assert snap["avg_compression_ratio"] == 3.0
        assert snap["total_compression_time_ms"] == 5.0

    def test_record_failure(self):
        m = ShipperMetrics()
        m.record_failure()
        m.record_failure()
        snap = m.snapshot()
        assert snap["failed_sends"] == 2

    def test_throughput_positive(self):
        m = ShipperMetrics()
        m.record_send(logs_count=100, compression_ratio=1.0, compression_time_ms=0.0)
        snap = m.snapshot()
        assert snap["throughput_logs_per_sec"] > 0
        assert snap["elapsed_seconds"] >= 0


class TestReceiverMetrics:
    """Tests for ReceiverMetrics."""

    def test_initial_snapshot(self):
        m = ReceiverMetrics()
        snap = m.snapshot()
        assert snap["logs_received"] == 0
        assert snap["batches_received"] == 0
        assert snap["bytes_compressed"] == 0
        assert snap["bytes_decompressed"] == 0
        assert snap["compression_ratio"] == 0.0

    def test_record_batch(self):
        m = ReceiverMetrics()
        m.record_batch(logs_count=5, compressed_size=100, decompressed_size=500)
        snap = m.snapshot()
        assert snap["logs_received"] == 5
        assert snap["batches_received"] == 1
        assert snap["bytes_compressed"] == 100
        assert snap["bytes_decompressed"] == 500
        # Ratio: 500 / 100 = 5.0
        assert snap["compression_ratio"] == 5.0

    def test_record_batch_multiple(self):
        m = ReceiverMetrics()
        m.record_batch(logs_count=5, compressed_size=100, decompressed_size=500)
        m.record_batch(logs_count=10, compressed_size=200, decompressed_size=600)
        snap = m.snapshot()
        assert snap["logs_received"] == 15
        assert snap["batches_received"] == 2
        assert snap["bytes_compressed"] == 300
        assert snap["bytes_decompressed"] == 1100
        # Ratio: 1100 / 300 = 3.67
        assert snap["compression_ratio"] == 3.67

    def test_throughput_positive(self):
        m = ReceiverMetrics()
        m.record_batch(logs_count=50, compressed_size=10, decompressed_size=100)
        snap = m.snapshot()
        assert snap["throughput_logs_per_sec"] > 0


class TestMetricsThreadSafety:
    """Verify metrics are safe under concurrent updates."""

    def test_shipper_metrics_concurrent(self):
        m = ShipperMetrics()
        num_threads = 10
        sends_per_thread = 100

        def worker():
            for _ in range(sends_per_thread):
                m.record_send(logs_count=1, compression_ratio=2.0, compression_time_ms=0.1)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        snap = m.snapshot()
        expected_total = num_threads * sends_per_thread
        assert snap["logs_sent"] == expected_total
        assert snap["batches_sent"] == expected_total

    def test_receiver_metrics_concurrent(self):
        m = ReceiverMetrics()
        num_threads = 10
        batches_per_thread = 100

        def worker():
            for _ in range(batches_per_thread):
                m.record_batch(logs_count=2, compressed_size=10, decompressed_size=50)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        snap = m.snapshot()
        expected_total = num_threads * batches_per_thread
        assert snap["logs_received"] == expected_total * 2  # 2 logs per batch
        assert snap["batches_received"] == expected_total
        assert snap["bytes_compressed"] == expected_total * 10
        assert snap["bytes_decompressed"] == expected_total * 50

    def test_shipper_mixed_operations_concurrent(self):
        """Mix record_send and record_failure concurrently."""
        m = ShipperMetrics()
        num_threads = 5
        ops_per_thread = 50

        def sender():
            for _ in range(ops_per_thread):
                m.record_send(logs_count=1, compression_ratio=1.0, compression_time_ms=0.0)

        def failer():
            for _ in range(ops_per_thread):
                m.record_failure()

        threads = []
        for _ in range(num_threads):
            threads.append(threading.Thread(target=sender))
            threads.append(threading.Thread(target=failer))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        snap = m.snapshot()
        assert snap["logs_sent"] == num_threads * ops_per_thread
        assert snap["failed_sends"] == num_threads * ops_per_thread

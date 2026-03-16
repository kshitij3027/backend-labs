"""Tests for ProducerMetrics — no Kafka broker required."""

import time

from src.metrics import ProducerMetrics


class TestRecordSuccess:
    """Verify that record_success updates internal counters."""

    def test_record_success_increments_total(self):
        metrics = ProducerMetrics()
        metrics.record_success("logs-application", latency=0.05)
        metrics.record_success("logs-errors", latency=0.02)

        snap = metrics.snapshot
        assert snap["total_sent"] == 2
        assert snap["topic_counts"]["logs-application"] == 1
        assert snap["topic_counts"]["logs-errors"] == 1


class TestRecordFailure:
    """Verify that record_failure tracks failures and error types."""

    def test_record_failure_increments_failed(self):
        metrics = ProducerMetrics()
        metrics.record_failure("logs-application", "BufferError")
        metrics.record_failure("logs-application", "KafkaException")
        metrics.record_failure("logs-errors", "BufferError")

        snap = metrics.snapshot
        assert snap["total_failed"] == 3
        assert snap["error_counts"]["BufferError"] == 2
        assert snap["error_counts"]["KafkaException"] == 1


class TestSnapshotStructure:
    """Verify the snapshot dict contains all required keys."""

    def test_snapshot_has_all_keys(self):
        metrics = ProducerMetrics()
        snap = metrics.snapshot

        expected_keys = {
            "total_sent",
            "total_failed",
            "topic_counts",
            "throughput",
            "error_counts",
            "error_rate",
        }
        assert set(snap.keys()) == expected_keys

    def test_error_rate_calculation(self):
        metrics = ProducerMetrics()
        metrics.record_success("t", 0.01)
        metrics.record_success("t", 0.01)
        metrics.record_failure("t", "err")

        snap = metrics.snapshot
        # 1 failure / 3 total = ~33.33 %
        assert 33.0 < snap["error_rate"] < 34.0


class TestThroughput:
    """Verify throughput calculation from the rolling window."""

    def test_throughput_calculation(self):
        metrics = ProducerMetrics()

        # Simulate two samples 1 second apart, 100 msgs each
        metrics.record_throughput(100)
        time.sleep(0.05)
        metrics.record_throughput(100)

        snap = metrics.snapshot
        assert snap["throughput"] > 0

    def test_empty_throughput_is_zero(self):
        metrics = ProducerMetrics()
        assert metrics.snapshot["throughput"] == 0.0

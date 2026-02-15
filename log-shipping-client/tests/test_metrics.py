"""Tests for the metrics module."""

import threading
import time

from src.metrics import Metrics, MetricsReporter


class TestMetricsCounters:
    def test_record_sent(self):
        m = Metrics()
        m.record_sent(10.0)
        m.record_sent(20.0)
        snap = m.snapshot_and_reset()
        assert snap["sent"] == 2
        assert snap["failed"] == 0

    def test_record_failed(self):
        m = Metrics()
        m.record_failed()
        m.record_failed()
        m.record_failed()
        snap = m.snapshot_and_reset()
        assert snap["failed"] == 3
        assert snap["sent"] == 0

    def test_latency_tracking(self):
        m = Metrics()
        m.record_sent(10.0)
        m.record_sent(30.0)
        m.record_sent(20.0)
        snap = m.snapshot_and_reset()
        assert snap["avg_latency_ms"] == 20.0
        assert snap["max_latency_ms"] == 30.0

    def test_buffer_usage(self):
        m = Metrics()
        m.record_buffer_usage(100)
        m.record_buffer_usage(200)
        snap = m.snapshot_and_reset()
        assert snap["avg_buffer_usage"] == 150.0


class TestSnapshotReset:
    def test_resets_to_zero(self):
        m = Metrics()
        m.record_sent(5.0)
        m.record_failed()
        m.record_buffer_usage(10)
        m.snapshot_and_reset()

        snap = m.snapshot_and_reset()
        assert snap["sent"] == 0
        assert snap["failed"] == 0
        assert snap["avg_latency_ms"] == 0.0
        assert snap["max_latency_ms"] == 0.0
        assert snap["avg_buffer_usage"] == 0.0

    def test_empty_snapshot(self):
        m = Metrics()
        snap = m.snapshot_and_reset()
        assert snap["sent"] == 0
        assert snap["failed"] == 0
        assert snap["avg_latency_ms"] == 0.0
        assert snap["max_latency_ms"] == 0.0
        assert snap["avg_buffer_usage"] == 0.0


class TestMetricsReporter:
    def test_reporter_lifecycle(self, capsys):
        shutdown = threading.Event()
        m = Metrics()
        m.record_sent(5.0)
        reporter = MetricsReporter(m, interval=0.1, shutdown_event=shutdown)
        reporter.start()
        time.sleep(0.3)
        shutdown.set()
        reporter.stop()

        captured = capsys.readouterr()
        assert "[metrics]" in captured.err
        assert "sent=" in captured.err

    def test_reporter_stops_cleanly(self):
        shutdown = threading.Event()
        m = Metrics()
        reporter = MetricsReporter(m, interval=0.1, shutdown_event=shutdown)
        reporter.start()
        shutdown.set()
        reporter.stop()
        # Should not hang

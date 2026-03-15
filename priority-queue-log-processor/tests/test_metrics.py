"""Tests for the MetricsTracker."""

import threading

from src.metrics import MetricsTracker
from src.models import LogMessage, Priority


class TestRecordEnqueued:
    def test_record_enqueued_increments(self, metrics: MetricsTracker) -> None:
        metrics.record_enqueued(Priority.CRITICAL)
        metrics.record_enqueued(Priority.CRITICAL)
        metrics.record_enqueued(Priority.CRITICAL)

        stats = metrics.get_stats()
        assert stats["enqueued"]["CRITICAL"] == 3


class TestRecordProcessed:
    def test_record_processed_tracks_time(self, metrics: MetricsTracker) -> None:
        metrics.record_processed(Priority.HIGH, 0.1)
        metrics.record_processed(Priority.HIGH, 0.3)

        stats = metrics.get_stats()
        assert stats["processed"]["HIGH"] == 2
        avg = stats["processing_times"]["HIGH"]["avg"]
        assert abs(avg - 0.2) < 1e-6


class TestRecordDropped:
    def test_record_dropped(self, metrics: MetricsTracker) -> None:
        metrics.record_dropped(Priority.LOW)
        metrics.record_dropped(Priority.LOW)

        stats = metrics.get_stats()
        assert stats["dropped"]["LOW"] == 2


class TestGetStats:
    def test_get_stats_shape(self, metrics: MetricsTracker) -> None:
        stats = metrics.get_stats()

        assert "enqueued" in stats
        assert "processed" in stats
        assert "dropped" in stats
        assert "totals" in stats
        assert "processing_times" in stats

        # Each per-priority dict should have all four priority names
        for key in ("enqueued", "processed", "dropped"):
            for p in Priority:
                assert p.name in stats[key]

        # Totals should have the three aggregate keys
        for key in ("enqueued", "processed", "dropped"):
            assert key in stats["totals"]

        # Processing times should have stats per priority
        for p in Priority:
            assert p.name in stats["processing_times"]
            pt = stats["processing_times"][p.name]
            assert "avg" in pt
            assert "p95" in pt
            assert "p99" in pt


class TestThreadSafety:
    def test_thread_safety(self, metrics: MetricsTracker) -> None:
        errors: list[Exception] = []

        def _record() -> None:
            try:
                for _ in range(100):
                    metrics.record_enqueued(Priority.MEDIUM)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_record) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        stats = metrics.get_stats()
        assert stats["enqueued"]["MEDIUM"] == 1000


class TestRecentMessages:
    def test_recent_messages_capped(self, metrics: MetricsTracker) -> None:
        for i in range(100):
            msg = LogMessage(message=f"msg-{i}", priority=Priority.LOW)
            metrics.record_processed(Priority.LOW, 0.01, message=msg)

        recent = metrics.get_recent_messages()
        assert len(recent) == 50

    def test_recent_messages_content(self, metrics: MetricsTracker) -> None:
        msg = LogMessage(message="test message", priority=Priority.HIGH)
        metrics.record_processed(Priority.HIGH, 0.05, message=msg)

        recent = metrics.get_recent_messages()
        assert len(recent) == 1

        entry = recent[0]
        assert "id" in entry
        assert "priority" in entry
        assert "message" in entry
        assert "processing_time_ms" in entry
        assert "timestamp" in entry

        assert entry["priority"] == "HIGH"
        assert entry["message"] == "test message"
        assert entry["processing_time_ms"] == 50.0


class TestProcessingTimePercentiles:
    def test_processing_time_percentiles(self, metrics: MetricsTracker) -> None:
        # Record 100 durations: 0.01, 0.02, ..., 1.00
        for i in range(1, 101):
            metrics.record_processed(Priority.CRITICAL, i * 0.01)

        stats = metrics.get_stats()
        pt = stats["processing_times"]["CRITICAL"]

        # p95 should be around 0.95-0.96, p99 around 0.99-1.00
        assert pt["p95"] >= 0.90
        assert pt["p99"] >= 0.95
        assert pt["avg"] > 0


class TestQueueDepth:
    def test_update_queue_depth(self, metrics: MetricsTracker) -> None:
        # Ensure the method runs without error; Prometheus gauge is set
        # but we verify via internal stats shape only.
        metrics.update_queue_depth({
            Priority.CRITICAL: 5,
            Priority.HIGH: 10,
            Priority.MEDIUM: 20,
            Priority.LOW: 50,
        })
        # No assertion on Prometheus internals; just confirm no exception.


class TestActiveWorkers:
    def test_update_active_workers(self, metrics: MetricsTracker) -> None:
        metrics.update_active_workers(8)
        # Gauge is set; no internal counter to assert against.

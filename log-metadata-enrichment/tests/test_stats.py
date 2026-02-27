"""Tests for the StatsTracker class."""

from src.stats import StatsTracker


class TestStatsTracker:
    """Verify thread-safe statistics tracking."""

    def test_initial_snapshot_is_zero(self):
        tracker = StatsTracker()
        snap = tracker.snapshot()
        assert snap.processed_count == 0
        assert snap.error_count == 0
        assert snap.success_rate == 0.0

    def test_record_success_increments_processed(self):
        tracker = StatsTracker()
        tracker.record_success()
        snap = tracker.snapshot()
        assert snap.processed_count == 1
        assert snap.error_count == 0
        assert snap.success_rate == 1.0

    def test_record_error_increments_both(self):
        tracker = StatsTracker()
        tracker.record_error()
        snap = tracker.snapshot()
        assert snap.processed_count == 1
        assert snap.error_count == 1
        assert snap.success_rate == 0.0

    def test_mixed_success_and_error(self):
        tracker = StatsTracker()
        tracker.record_success()
        tracker.record_success()
        tracker.record_success()
        tracker.record_error()
        snap = tracker.snapshot()
        assert snap.processed_count == 4
        assert snap.error_count == 1
        assert snap.success_rate == 0.75

    def test_runtime_seconds_is_positive(self):
        tracker = StatsTracker()
        tracker.record_success()
        snap = tracker.snapshot()
        assert snap.runtime_seconds > 0

    def test_average_throughput_is_positive_after_records(self):
        tracker = StatsTracker()
        tracker.record_success()
        tracker.record_success()
        snap = tracker.snapshot()
        assert snap.average_throughput > 0

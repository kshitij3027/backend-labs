"""Tests for the metrics module."""

from src.metrics import Metrics


class TestMetrics:
    def test_increment_and_snapshot(self):
        m = Metrics()
        m.increment("INFO")
        m.increment("ERROR")
        m.increment("INFO")

        snap = m.snapshot()
        assert snap["total_received"] == 3
        assert snap["level_distribution"]["INFO"] == 2
        assert snap["level_distribution"]["ERROR"] == 1

    def test_empty_snapshot(self):
        m = Metrics()
        snap = m.snapshot()
        assert snap["total_received"] == 0
        assert snap["level_distribution"] == {}
        assert snap["logs_per_second"] == 0.0

    def test_level_uppercased(self):
        m = Metrics()
        m.increment("info")
        m.increment("Info")

        snap = m.snapshot()
        assert snap["level_distribution"]["INFO"] == 2

    def test_logs_per_second(self):
        m = Metrics()
        for _ in range(10):
            m.increment("INFO")

        snap = m.snapshot()
        assert snap["logs_per_second"] > 0
        assert snap["elapsed_seconds"] >= 0

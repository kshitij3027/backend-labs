"""Tests for CLI dashboard."""
import threading
import pytest
from unittest.mock import MagicMock
from src.monitoring.metrics import MetricsCollector
from src.monitoring.cli_dashboard import CLIDashboard


class TestCLIDashboard:
    def test_build_display_returns_layout(self):
        metrics = MetricsCollector()
        metrics.record_consumed("c-0", 0, 10)
        metrics.record_consumed("c-1", 1, 5)
        metrics.record_rebalance("assign", [0, 1], "c-0")

        dashboard = CLIDashboard(metrics, producer_stats_fn=lambda: {"produced": 100, "errors": 0})
        layout = dashboard.build_display()
        # Should not raise and should return a Layout
        assert layout is not None

    def test_build_display_with_empty_metrics(self):
        metrics = MetricsCollector()
        dashboard = CLIDashboard(metrics)
        layout = dashboard.build_display()
        assert layout is not None

    def test_build_display_with_producer_stats(self):
        metrics = MetricsCollector()
        stats = {"produced": 500, "errors": 2, "per_partition": {0: 100, 1: 100}}
        dashboard = CLIDashboard(metrics, producer_stats_fn=lambda: stats)
        layout = dashboard.build_display()
        assert layout is not None

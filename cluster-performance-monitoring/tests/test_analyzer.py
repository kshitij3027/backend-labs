"""Tests for the PerformanceAnalyzer."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.aggregator import MetricAggregator
from src.analyzer import PerformanceAnalyzer
from src.config import Config
from src.models import MetricPoint
from src.storage import MetricStore


def _make_point(
    node_id: str, metric_name: str, value: float
) -> MetricPoint:
    """Helper to create a MetricPoint with the current UTC timestamp."""
    return MetricPoint(
        timestamp=datetime.now(timezone.utc),
        node_id=node_id,
        metric_name=metric_name,
        value=value,
        labels={},
    )


def _build_analyzer(points: list[MetricPoint]) -> PerformanceAnalyzer:
    """Create a store, aggregator, and analyzer seeded with the given points."""
    store = MetricStore(max_points_per_series=100)
    store.store(points)
    aggregator = MetricAggregator(store, window_seconds=300.0)
    config = Config()
    return PerformanceAnalyzer(aggregator, config)


class TestAlerts:
    """Tests for alert generation."""

    def test_no_alerts_when_below_thresholds(self) -> None:
        """Normal cpu=40 and mem=50 should produce no alerts."""
        points = [
            _make_point("node-1", "cpu_usage", 40.0),
            _make_point("node-1", "memory_usage", 50.0),
        ]
        analyzer = _build_analyzer(points)
        alerts = analyzer.get_alerts()
        assert len(alerts) == 0

    def test_warning_alert_generated(self) -> None:
        """cpu=75 (above 70 warning, below 90 critical) produces a warning."""
        points = [
            _make_point("node-1", "cpu_usage", 75.0),
            _make_point("node-1", "memory_usage", 50.0),
        ]
        analyzer = _build_analyzer(points)
        alerts = analyzer.get_alerts()

        cpu_alerts = [a for a in alerts if a.metric_name == "cpu_usage"]
        assert len(cpu_alerts) == 1
        assert cpu_alerts[0].level == "warning"
        assert cpu_alerts[0].node_id == "node-1"
        assert cpu_alerts[0].current_value == 75.0

    def test_critical_alert_generated(self) -> None:
        """cpu=95 (above 90 critical) produces a critical alert, not a warning."""
        points = [
            _make_point("node-1", "cpu_usage", 95.0),
            _make_point("node-1", "memory_usage", 50.0),
        ]
        analyzer = _build_analyzer(points)
        alerts = analyzer.get_alerts()

        cpu_alerts = [a for a in alerts if a.metric_name == "cpu_usage"]
        assert len(cpu_alerts) == 1
        assert cpu_alerts[0].level == "critical"
        # Should NOT also produce a warning
        warning_cpu = [
            a for a in alerts
            if a.metric_name == "cpu_usage" and a.level == "warning"
        ]
        assert len(warning_cpu) == 0


class TestPerformanceScore:
    """Tests for score computation."""

    def test_performance_score_deductions(self) -> None:
        """Score starts at 100; deduct 10 per warning, 25 per critical."""
        points = [
            # cpu=75 -> warning (-10)
            _make_point("node-1", "cpu_usage", 75.0),
            # memory=96 -> critical (-25)
            _make_point("node-1", "memory_usage", 96.0),
        ]
        analyzer = _build_analyzer(points)
        alerts = analyzer.get_alerts()

        assert len(alerts) == 2
        score = analyzer._compute_score(alerts)
        # 100 - 10 (cpu warning) - 25 (memory critical) = 65
        assert score == pytest.approx(65.0)


class TestHealthStatus:
    """Tests for overall health status determination."""

    def test_health_status_critical(self) -> None:
        """A critical alert results in status 'critical'."""
        points = [
            _make_point("node-1", "cpu_usage", 95.0),
        ]
        analyzer = _build_analyzer(points)
        health = analyzer.evaluate()
        assert health.status == "critical"

    def test_health_status_healthy(self) -> None:
        """No alerts results in status 'healthy'."""
        points = [
            _make_point("node-1", "cpu_usage", 40.0),
            _make_point("node-1", "memory_usage", 50.0),
        ]
        analyzer = _build_analyzer(points)
        health = analyzer.evaluate()
        assert health.status == "healthy"


class TestRecommendations:
    """Tests for recommendation generation."""

    def test_recommendations_generated(self) -> None:
        """Alerts produce non-empty recommendation strings."""
        points = [
            _make_point("node-1", "cpu_usage", 75.0),
            _make_point("node-1", "memory_usage", 96.0),
            _make_point("node-1", "write_latency", 150.0),
        ]
        analyzer = _build_analyzer(points)
        alerts = analyzer.get_alerts()
        recommendations = analyzer._generate_recommendations(alerts)

        assert len(recommendations) > 0
        for rec in recommendations:
            assert isinstance(rec, str)
            assert len(rec) > 0

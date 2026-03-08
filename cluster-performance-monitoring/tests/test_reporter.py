"""Tests for the ReportGenerator."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.aggregator import MetricAggregator
from src.analyzer import PerformanceAnalyzer
from src.config import Config
from src.models import MetricPoint, NodeInfo, PerformanceReport
from src.reporter import ReportGenerator
from src.simulator import NodeSimulator
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


def _build_reporter(
    points: list[MetricPoint], data_dir: Path
) -> ReportGenerator:
    """Create the full pipeline seeded with given points, using tmp data_dir."""
    store = MetricStore(max_points_per_series=100)
    store.store(points)
    aggregator = MetricAggregator(store, window_seconds=300.0)
    config = Config()
    analyzer = PerformanceAnalyzer(aggregator, config)
    return ReportGenerator(analyzer, data_dir=str(data_dir))


class TestReportGeneration:
    """Tests for report generation and persistence."""

    def test_generate_creates_report(self, tmp_path: Path) -> None:
        """Generate a report and verify it returns a valid PerformanceReport."""
        points = [
            _make_point("node-1", "cpu_usage", 50.0),
            _make_point("node-1", "memory_usage", 60.0),
            _make_point("node-1", "throughput", 300.0),
        ]
        reporter = _build_reporter(points, tmp_path)
        report = reporter.generate()

        assert isinstance(report, PerformanceReport)
        assert report.report_id.startswith("perf_report_")
        assert report.generated_at is not None
        assert report.cluster_health is not None
        assert report.performance_summary["active_nodes"] == 1
        assert report.alerts_summary["total"] >= 0

    def test_report_saved_to_disk(self, tmp_path: Path) -> None:
        """Generate report, verify JSON file exists in data_dir."""
        points = [
            _make_point("node-1", "cpu_usage", 50.0),
        ]
        reporter = _build_reporter(points, tmp_path)
        report = reporter.generate()

        report_files = list(tmp_path.glob("perf_report_*.json"))
        assert len(report_files) == 1

        # Verify the file contains valid JSON
        raw = report_files[0].read_text(encoding="utf-8")
        data = json.loads(raw)
        assert data["report_id"] == report.report_id

    def test_get_latest_returns_report(self, tmp_path: Path) -> None:
        """Generate report, call get_latest(), verify same report returned."""
        points = [
            _make_point("node-1", "cpu_usage", 50.0),
            _make_point("node-1", "memory_usage", 60.0),
        ]
        reporter = _build_reporter(points, tmp_path)
        generated = reporter.generate()

        latest = reporter.get_latest()
        assert latest is not None
        assert latest.report_id == generated.report_id
        assert latest.cluster_health.status == generated.cluster_health.status

    def test_get_latest_returns_none_when_empty(self, tmp_path: Path) -> None:
        """Verify None for empty data_dir."""
        points: list[MetricPoint] = []
        reporter = _build_reporter(points, tmp_path)

        latest = reporter.get_latest()
        assert latest is None


class TestIntegrationPipeline:
    """Full integration test through the entire pipeline."""

    def test_integration_pipeline(self, tmp_path: Path) -> None:
        """Full pipeline: simulator -> store -> aggregator -> analyzer -> reporter."""
        # Create simulator
        node_info = NodeInfo(
            node_id="node-1", role="primary", host="localhost", port=8001
        )
        simulator = NodeSimulator(node_info, seed=42)

        # Collect metrics
        store = MetricStore(max_points_per_series=1000)
        for _ in range(5):
            points = simulator.collect()
            store.store(points)

        assert store.point_count() > 0

        # Build aggregator, analyzer, reporter
        aggregator = MetricAggregator(store, window_seconds=300.0)
        config = Config()
        analyzer = PerformanceAnalyzer(aggregator, config)
        reporter = ReportGenerator(analyzer, data_dir=str(tmp_path))

        # Generate report
        report = reporter.generate()

        assert isinstance(report, PerformanceReport)
        assert report.cluster_health.status in {"healthy", "warning", "critical"}
        assert 0.0 <= report.cluster_health.performance_score <= 100.0
        assert report.cluster_health.active_nodes >= 1
        assert len(report.recommendations) > 0

        # Verify the report was saved and can be retrieved
        latest = reporter.get_latest()
        assert latest is not None
        assert latest.report_id == report.report_id

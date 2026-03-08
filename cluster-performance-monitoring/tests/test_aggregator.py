"""Tests for the MetricAggregator."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from src.aggregator import MetricAggregator
from src.models import MetricPoint
from src.storage import MetricStore


def _make_point(
    node_id: str, metric_name: str, value: float, labels: dict | None = None
) -> MetricPoint:
    """Helper to create a MetricPoint with the current UTC timestamp."""
    return MetricPoint(
        timestamp=datetime.now(timezone.utc),
        node_id=node_id,
        metric_name=metric_name,
        value=value,
        labels=labels or {},
    )


class TestGetNodeStats:
    """Tests for MetricAggregator.get_node_stats."""

    def test_node_stats_with_known_data(self) -> None:
        """Seed 10 points for node-1/cpu_usage, verify min/max/avg/p95/p99."""
        store = MetricStore(max_points_per_series=100)
        values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        points = [_make_point("node-1", "cpu_usage", v) for v in values]
        store.store(points)

        aggregator = MetricAggregator(store, window_seconds=300.0)
        result = aggregator.get_node_stats("node-1", "cpu_usage")

        assert result is not None
        assert result.node_id == "node-1"
        assert result.metric_name == "cpu_usage"
        assert result.min == 10.0
        assert result.max == 100.0
        assert result.avg == 55.0
        assert result.count == 10

        arr = np.array(values)
        expected_p95 = float(np.percentile(arr, 95))
        expected_p99 = float(np.percentile(arr, 99))
        assert result.p95 == pytest.approx(expected_p95, rel=1e-6)
        assert result.p99 == pytest.approx(expected_p99, rel=1e-6)

    def test_node_stats_returns_none_for_empty(self) -> None:
        """Empty store returns None."""
        store = MetricStore(max_points_per_series=100)
        aggregator = MetricAggregator(store, window_seconds=300.0)

        result = aggregator.get_node_stats("node-1", "cpu_usage")
        assert result is None


class TestGetAllNodeStats:
    """Tests for MetricAggregator.get_all_node_stats."""

    def test_get_all_node_stats(self) -> None:
        """Seed data for 2 nodes, 2 metrics each. Verify 4 AggregatedMetric results."""
        store = MetricStore(max_points_per_series=100)
        points = [
            _make_point("node-1", "cpu_usage", 50.0),
            _make_point("node-1", "memory_usage", 60.0),
            _make_point("node-2", "cpu_usage", 45.0),
            _make_point("node-2", "memory_usage", 55.0),
        ]
        store.store(points)

        aggregator = MetricAggregator(store, window_seconds=300.0)
        results = aggregator.get_all_node_stats()

        assert len(results) == 4

        # Build a lookup for easy verification
        lookup = {(r.node_id, r.metric_name): r for r in results}
        assert ("node-1", "cpu_usage") in lookup
        assert ("node-1", "memory_usage") in lookup
        assert ("node-2", "cpu_usage") in lookup
        assert ("node-2", "memory_usage") in lookup

        assert lookup[("node-1", "cpu_usage")].avg == 50.0
        assert lookup[("node-2", "memory_usage")].avg == 55.0


class TestClusterTotals:
    """Tests for MetricAggregator.get_cluster_totals."""

    def test_cluster_totals(self) -> None:
        """Seed cpu_usage and throughput for 2 nodes, verify cluster totals."""
        store = MetricStore(max_points_per_series=100)
        points = [
            # Node 1: cpu avg = 60, throughput avg = 300
            _make_point("node-1", "cpu_usage", 60.0),
            _make_point("node-1", "throughput", 300.0),
            # Node 2: cpu avg = 40, throughput avg = 200
            _make_point("node-2", "cpu_usage", 40.0),
            _make_point("node-2", "throughput", 200.0),
        ]
        store.store(points)

        aggregator = MetricAggregator(store, window_seconds=300.0)
        totals = aggregator.get_cluster_totals()

        # avg_cpu_usage = (60 + 40) / 2 = 50
        assert totals["avg_cpu_usage"] == pytest.approx(50.0)
        # total_throughput = 300 + 200 = 500
        assert totals["total_throughput"] == pytest.approx(500.0)
        assert totals["active_nodes"] == 2

    def test_cluster_totals_empty_store(self) -> None:
        """Verify zeros returned for empty store."""
        store = MetricStore(max_points_per_series=100)
        aggregator = MetricAggregator(store, window_seconds=300.0)
        totals = aggregator.get_cluster_totals()

        assert totals["avg_cpu_usage"] == 0.0
        assert totals["avg_memory_usage"] == 0.0
        assert totals["total_throughput"] == 0.0
        assert totals["active_nodes"] == 0

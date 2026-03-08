"""Tests for Pydantic models."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.models import Alert, ClusterHealth, MetricPoint, PerformanceReport


class TestMetricPoint:
    def test_creation_and_json_serialization(self) -> None:
        point = MetricPoint(
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            node_id="node-1",
            metric_name="cpu_usage",
            value=55.3,
        )
        assert point.node_id == "node-1"
        assert point.metric_name == "cpu_usage"
        assert point.value == 55.3
        assert point.labels == {}

        # Serialize to JSON and back
        data = json.loads(point.model_dump_json())
        assert data["node_id"] == "node-1"
        assert data["value"] == 55.3
        assert data["labels"] == {}

    def test_creation_with_labels(self) -> None:
        point = MetricPoint(
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            node_id="node-2",
            metric_name="disk_io",
            value=1024.0,
            labels={"device": "sda", "type": "read"},
        )
        assert point.labels == {"device": "sda", "type": "read"}

        data = json.loads(point.model_dump_json())
        assert data["labels"]["device"] == "sda"


class TestAlert:
    def test_creation(self) -> None:
        alert = Alert(
            level="warning",
            metric_name="cpu_usage",
            node_id="node-1",
            current_value=75.0,
            threshold=70.0,
            message="CPU usage above warning threshold",
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        assert alert.level == "warning"
        assert alert.current_value == 75.0
        assert alert.threshold == 70.0


class TestClusterHealth:
    def test_with_nested_alerts(self) -> None:
        alert = Alert(
            level="critical",
            metric_name="memory_usage",
            node_id="node-3",
            current_value=96.5,
            threshold=95.0,
            message="Memory usage critical",
            timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        health = ClusterHealth(
            status="degraded",
            performance_score=72.5,
            active_nodes=3,
            total_throughput=15000.0,
            avg_cpu_usage=45.0,
            avg_memory_usage=78.0,
            alerts=[alert],
            recommendations=["Scale up node-3 memory"],
        )
        assert health.status == "degraded"
        assert len(health.alerts) == 1
        assert health.alerts[0].level == "critical"
        assert health.recommendations == ["Scale up node-3 memory"]

        # Ensure nested serialization works
        data = json.loads(health.model_dump_json())
        assert data["alerts"][0]["metric_name"] == "memory_usage"


class TestPerformanceReport:
    def test_serialization_round_trip(self) -> None:
        alert = Alert(
            level="warning",
            metric_name="latency",
            node_id="node-2",
            current_value=120.0,
            threshold=100.0,
            message="Latency above warning",
            timestamp=datetime(2025, 6, 15, 8, 30, 0, tzinfo=timezone.utc),
        )
        health = ClusterHealth(
            status="healthy",
            performance_score=95.0,
            active_nodes=3,
            total_throughput=25000.0,
            avg_cpu_usage=35.0,
            avg_memory_usage=55.0,
            alerts=[alert],
            recommendations=["Monitor latency on node-2"],
        )
        report = PerformanceReport(
            report_id="rpt-001",
            generated_at=datetime(2025, 6, 15, 9, 0, 0, tzinfo=timezone.utc),
            cluster_health=health,
            performance_summary={"total_requests": 100000, "error_rate": 0.01},
            alerts_summary={"warning": 1, "critical": 0},
            recommendations=["Monitor latency on node-2"],
            node_metrics={
                "node-1": {"cpu": 30.0, "memory": 50.0},
                "node-2": {"cpu": 40.0, "memory": 60.0},
            },
        )

        # Round-trip through JSON
        json_str = report.model_dump_json()
        restored = PerformanceReport.model_validate_json(json_str)

        assert restored.report_id == "rpt-001"
        assert restored.cluster_health.status == "healthy"
        assert restored.cluster_health.alerts[0].level == "warning"
        assert restored.performance_summary["total_requests"] == 100000
        assert restored.alerts_summary["warning"] == 1
        assert restored.node_metrics["node-1"]["cpu"] == 30.0
        assert restored.recommendations == ["Monitor latency on node-2"]

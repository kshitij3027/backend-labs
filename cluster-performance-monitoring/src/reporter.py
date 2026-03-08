"""Generates and persists JSON performance reports."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from src.analyzer import PerformanceAnalyzer
from src.models import PerformanceReport


class ReportGenerator:
    """Generates and persists JSON performance reports."""

    def __init__(self, analyzer: PerformanceAnalyzer, data_dir: str = "data") -> None:
        self.analyzer = analyzer
        self.data_dir = Path(data_dir)

    def generate(self) -> PerformanceReport:
        """Generate a full performance report and save to disk.

        Returns:
            A ``PerformanceReport`` with cluster health, alerts summary,
            recommendations, and per-node metric details.
        """
        cluster_health = self.analyzer.evaluate()

        # Build per-node metrics from the aggregator
        all_stats = self.analyzer.aggregator.get_all_node_stats()
        node_metrics: dict[str, dict[str, dict]] = defaultdict(dict)
        for stat in all_stats:
            node_metrics[stat.node_id][stat.metric_name] = {
                "min": stat.min,
                "max": stat.max,
                "avg": stat.avg,
                "p95": stat.p95,
                "p99": stat.p99,
                "count": stat.count,
            }

        # Alerts summary
        critical_count = sum(
            1 for a in cluster_health.alerts if a.level == "critical"
        )
        warning_count = sum(
            1 for a in cluster_health.alerts if a.level == "warning"
        )

        ts = int(time.time())
        report_id = f"perf_report_{ts}"

        report = PerformanceReport(
            report_id=report_id,
            generated_at=datetime.now(timezone.utc),
            cluster_health=cluster_health,
            performance_summary={
                "active_nodes": cluster_health.active_nodes,
                "performance_score": cluster_health.performance_score,
                "total_throughput": cluster_health.total_throughput,
            },
            alerts_summary={
                "critical": critical_count,
                "warning": warning_count,
                "total": critical_count + warning_count,
            },
            recommendations=cluster_health.recommendations,
            node_metrics=dict(node_metrics),
        )

        # Persist to disk
        self.data_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.data_dir / f"{report_id}.json"
        report_path.write_text(
            report.model_dump_json(indent=2), encoding="utf-8"
        )

        return report

    def get_latest(self) -> PerformanceReport | None:
        """Read the most recent report file from data_dir.

        Returns:
            The latest ``PerformanceReport`` or ``None`` if no reports exist.
        """
        if not self.data_dir.exists():
            return None

        report_files = sorted(self.data_dir.glob("perf_report_*.json"))
        if not report_files:
            return None

        latest_path = report_files[-1]
        raw = latest_path.read_text(encoding="utf-8")
        return PerformanceReport.model_validate_json(raw)

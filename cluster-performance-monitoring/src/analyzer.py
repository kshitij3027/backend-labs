"""Evaluates metrics against thresholds, generates alerts and health status."""

from __future__ import annotations

from datetime import datetime, timezone

from src.aggregator import MetricAggregator
from src.config import Config
from src.models import Alert, ClusterHealth


# Metrics that are checked against latency thresholds
_LATENCY_METRICS = {"write_latency", "read_latency", "replication_latency"}


class PerformanceAnalyzer:
    """Evaluates metrics against thresholds, generates alerts and health status."""

    def __init__(self, aggregator: MetricAggregator, config: Config) -> None:
        self.aggregator = aggregator
        self.config = config

    def get_alerts(self) -> list[Alert]:
        """Check all aggregated metrics against thresholds and generate alerts.

        Check rules:
        - cpu_usage avg > cpu_critical  -> critical alert
        - cpu_usage avg > cpu_warning   -> warning alert (only if not critical)
        - memory_usage avg > memory_critical -> critical alert
        - memory_usage avg > memory_warning  -> warning alert (only if not critical)
        - write_latency/read_latency/replication_latency avg > latency_critical -> critical
        - write_latency/read_latency/replication_latency avg > latency_warning  -> warning

        Returns:
            List of ``Alert`` objects.
        """
        all_stats = self.aggregator.get_all_node_stats()
        alerts: list[Alert] = []
        now = datetime.now(timezone.utc)

        for stat in all_stats:
            if stat.metric_name == "cpu_usage":
                alert = self._check_threshold(
                    stat.node_id,
                    stat.metric_name,
                    stat.avg,
                    self.config.cpu_warning,
                    self.config.cpu_critical,
                    now,
                )
                if alert is not None:
                    alerts.append(alert)

            elif stat.metric_name == "memory_usage":
                alert = self._check_threshold(
                    stat.node_id,
                    stat.metric_name,
                    stat.avg,
                    self.config.memory_warning,
                    self.config.memory_critical,
                    now,
                )
                if alert is not None:
                    alerts.append(alert)

            elif stat.metric_name in _LATENCY_METRICS:
                alert = self._check_threshold(
                    stat.node_id,
                    stat.metric_name,
                    stat.avg,
                    self.config.latency_warning,
                    self.config.latency_critical,
                    now,
                )
                if alert is not None:
                    alerts.append(alert)

        return alerts

    def _check_threshold(
        self,
        node_id: str,
        metric_name: str,
        value: float,
        warning_threshold: float,
        critical_threshold: float,
        timestamp: datetime,
    ) -> Alert | None:
        """Check a value against warning/critical thresholds.

        Returns a critical alert if above critical, otherwise a warning alert
        if above warning, otherwise None.
        """
        if value > critical_threshold:
            return Alert(
                level="critical",
                metric_name=metric_name,
                node_id=node_id,
                current_value=round(value, 2),
                threshold=critical_threshold,
                message=(
                    f"{metric_name} on {node_id} is {value:.2f}, "
                    f"exceeding critical threshold of {critical_threshold}"
                ),
                timestamp=timestamp,
            )
        elif value > warning_threshold:
            return Alert(
                level="warning",
                metric_name=metric_name,
                node_id=node_id,
                current_value=round(value, 2),
                threshold=warning_threshold,
                message=(
                    f"{metric_name} on {node_id} is {value:.2f}, "
                    f"exceeding warning threshold of {warning_threshold}"
                ),
                timestamp=timestamp,
            )
        return None

    def _compute_score(self, alerts: list[Alert]) -> float:
        """Compute performance score.

        Start at 100, deduct 10 per warning, 25 per critical.
        The score is clamped to a minimum of 0.
        """
        score = 100.0
        for alert in alerts:
            if alert.level == "critical":
                score -= 25.0
            elif alert.level == "warning":
                score -= 10.0
        return max(0.0, score)

    def _generate_recommendations(self, alerts: list[Alert]) -> list[str]:
        """Generate text recommendations based on alert types.

        Returns a list of unique recommendation strings. If there are no
        alerts, returns a single message indicating all metrics are normal.
        """
        if not alerts:
            return ["All metrics within normal thresholds"]

        recommendations: list[str] = []
        seen: set[str] = set()

        for alert in alerts:
            if alert.metric_name == "cpu_usage" and "cpu" not in seen:
                recommendations.append(
                    "Consider scaling horizontally or optimizing CPU-intensive operations"
                )
                seen.add("cpu")
            elif alert.metric_name == "memory_usage" and "memory" not in seen:
                recommendations.append(
                    "Investigate memory leaks or increase available memory"
                )
                seen.add("memory")
            elif alert.metric_name in _LATENCY_METRICS and "latency" not in seen:
                recommendations.append(
                    "Review I/O patterns and consider write batching or caching"
                )
                seen.add("latency")

        return recommendations

    def evaluate(self) -> ClusterHealth:
        """Full evaluation: alerts + score + status + recommendations.

        The status is ``"critical"`` if any critical alert exists,
        ``"warning"`` if any warning alert exists, otherwise ``"healthy"``.

        Returns:
            A ``ClusterHealth`` object summarising the cluster state.
        """
        cluster_totals = self.aggregator.get_cluster_totals()
        alerts = self.get_alerts()
        score = self._compute_score(alerts)
        recommendations = self._generate_recommendations(alerts)

        # Determine overall status
        has_critical = any(a.level == "critical" for a in alerts)
        has_warning = any(a.level == "warning" for a in alerts)

        if has_critical:
            status = "critical"
        elif has_warning:
            status = "warning"
        else:
            status = "healthy"

        return ClusterHealth(
            status=status,
            performance_score=score,
            active_nodes=cluster_totals["active_nodes"],
            total_throughput=cluster_totals["total_throughput"],
            avg_cpu_usage=cluster_totals["avg_cpu_usage"],
            avg_memory_usage=cluster_totals["avg_memory_usage"],
            alerts=alerts,
            recommendations=recommendations,
        )

"""Aggregates metrics from the MetricStore over a configurable time window."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from src.models import AggregatedMetric
from src.storage import MetricStore


class MetricAggregator:
    """Aggregates metrics from the MetricStore over a time window."""

    def __init__(self, store: MetricStore, window_seconds: float = 300.0) -> None:
        self.store = store
        self.window_seconds = window_seconds

    def get_node_stats(
        self, node_id: str, metric_name: str
    ) -> AggregatedMetric | None:
        """Compute min/max/avg/p95/p99 for a specific node+metric over the window.

        Args:
            node_id: The cluster node identifier.
            metric_name: The metric name to aggregate.

        Returns:
            An ``AggregatedMetric`` or ``None`` if no data points exist.
        """
        all_points = self.store.get_all_in_window(self.window_seconds)
        values = [
            p.value
            for p in all_points
            if p.node_id == node_id and p.metric_name == metric_name
        ]

        if not values:
            return None

        arr = np.array(values)
        return AggregatedMetric(
            metric_name=metric_name,
            node_id=node_id,
            min=float(np.min(arr)),
            max=float(np.max(arr)),
            avg=float(np.mean(arr)),
            p95=float(np.percentile(arr, 95)),
            p99=float(np.percentile(arr, 99)),
            count=len(values),
        )

    def get_all_node_stats(self) -> list[AggregatedMetric]:
        """Compute stats for every node+metric combination found in the window.

        Returns:
            A list of ``AggregatedMetric`` objects, one per unique
            ``(node_id, metric_name)`` pair.
        """
        all_points = self.store.get_all_in_window(self.window_seconds)

        # Group values by (node_id, metric_name)
        groups: dict[tuple[str, str], list[float]] = defaultdict(list)
        for p in all_points:
            groups[(p.node_id, p.metric_name)].append(p.value)

        results: list[AggregatedMetric] = []
        for (node_id, metric_name), values in groups.items():
            arr = np.array(values)
            results.append(
                AggregatedMetric(
                    metric_name=metric_name,
                    node_id=node_id,
                    min=float(np.min(arr)),
                    max=float(np.max(arr)),
                    avg=float(np.mean(arr)),
                    p95=float(np.percentile(arr, 95)),
                    p99=float(np.percentile(arr, 99)),
                    count=len(values),
                )
            )

        return results

    def get_cluster_totals(self) -> dict:
        """Compute cluster-wide averages and totals.

        Returns:
            A dict with keys:
            - ``avg_cpu_usage``: average cpu_usage across all nodes
              (average of each node's avg)
            - ``avg_memory_usage``: average memory_usage across all nodes
            - ``total_throughput``: SUM of each node's avg throughput
            - ``active_nodes``: number of distinct node_ids in the window
        """
        all_stats = self.get_all_node_stats()

        if not all_stats:
            return {
                "avg_cpu_usage": 0.0,
                "avg_memory_usage": 0.0,
                "total_throughput": 0.0,
                "active_nodes": 0,
            }

        # Collect per-node averages for cpu and memory
        cpu_avgs: list[float] = []
        memory_avgs: list[float] = []
        throughput_avgs: list[float] = []
        node_ids: set[str] = set()

        for stat in all_stats:
            node_ids.add(stat.node_id)
            if stat.metric_name == "cpu_usage":
                cpu_avgs.append(stat.avg)
            elif stat.metric_name == "memory_usage":
                memory_avgs.append(stat.avg)
            elif stat.metric_name == "throughput":
                throughput_avgs.append(stat.avg)

        return {
            "avg_cpu_usage": float(np.mean(cpu_avgs)) if cpu_avgs else 0.0,
            "avg_memory_usage": float(np.mean(memory_avgs)) if memory_avgs else 0.0,
            "total_throughput": float(np.sum(throughput_avgs)) if throughput_avgs else 0.0,
            "active_nodes": len(node_ids),
        }

"""Tests for the NodeSimulator metric generation."""

from __future__ import annotations

from src.models import MetricPoint, NodeInfo
from src.simulator import NodeSimulator

ALL_METRIC_NAMES = {
    "cpu_usage",
    "memory_usage",
    "disk_io_read",
    "disk_io_write",
    "network_bytes_in",
    "network_bytes_out",
    "write_latency",
    "read_latency",
    "replication_latency",
    "throughput",
    "request_count",
}


class TestCollect:
    """Tests for NodeSimulator.collect()."""

    def test_collect_returns_metric_points(self, simulator: NodeSimulator) -> None:
        """collect() returns a non-empty list of MetricPoint objects."""
        points = simulator.collect()
        assert len(points) > 0
        assert all(isinstance(p, MetricPoint) for p in points)

    def test_all_metric_categories_present(self, simulator: NodeSimulator) -> None:
        """All 11 metric categories appear in a single collect() call."""
        points = simulator.collect()
        names = {p.metric_name for p in points}
        assert names == ALL_METRIC_NAMES

    def test_cpu_has_per_core_labels(self, simulator: NodeSimulator) -> None:
        """cpu_usage points have a 'core' label and there are 4 of them."""
        points = simulator.collect()
        cpu_points = [p for p in points if p.metric_name == "cpu_usage"]
        assert len(cpu_points) == 4
        cores = {p.labels["core"] for p in cpu_points}
        assert cores == {"0", "1", "2", "3"}

    def test_values_in_expected_range(self, simulator: NodeSimulator) -> None:
        """All metric values are >= 0; cpu and memory are <= 100."""
        for _ in range(20):
            points = simulator.collect()
            for p in points:
                assert p.value >= 0, f"{p.metric_name} had negative value {p.value}"
                if p.metric_name in ("cpu_usage", "memory_usage"):
                    assert p.value <= 100, (
                        f"{p.metric_name} exceeded 100: {p.value}"
                    )

    def test_primary_vs_replica_profiles(
        self, primary_node_info: NodeInfo, replica_node_info: NodeInfo
    ) -> None:
        """Primary nodes have higher average CPU than replica nodes."""
        primary_sim = NodeSimulator(primary_node_info, seed=42)
        replica_sim = NodeSimulator(replica_node_info, seed=42)

        primary_cpu_values: list[float] = []
        replica_cpu_values: list[float] = []

        for _ in range(100):
            for p in primary_sim.collect():
                if p.metric_name == "cpu_usage":
                    primary_cpu_values.append(p.value)
            for p in replica_sim.collect():
                if p.metric_name == "cpu_usage":
                    replica_cpu_values.append(p.value)

        primary_avg = sum(primary_cpu_values) / len(primary_cpu_values)
        replica_avg = sum(replica_cpu_values) / len(replica_cpu_values)
        assert primary_avg > replica_avg, (
            f"Primary avg CPU ({primary_avg:.1f}) should be higher "
            f"than replica avg CPU ({replica_avg:.1f})"
        )


class TestDegradation:
    """Tests for degradation injection and clearing."""

    def test_degradation_shifts_values(self, simulator: NodeSimulator) -> None:
        """Injecting 'high_load' causes CPU > 80 and memory > 85."""
        simulator.inject_degradation("high_load")

        for _ in range(20):
            points = simulator.collect()
            cpu_values = [p.value for p in points if p.metric_name == "cpu_usage"]
            mem_values = [p.value for p in points if p.metric_name == "memory_usage"]

            for v in cpu_values:
                assert v > 80, f"Degraded CPU should be > 80, got {v}"
            for v in mem_values:
                assert v > 85, f"Degraded memory should be > 85, got {v}"

    def test_clear_degradation(self, simulator: NodeSimulator) -> None:
        """After clearing degradation, values return to normal ranges."""
        simulator.inject_degradation("high_load")
        simulator.clear_degradation()

        assert not simulator.is_degraded

        cpu_values: list[float] = []
        for _ in range(50):
            points = simulator.collect()
            cpu_values.extend(p.value for p in points if p.metric_name == "cpu_usage")

        avg_cpu = sum(cpu_values) / len(cpu_values)
        # Normal primary CPU mean is 45 +/- 10; should be well below 80
        assert avg_cpu < 70, (
            f"After clearing degradation, avg CPU ({avg_cpu:.1f}) should be < 70"
        )

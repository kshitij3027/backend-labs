"""Simulates realistic metric generation for cluster nodes."""

from __future__ import annotations

import random
from datetime import datetime, timezone

from src.models import MetricPoint, NodeInfo


# Base profiles keyed by role
_PROFILES: dict[str, dict[str, tuple[float, float]]] = {
    "primary": {
        "cpu_usage": (45.0, 10.0),          # mean, stddev
        "memory_usage": (62.0, 8.0),
        "disk_io_read": (20.0, 8.0),        # MB/s
        "disk_io_write": (18.0, 6.0),       # MB/s
        "network_bytes_in": (10.0, 4.0),    # MB/s
        "network_bytes_out": (8.0, 3.0),    # MB/s
        "write_latency": (25.0, 10.0),      # ms
        "read_latency": (10.0, 4.0),        # ms
        "replication_latency": (50.0, 20.0), # ms
        "throughput": (300.0, 80.0),         # ops/sec
        "request_count": (120.0, 30.0),      # count
    },
    "replica": {
        "cpu_usage": (28.0, 8.0),
        "memory_usage": (55.0, 7.0),
        "disk_io_read": (25.0, 8.0),
        "disk_io_write": (10.0, 5.0),
        "network_bytes_in": (8.0, 3.0),
        "network_bytes_out": (6.0, 2.5),
        "write_latency": (10.0, 4.0),
        "read_latency": (8.0, 3.0),
        "replication_latency": (55.0, 22.0),
        "throughput": (250.0, 60.0),
        "request_count": (100.0, 25.0),
    },
}

# Value clamp ranges (min, max) per metric
_CLAMPS: dict[str, tuple[float, float]] = {
    "cpu_usage": (0.0, 100.0),
    "memory_usage": (0.0, 100.0),
    "disk_io_read": (0.0, 500.0),
    "disk_io_write": (0.0, 500.0),
    "network_bytes_in": (0.0, 200.0),
    "network_bytes_out": (0.0, 200.0),
    "write_latency": (0.0, 5000.0),
    "read_latency": (0.0, 5000.0),
    "replication_latency": (0.0, 5000.0),
    "throughput": (0.0, 10000.0),
    "request_count": (0.0, 10000.0),
}

# Degradation overrides: scenario -> metric -> (mean, stddev)
_DEGRADATION: dict[str, dict[str, tuple[float, float]]] = {
    "high_load": {
        "cpu_usage": (90.0, 3.0),
        "memory_usage": (94.0, 2.0),
        "write_latency": (120.0, 30.0),
        "read_latency": (60.0, 15.0),
        "throughput": (450.0, 30.0),
        "request_count": (180.0, 15.0),
    },
    "slow_disk": {
        "disk_io_read": (3.0, 1.0),
        "disk_io_write": (1.5, 0.5),
        "write_latency": (400.0, 100.0),
        "read_latency": (80.0, 20.0),
    },
    "network_issue": {
        "network_bytes_in": (1.0, 0.5),
        "network_bytes_out": (0.8, 0.3),
        "replication_latency": (500.0, 100.0),
        "throughput": (80.0, 20.0),
    },
}

NUM_CPU_CORES = 4


class NodeSimulator:
    """Simulates realistic metric generation for a cluster node."""

    def __init__(self, node_info: NodeInfo, seed: int | None = None) -> None:
        self.node_info = node_info
        self._rng = random.Random(seed)

        role = node_info.role if node_info.role in _PROFILES else "replica"
        self._base_profile = dict(_PROFILES[role])
        self._active_profile = dict(self._base_profile)
        self._degradation_active = False

    def collect(self) -> list[MetricPoint]:
        """Generate a batch of realistic metrics for this node."""
        now = datetime.now(timezone.utc)
        points: list[MetricPoint] = []

        for metric_name, (mean, stddev) in self._active_profile.items():
            if metric_name == "cpu_usage":
                # Generate per-core metrics
                for core in range(NUM_CPU_CORES):
                    value = self._rng.gauss(mean, stddev)
                    value = self._clamp(metric_name, value)
                    points.append(
                        MetricPoint(
                            timestamp=now,
                            node_id=self.node_info.node_id,
                            metric_name=metric_name,
                            value=round(value, 2),
                            labels={"core": str(core)},
                        )
                    )
            else:
                value = self._rng.gauss(mean, stddev)
                value = self._clamp(metric_name, value)
                points.append(
                    MetricPoint(
                        timestamp=now,
                        node_id=self.node_info.node_id,
                        metric_name=metric_name,
                        value=round(value, 2),
                        labels={},
                    )
                )

        return points

    def inject_degradation(self, scenario: str = "high_load") -> None:
        """Activate a degradation scenario.

        Scenarios:
        - "high_load": CPU spikes to 85-95%, memory to 90-98%
        - "slow_disk": disk I/O drops, write latency spikes to 200-600ms
        - "network_issue": network throughput drops, replication latency spikes
        """
        if scenario not in _DEGRADATION:
            raise ValueError(f"Unknown degradation scenario: {scenario!r}")

        self._degradation_active = True
        # Start from base profile and overlay degradation values
        self._active_profile = dict(self._base_profile)
        self._active_profile.update(_DEGRADATION[scenario])

    def clear_degradation(self) -> None:
        """Return to normal operation."""
        self._degradation_active = False
        self._active_profile = dict(self._base_profile)

    @property
    def is_degraded(self) -> bool:
        """Whether a degradation scenario is currently active."""
        return self._degradation_active

    def _clamp(self, metric_name: str, value: float) -> float:
        lo, hi = _CLAMPS.get(metric_name, (0.0, float("inf")))
        return max(lo, min(hi, value))

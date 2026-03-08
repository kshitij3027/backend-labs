"""Shared pytest fixtures for cluster performance monitoring tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import Config
from src.models import MetricPoint, NodeInfo
from src.storage import MetricStore


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> Config:
    """Return a Config loaded purely from defaults (no YAML overlay).

    Any environment variables that might interfere are cleared, and the
    working directory is set to a temporary path so that the YAML file
    is not found.
    """
    env_vars = [
        "HOST", "PORT", "NUM_NODES", "COLLECTION_INTERVAL",
        "RETENTION_SECONDS", "AGGREGATION_WINDOW",
        "CPU_WARNING", "CPU_CRITICAL", "MEMORY_WARNING", "MEMORY_CRITICAL",
        "LATENCY_WARNING", "LATENCY_CRITICAL", "DASHBOARD_REFRESH",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir("/tmp")
    return Config.load()


@pytest.fixture
def metric_store() -> MetricStore:
    """Return a MetricStore with a small buffer for testing."""
    return MetricStore(max_points_per_series=100)


@pytest.fixture
def sample_points() -> list[MetricPoint]:
    """Return 10 MetricPoint objects for node-1 cpu_usage.

    Timestamps are spaced 5 seconds apart, values range from 30 to 70.
    """
    base_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    points: list[MetricPoint] = []
    for i in range(10):
        points.append(
            MetricPoint(
                timestamp=base_time + timedelta(seconds=i * 5),
                node_id="node-1",
                metric_name="cpu_usage",
                value=30.0 + (i * 40.0 / 9),  # linearly from 30 to ~74.4
                labels={},
            )
        )
    return points


@pytest.fixture
def node_info() -> NodeInfo:
    """Return a sample NodeInfo for node-1."""
    return NodeInfo(node_id="node-1", role="primary", host="localhost", port=8001)

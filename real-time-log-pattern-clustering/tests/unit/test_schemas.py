"""Unit tests for the Pydantic v2 data models (project_requirements §8)."""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.schemas import (
    AlgoResult,
    AnomalyAlert,
    ClusterAssignment,
    HealthResponse,
    LogEntry,
    PatternRecord,
    StatsSnapshot,
)


def test_log_entry_parses_iso_timestamp() -> None:
    """LogEntry coerces an ISO-8601 string into a datetime and defaults optionals."""
    entry = LogEntry(
        timestamp="2026-06-23T12:30:00",
        service="auth",
        level="ERROR",
        message="Multiple failed login attempts detected",
    )
    assert isinstance(entry.timestamp, datetime)
    assert entry.timestamp.year == 2026 and entry.timestamp.hour == 12
    assert entry.service == "auth"
    # Optional network/behavioral fields default to None.
    assert entry.source_ip is None
    assert entry.endpoint is None
    assert entry.response_time_ms is None
    assert entry.status_code is None


def test_log_entry_accepts_optional_fields() -> None:
    """Optional fields are populated and typed when provided."""
    entry = LogEntry(
        timestamp="2026-06-23T12:30:00",
        service="api",
        level="INFO",
        message="GET /users 200",
        source_ip="10.0.0.1",
        endpoint="/users",
        response_time_ms=12.5,
        status_code=200,
    )
    assert entry.source_ip == "10.0.0.1"
    assert entry.response_time_ms == 12.5
    assert entry.status_code == 200


def test_algo_result_allows_noise_cluster_and_validates_confidence() -> None:
    """cluster_id == -1 is allowed; confidence must lie in [0, 1]."""
    noise = AlgoResult(algorithm="hdbscan", cluster_id=-1, confidence=0.0, is_anomaly=True)
    assert noise.cluster_id == -1
    assert noise.is_anomaly is True

    ok = AlgoResult(algorithm="kmeans", cluster_id=2, confidence=0.87)
    assert ok.confidence == 0.87
    # is_anomaly defaults to False.
    assert ok.is_anomaly is False

    with pytest.raises(ValidationError):
        AlgoResult(algorithm="dbscan", cluster_id=0, confidence=1.5)


def test_cluster_assignment_holds_algo_results() -> None:
    """ClusterAssignment aggregates per-algorithm results."""
    assignment = ClusterAssignment(
        results=[
            AlgoResult(algorithm="kmeans", cluster_id=2, confidence=0.87),
            AlgoResult(algorithm="dbscan", cluster_id=0, confidence=0.92),
        ],
        is_new_pattern=False,
        is_anomaly=True,
        pattern_type="security_pattern",
        masked_message="Multiple failed login attempts detected",
    )
    assert len(assignment.results) == 2
    assert assignment.pattern_type == "security_pattern"


def test_pattern_record_round_trips() -> None:
    """PatternRecord builds and serializes with datetime fields intact."""
    now = datetime(2026, 6, 23, 12, 0, 0)
    record = PatternRecord(
        pattern_id="p-1",
        pattern_type="security_pattern",
        algorithm="kmeans",
        representative="failed login",
        count=42,
        confidence=0.87,
        first_seen=now,
        last_seen=now,
    )
    dumped = record.model_dump()
    assert dumped["count"] == 42
    assert dumped["pattern_type"] == "security_pattern"


def test_anomaly_alert_defaults_algorithms_list() -> None:
    """AnomalyAlert builds with a list of flagging algorithms."""
    alert = AnomalyAlert(
        timestamp=datetime(2026, 6, 23, 12, 0, 0),
        message="unusual spike",
        service="auth",
        algorithms=["hdbscan"],
        score=0.95,
    )
    assert alert.algorithms == ["hdbscan"]
    assert alert.score == 0.95


def test_stats_snapshot_round_trips() -> None:
    """StatsSnapshot survives a model_dump() round-trip."""
    snap = StatsSnapshot(
        total_processed=1000,
        throughput_per_sec=1247.0,
        total_clusters=12,
        patterns_discovered=5,
        anomalies_detected=2,
        algorithms=["kmeans", "dbscan", "hdbscan"],
    )
    dumped = snap.model_dump()
    restored = StatsSnapshot(**dumped)
    assert restored == snap
    # Optional quality metrics default to None.
    assert restored.silhouette is None
    assert restored.davies_bouldin is None
    assert restored.coherence is None


def test_health_response_lists_three_algorithms() -> None:
    """HealthResponse carries the engine's three clustering algorithms."""
    health = HealthResponse(
        status="ok",
        version="0.1.0",
        algorithms=["kmeans", "dbscan", "hdbscan"],
    )
    assert health.status == "ok"
    assert health.version == "0.1.0"
    assert len(health.algorithms) == 3
    assert set(health.algorithms) == {"kmeans", "dbscan", "hdbscan"}

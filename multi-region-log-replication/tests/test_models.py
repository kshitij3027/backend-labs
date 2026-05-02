"""Unit tests for ``src.models`` — pydantic v2 round-trips and required fields."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    HealthSnapshot,
    LogEntry,
    LogWriteRequest,
    RegionStatus,
)


def test_log_entry_round_trip(sample_log_entry: LogEntry):
    """JSON dump → JSON validate yields an identical model."""
    dumped = sample_log_entry.model_dump_json()
    restored = LogEntry.model_validate_json(dumped)
    assert restored == sample_log_entry


def test_log_entry_defaults_populate_uuid_and_timestamp():
    """log_id and created_at have factories — they must populate when omitted."""
    e = LogEntry(data={"message": "hi"}, region="us-east")
    assert e.log_id and len(e.log_id) > 0
    assert isinstance(e.created_at, float) and e.created_at > 0
    assert e.vector_clock == {}
    assert e.logical_ts == 0


def test_log_entry_required_fields():
    """``data`` and ``region`` are required (no factory) — omission must raise."""
    with pytest.raises(ValidationError):
        LogEntry()  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        LogEntry(data={"message": "x"})  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        LogEntry(region="us-east")  # type: ignore[call-arg]


def test_log_write_request_minimal():
    """Spec body shape: only message/level/service required, all strings."""
    req = LogWriteRequest(message="hi", level="info", service="test")
    assert req.message == "hi"
    assert req.level == "info"
    assert req.service == "test"


def test_log_write_request_missing_field_raises():
    with pytest.raises(ValidationError):
        LogWriteRequest(message="hi", level="info")  # type: ignore[call-arg]


def test_region_status_round_trip():
    rs = RegionStatus(
        region_id="us-east",
        is_primary=True,
        is_healthy=True,
        log_count=42,
        vector_clock={"us-east": 7, "europe": 3, "asia": 5},
        logical_ts=7,
        replication_lag_ms=12.5,
        replication_success_rate=0.999,
    )
    dumped = rs.model_dump_json()
    restored = RegionStatus.model_validate_json(dumped)
    assert restored == rs


def test_region_status_optional_metrics_default_to_none():
    rs = RegionStatus(
        region_id="europe", is_primary=False, is_healthy=True, log_count=0
    )
    assert rs.replication_lag_ms is None
    assert rs.replication_success_rate is None
    assert rs.vector_clock == {}
    assert rs.logical_ts == 0


def test_health_snapshot_round_trip(sample_log_entry: LogEntry):
    snapshot = HealthSnapshot(
        overall_status="healthy",
        regions=[
            RegionStatus(
                region_id="us-east",
                is_primary=True,
                is_healthy=True,
                log_count=1,
                vector_clock={"us-east": 1},
                logical_ts=1,
            ),
            RegionStatus(
                region_id="europe",
                is_primary=False,
                is_healthy=True,
                log_count=1,
            ),
        ],
        current_primary="us-east",
    )
    dumped = snapshot.model_dump_json()
    restored = HealthSnapshot.model_validate_json(dumped)
    assert restored == snapshot

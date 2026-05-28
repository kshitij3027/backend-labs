"""Unit tests for src.models — enum semantics and pydantic validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    DecisionRecord,
    LoadConfig,
    MetricSnapshot,
    OptimizerConfigUpdate,
    OptimizerState,
    OptimizerStatus,
)


# --- OptimizerState ---------------------------------------------------------


def test_optimizer_state_is_str_enum() -> None:
    assert issubclass(OptimizerState, str)
    # Members compare equal to their raw string value.
    assert OptimizerState.LEARNING == "learning"
    assert OptimizerState.OPTIMIZING == "optimizing"


def test_optimizer_state_values() -> None:
    assert OptimizerState.LEARNING.value == "learning"
    assert OptimizerState.OPTIMIZING.value == "optimizing"
    assert OptimizerState.STABLE.value == "stable"
    assert OptimizerState.EMERGENCY.value == "emergency"
    assert {s.value for s in OptimizerState} == {
        "learning",
        "optimizing",
        "stable",
        "emergency",
    }


# --- MetricSnapshot ---------------------------------------------------------


def _snapshot_payload() -> dict:
    return {
        "timestamp": 1717000000.0,
        "batch_size": 100,
        "throughput": 1234.5,
        "latency_ms": 42.0,
        "cpu_percent": 55.0,
        "memory_percent": 60.0,
        "memory_available_mb": 2048.0,
        "queue_depth": 7,
    }


def test_metric_snapshot_constructs_and_round_trips() -> None:
    payload = _snapshot_payload()
    snap = MetricSnapshot(**payload)

    assert snap.batch_size == 100
    assert snap.throughput == pytest.approx(1234.5)
    assert snap.queue_depth == 7

    dumped = snap.model_dump()
    assert dumped == payload
    # Round-trip back into a model yields an equal object.
    assert MetricSnapshot(**dumped) == snap


def test_metric_snapshot_queue_depth_defaults_to_zero() -> None:
    payload = _snapshot_payload()
    del payload["queue_depth"]
    snap = MetricSnapshot(**payload)
    assert snap.queue_depth == 0


# --- OptimizerStatus --------------------------------------------------------


def test_optimizer_status_constructs_and_serializes() -> None:
    status = OptimizerStatus(
        state=OptimizerState.OPTIMIZING,
        batch_size=200,
        last_gradient=0.05,
        smoothing_alpha=0.2,
        min_batch_size=50,
        max_batch_size=5000,
        constraint_active=False,
    )

    assert status.state is OptimizerState.OPTIMIZING
    assert status.reason == ""  # default

    dumped = status.model_dump()
    assert dumped["state"] == "optimizing"
    assert dumped["batch_size"] == 200
    assert dumped["constraint_active"] is False


def test_optimizer_status_accepts_state_as_string() -> None:
    """pydantic coerces a raw string into the OptimizerState enum."""
    status = OptimizerStatus(
        state="stable",
        batch_size=300,
        last_gradient=0.0,
        smoothing_alpha=0.2,
        min_batch_size=50,
        max_batch_size=5000,
        constraint_active=True,
        reason="settled",
    )
    assert status.state is OptimizerState.STABLE
    assert status.reason == "settled"


# --- LoadConfig -------------------------------------------------------------


def test_load_config_defaults() -> None:
    cfg = LoadConfig(messages_per_second=100.0)
    assert cfg.burst_probability == pytest.approx(0.0)
    assert cfg.payload_size_bytes == 256


def test_load_config_accepts_valid_values() -> None:
    cfg = LoadConfig(
        messages_per_second=500.0,
        burst_probability=0.75,
        payload_size_bytes=1024,
    )
    assert cfg.messages_per_second == pytest.approx(500.0)
    assert cfg.burst_probability == pytest.approx(0.75)
    assert cfg.payload_size_bytes == 1024


def test_load_config_accepts_burst_probability_bounds() -> None:
    assert LoadConfig(messages_per_second=1.0, burst_probability=0.0).burst_probability == 0.0
    assert LoadConfig(messages_per_second=1.0, burst_probability=1.0).burst_probability == 1.0


def test_load_config_rejects_negative_messages_per_second() -> None:
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=-1.0)


def test_load_config_rejects_burst_probability_above_one() -> None:
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=10.0, burst_probability=1.5)


def test_load_config_rejects_negative_burst_probability() -> None:
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=10.0, burst_probability=-0.1)


def test_load_config_rejects_negative_payload_size() -> None:
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=10.0, payload_size_bytes=-1)


# --- OptimizerConfigUpdate --------------------------------------------------


def test_optimizer_config_update_all_none_default() -> None:
    """An empty patch (no fields set) is valid; every field defaults to None."""
    patch = OptimizerConfigUpdate()
    dumped = patch.model_dump()
    assert all(value is None for value in dumped.values())


def test_optimizer_config_update_partial() -> None:
    patch = OptimizerConfigUpdate(smoothing_alpha=0.4, max_batch_size=8000)
    assert patch.smoothing_alpha == pytest.approx(0.4)
    assert patch.max_batch_size == 8000
    # Unset fields remain None.
    assert patch.min_batch_size is None

    # exclude_unset captures only what was explicitly provided (patch semantics).
    assert patch.model_dump(exclude_unset=True) == {
        "smoothing_alpha": 0.4,
        "max_batch_size": 8000,
    }


# --- DecisionRecord ---------------------------------------------------------


def test_decision_record_constructs_with_optimizer_state() -> None:
    record = DecisionRecord(
        timestamp=1717000000.0,
        old_batch_size=100,
        new_batch_size=110,
        gradient=0.12,
        state=OptimizerState.OPTIMIZING,
        reason="probing larger batch",
    )
    assert record.state is OptimizerState.OPTIMIZING
    assert record.old_batch_size == 100
    assert record.new_batch_size == 110

    dumped = record.model_dump()
    assert dumped["state"] == "optimizing"
    assert dumped["reason"] == "probing larger batch"

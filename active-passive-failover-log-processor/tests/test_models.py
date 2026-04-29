"""Tests for src/models.py — enum values, dataclass round-trips, helpers."""

from __future__ import annotations

import orjson

from src.models import (
    ElectionMessage,
    ElectionResult,
    HeartbeatMessage,
    LogEntry,
    NodeState,
    StateSnapshot,
    from_json,
    to_json,
)


# --- enum values --------------------------------------------------------


def test_node_state_values() -> None:
    """Each enum member's value matches its name (string-typed enum)."""
    assert NodeState.INACTIVE.value == "INACTIVE"
    assert NodeState.STANDBY.value == "STANDBY"
    assert NodeState.PRIMARY.value == "PRIMARY"
    assert NodeState.ELECTION.value == "ELECTION"
    assert NodeState.FAILED.value == "FAILED"


def test_node_state_inherits_str() -> None:
    """NodeState is a subclass of str so JSON encoders treat it as text."""
    assert isinstance(NodeState.PRIMARY, str)
    assert NodeState.PRIMARY == "PRIMARY"
    # orjson.dumps on a NodeState should produce a JSON string, not int/object
    assert orjson.dumps(NodeState.PRIMARY) == b'"PRIMARY"'


# --- round-trips --------------------------------------------------------


def test_heartbeat_message_round_trip() -> None:
    msg = HeartbeatMessage(
        node_id="node-1",
        timestamp=1234.5,
        state=NodeState.PRIMARY,
        role="primary",
        metrics={"logs_per_sec": 12.0, "last_log_id": 99.0, "log_count": 99.0},
    )
    payload = to_json(msg)
    assert isinstance(payload, bytes)
    restored = from_json(HeartbeatMessage, payload)
    assert restored.node_id == msg.node_id
    assert restored.timestamp == msg.timestamp
    assert restored.role == msg.role
    assert restored.metrics == msg.metrics
    # Critical: state field must rehydrate as the enum, not a bare str.
    assert isinstance(restored.state, NodeState)
    assert restored.state == NodeState.PRIMARY


def test_election_message_round_trip() -> None:
    msg = ElectionMessage(
        candidate="node-2",
        priority=42,
        term=7,
        timestamp=999.0,
    )
    restored = from_json(ElectionMessage, to_json(msg))
    assert restored == msg


def test_election_result_round_trip() -> None:
    result = ElectionResult(winner="node-3", term=8, timestamp=1000.0)
    restored = from_json(ElectionResult, to_json(result))
    assert restored == result


def test_state_snapshot_round_trip_defaults() -> None:
    snap = StateSnapshot()
    assert snap.version == 1
    assert snap.log_count == 0
    restored = from_json(StateSnapshot, to_json(snap))
    assert restored == snap


def test_state_snapshot_round_trip_populated() -> None:
    snap = StateSnapshot(
        version=1,
        log_count=120,
        last_log_id=120,
        watermark=2.5,
        taken_at=1234.5,
    )
    restored = from_json(StateSnapshot, to_json(snap))
    assert restored == snap


def test_log_entry_round_trip() -> None:
    entry = LogEntry(log_id=5, message="hello", level="INFO", timestamp=42.0)
    restored = from_json(LogEntry, to_json(entry))
    assert restored == entry


def test_from_json_accepts_str() -> None:
    entry = LogEntry(log_id=5, message="x", level="DEBUG", timestamp=1.0)
    payload = to_json(entry).decode("utf-8")
    restored = from_json(LogEntry, payload)
    assert restored == entry

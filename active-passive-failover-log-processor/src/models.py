"""Domain models for the active-passive failover cluster.

All wire-format serialization goes through orjson via the helpers at the
bottom of this module. NodeState inherits from str so that JSON round-trips
keep the enum value as a plain string ("PRIMARY", "STANDBY", ...).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Type, TypeVar, Union

import orjson


class NodeState(str, Enum):
    """Lifecycle state of a node in the failover cluster.

    Inheriting from str lets orjson serialize an enum member as its raw
    string value, which makes wire round-trips and Redis storage trivial.
    """

    INACTIVE = "INACTIVE"
    STANDBY = "STANDBY"
    PRIMARY = "PRIMARY"
    ELECTION = "ELECTION"
    FAILED = "FAILED"


@dataclass(slots=True)
class HeartbeatMessage:
    """Heartbeat written by the primary every HEARTBEAT_INTERVAL seconds.

    The metrics dict carries (at minimum) logs_per_sec, last_log_id, and
    log_count so standbys can observe the primary's throughput.
    """

    node_id: str
    timestamp: float
    state: NodeState
    role: str  # "primary" | "standby"
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class ElectionMessage:
    """Candidacy broadcast sent during a leadership election."""

    candidate: str
    priority: int
    term: int
    timestamp: float


@dataclass(slots=True)
class ElectionResult:
    """Final election outcome broadcast to all nodes after a winner is decided."""

    winner: str
    term: int
    timestamp: float


@dataclass(slots=True)
class StateSnapshot:
    """Application state persisted to Redis every STATE_SYNC_INTERVAL seconds.

    Version is fixed at 1 for now; bump it whenever the schema changes so
    older nodes can detect (and refuse to load) incompatible snapshots.
    """

    version: int = 1
    log_count: int = 0
    last_log_id: int = 0
    watermark: float = 0.0
    taken_at: float = 0.0


@dataclass(slots=True)
class LogEntry:
    """A single ingested log line."""

    log_id: int
    message: str
    level: str
    timestamp: float


T = TypeVar("T")


def to_json(obj: Any) -> bytes:
    """Serialize a dataclass instance to a UTF-8 JSON byte string via orjson."""
    return orjson.dumps(asdict(obj))


def from_json(cls: Type[T], data: Union[bytes, str]) -> T:
    """Deserialize JSON data into an instance of ``cls``.

    Accepts either bytes or str. If the dataclass has a ``state`` field,
    the value is rehydrated into a ``NodeState`` enum member rather than
    being left as a raw string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    payload: dict[str, Any] = orjson.loads(data)
    if "state" in payload and not isinstance(payload["state"], NodeState):
        payload["state"] = NodeState(payload["state"])
    return cls(**payload)  # type: ignore[call-arg]

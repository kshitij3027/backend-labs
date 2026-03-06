from enum import Enum
from dataclasses import dataclass, field
import time


class ConsistencyLevel(str, Enum):
    STRONG = "strong"
    BALANCED = "balanced"
    EVENTUAL = "eventual"


@dataclass
class QuorumConfig:
    total_replicas: int = 5
    read_quorum: int = 3
    write_quorum: int = 3
    consistency_level: ConsistencyLevel = ConsistencyLevel.BALANCED
    timeout_ms: int = 5000

    def __post_init__(self):
        self.update_for_consistency_level(self.consistency_level)

    def update_for_consistency_level(self, level: ConsistencyLevel):
        self.consistency_level = level
        n = self.total_replicas
        if level == ConsistencyLevel.STRONG:
            self.read_quorum = n
            self.write_quorum = n
        elif level == ConsistencyLevel.BALANCED:
            self.read_quorum = (n // 2) + 1
            self.write_quorum = (n // 2) + 1
        elif level == ConsistencyLevel.EVENTUAL:
            self.read_quorum = 1
            self.write_quorum = 1

    def validate_consistency(self) -> bool:
        return self.read_quorum + self.write_quorum > self.total_replicas

    def to_dict(self) -> dict:
        return {
            "total_replicas": self.total_replicas,
            "read_quorum": self.read_quorum,
            "write_quorum": self.write_quorum,
            "consistency_level": self.consistency_level.value,
            "timeout_ms": self.timeout_ms,
        }


class VectorClock:
    def __init__(self):
        self.clock: dict[str, int] = {}

    def increment(self, node_id: str):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def update(self, other: "VectorClock"):
        for key, value in other.clock.items():
            self.clock[key] = max(self.clock.get(key, 0), value)

    def compare(self, other: "VectorClock") -> str:
        all_keys = set(self.clock.keys()) | set(other.clock.keys())
        self_le_other = all(
            self.clock.get(k, 0) <= other.clock.get(k, 0) for k in all_keys
        )
        other_le_self = all(
            other.clock.get(k, 0) <= self.clock.get(k, 0) for k in all_keys
        )
        if self_le_other and other_le_self:
            return "equal"
        elif self_le_other:
            return "before"
        elif other_le_self:
            return "after"
        else:
            return "concurrent"

    def copy(self) -> "VectorClock":
        vc = VectorClock()
        vc.clock = dict(self.clock)
        return vc

    def to_dict(self) -> dict:
        return dict(self.clock)

    @classmethod
    def from_dict(cls, d: dict) -> "VectorClock":
        vc = cls()
        vc.clock = dict(d)
        return vc


@dataclass
class LogEntry:
    key: str
    value: str
    timestamp: float = field(default_factory=time.time)
    vector_clock: VectorClock = field(default_factory=VectorClock)
    node_id: str = ""

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock.to_dict(),
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(
            key=d["key"],
            value=d["value"],
            timestamp=d.get("timestamp", time.time()),
            vector_clock=VectorClock.from_dict(d.get("vector_clock", {})),
            node_id=d.get("node_id", ""),
        )

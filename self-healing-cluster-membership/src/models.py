"""Data models for the self-healing cluster membership system."""

import time
from dataclasses import dataclass, field
from enum import Enum


class NodeStatus(str, Enum):
    """Status of a node in the cluster."""
    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    FAILED = "failed"


class NodeRole(str, Enum):
    """Role of a node in the cluster."""
    LEADER = "leader"
    WORKER = "worker"


@dataclass
class NodeInfo:
    """Information about a single node in the cluster."""
    node_id: str
    address: str
    port: int
    role: NodeRole
    status: NodeStatus
    last_seen: float = field(default_factory=time.time)
    heartbeat_count: int = 0
    suspicion_level: float = 0.0
    incarnation: int = 0

    def to_dict(self) -> dict:
        """Serialize NodeInfo to a dictionary suitable for orjson."""
        return {
            "node_id": self.node_id,
            "address": self.address,
            "port": self.port,
            "role": self.role.value,
            "status": self.status.value,
            "last_seen": self.last_seen,
            "heartbeat_count": self.heartbeat_count,
            "suspicion_level": self.suspicion_level,
            "incarnation": self.incarnation,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NodeInfo":
        """Deserialize a dictionary into a NodeInfo instance."""
        return cls(
            node_id=data["node_id"],
            address=data["address"],
            port=data["port"],
            role=NodeRole(data["role"]),
            status=NodeStatus(data["status"]),
            last_seen=data.get("last_seen", time.time()),
            heartbeat_count=data.get("heartbeat_count", 0),
            suspicion_level=data.get("suspicion_level", 0.0),
            incarnation=data.get("incarnation", 0),
        )


@dataclass
class GossipMessage:
    """A gossip message exchanged between nodes."""
    sender_id: str
    digest: list[dict]
    timestamp: float

    def to_dict(self) -> dict:
        """Serialize GossipMessage to a dictionary."""
        return {
            "sender_id": self.sender_id,
            "digest": self.digest,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GossipMessage":
        """Deserialize a dictionary into a GossipMessage instance."""
        return cls(
            sender_id=data["sender_id"],
            digest=data["digest"],
            timestamp=data["timestamp"],
        )

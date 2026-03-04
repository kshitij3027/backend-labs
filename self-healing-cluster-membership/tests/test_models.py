"""Tests for data models (NodeInfo, GossipMessage, enums)."""

import time

import orjson
import pytest

from src.models import GossipMessage, NodeInfo, NodeRole, NodeStatus


class TestNodeStatusEnum:
    """Tests for NodeStatus enum values."""

    def test_healthy_value(self):
        assert NodeStatus.HEALTHY.value == "healthy"

    def test_suspected_value(self):
        assert NodeStatus.SUSPECTED.value == "suspected"

    def test_failed_value(self):
        assert NodeStatus.FAILED.value == "failed"

    def test_string_enum(self):
        """NodeStatus values are strings."""
        assert isinstance(NodeStatus.HEALTHY, str)
        assert NodeStatus.HEALTHY == "healthy"


class TestNodeRoleEnum:
    """Tests for NodeRole enum values."""

    def test_leader_value(self):
        assert NodeRole.LEADER.value == "leader"

    def test_worker_value(self):
        assert NodeRole.WORKER.value == "worker"

    def test_string_enum(self):
        """NodeRole values are strings."""
        assert isinstance(NodeRole.LEADER, str)
        assert NodeRole.LEADER == "leader"


class TestNodeInfo:
    """Tests for NodeInfo dataclass."""

    def test_creation_with_defaults(self):
        """NodeInfo can be created with minimal args; defaults are sane."""
        node = NodeInfo(
            node_id="node-1",
            address="127.0.0.1",
            port=5000,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        assert node.node_id == "node-1"
        assert node.address == "127.0.0.1"
        assert node.port == 5000
        assert node.role == NodeRole.WORKER
        assert node.status == NodeStatus.HEALTHY
        assert node.heartbeat_count == 0
        assert node.suspicion_level == 0.0
        assert node.incarnation == 0
        # last_seen should be approximately now
        assert abs(node.last_seen - time.time()) < 2.0

    def test_to_dict(self):
        """to_dict produces a plain dict with string enum values."""
        node = NodeInfo(
            node_id="node-1",
            address="10.0.0.1",
            port=5000,
            role=NodeRole.LEADER,
            status=NodeStatus.SUSPECTED,
            last_seen=1000.0,
            heartbeat_count=5,
            suspicion_level=3.2,
            incarnation=2,
        )
        d = node.to_dict()
        assert d["node_id"] == "node-1"
        assert d["address"] == "10.0.0.1"
        assert d["port"] == 5000
        assert d["role"] == "leader"
        assert d["status"] == "suspected"
        assert d["last_seen"] == 1000.0
        assert d["heartbeat_count"] == 5
        assert d["suspicion_level"] == 3.2
        assert d["incarnation"] == 2

    def test_from_dict(self):
        """from_dict reconstructs a NodeInfo from a plain dict."""
        d = {
            "node_id": "node-2",
            "address": "10.0.0.2",
            "port": 5001,
            "role": "worker",
            "status": "healthy",
            "last_seen": 2000.0,
            "heartbeat_count": 10,
            "suspicion_level": 0.0,
            "incarnation": 1,
        }
        node = NodeInfo.from_dict(d)
        assert node.node_id == "node-2"
        assert node.role == NodeRole.WORKER
        assert node.status == NodeStatus.HEALTHY
        assert node.incarnation == 1

    def test_round_trip(self):
        """to_dict -> from_dict produces an equivalent NodeInfo."""
        original = NodeInfo(
            node_id="node-3",
            address="10.0.0.3",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.FAILED,
            last_seen=3000.0,
            heartbeat_count=42,
            suspicion_level=9.5,
            incarnation=7,
        )
        restored = NodeInfo.from_dict(original.to_dict())
        assert restored.node_id == original.node_id
        assert restored.address == original.address
        assert restored.port == original.port
        assert restored.role == original.role
        assert restored.status == original.status
        assert restored.last_seen == original.last_seen
        assert restored.heartbeat_count == original.heartbeat_count
        assert restored.suspicion_level == original.suspicion_level
        assert restored.incarnation == original.incarnation

    def test_orjson_serialization(self):
        """NodeInfo.to_dict() output is valid for orjson serialization."""
        node = NodeInfo(
            node_id="node-4",
            address="10.0.0.4",
            port=5003,
            role=NodeRole.LEADER,
            status=NodeStatus.HEALTHY,
        )
        data = node.to_dict()
        serialized = orjson.dumps(data)
        assert isinstance(serialized, bytes)
        deserialized = orjson.loads(serialized)
        assert deserialized["node_id"] == "node-4"
        assert deserialized["role"] == "leader"


class TestGossipMessage:
    """Tests for GossipMessage dataclass."""

    def test_creation(self):
        """GossipMessage can be created with required fields."""
        msg = GossipMessage(
            sender_id="node-1",
            digest=[{"node_id": "node-2", "status": "healthy"}],
            timestamp=1000.0,
        )
        assert msg.sender_id == "node-1"
        assert len(msg.digest) == 1
        assert msg.timestamp == 1000.0

    def test_to_dict(self):
        """to_dict produces a serializable dictionary."""
        msg = GossipMessage(
            sender_id="node-1",
            digest=[{"node_id": "node-2"}],
            timestamp=5000.0,
        )
        d = msg.to_dict()
        assert d["sender_id"] == "node-1"
        assert d["digest"] == [{"node_id": "node-2"}]
        assert d["timestamp"] == 5000.0

    def test_from_dict(self):
        """from_dict reconstructs a GossipMessage from a plain dict."""
        d = {
            "sender_id": "node-3",
            "digest": [{"node_id": "node-1"}, {"node_id": "node-2"}],
            "timestamp": 6000.0,
        }
        msg = GossipMessage.from_dict(d)
        assert msg.sender_id == "node-3"
        assert len(msg.digest) == 2
        assert msg.timestamp == 6000.0

    def test_round_trip(self):
        """to_dict -> from_dict round-trip preserves data."""
        original = GossipMessage(
            sender_id="node-5",
            digest=[{"node_id": "node-1", "incarnation": 3}],
            timestamp=7777.0,
        )
        restored = GossipMessage.from_dict(original.to_dict())
        assert restored.sender_id == original.sender_id
        assert restored.digest == original.digest
        assert restored.timestamp == original.timestamp

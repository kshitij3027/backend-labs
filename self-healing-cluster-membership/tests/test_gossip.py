"""Tests for the gossip protocol."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
from aioresponses import aioresponses

from src.config import ClusterConfig
from src.gossip import GossipProtocol
from src.models import GossipMessage, NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


@pytest.fixture
def gossip_config() -> ClusterConfig:
    """Return a config tuned for gossip protocol testing."""
    return ClusterConfig(
        node_id="test-node-1",
        address="127.0.0.1",
        port=5001,
        gossip_interval=0.1,
        gossip_fanout=3,
    )


@pytest.fixture
def gossip_registry() -> MembershipRegistry:
    """Return a fresh registry for gossip tests."""
    return MembershipRegistry()


@pytest.fixture
async def gossip_protocol(
    gossip_config: ClusterConfig,
    gossip_registry: MembershipRegistry,
) -> GossipProtocol:
    """Return a GossipProtocol instance."""
    return GossipProtocol(config=gossip_config, registry=gossip_registry)


async def _register_peer(
    registry: MembershipRegistry,
    node_id: str = "peer-1",
    address: str = "127.0.0.1",
    port: int = 5002,
    status: NodeStatus = NodeStatus.HEALTHY,
) -> NodeInfo:
    """Helper to register a peer node in the registry."""
    node = NodeInfo(
        node_id=node_id,
        address=address,
        port=port,
        role=NodeRole.WORKER,
        status=status,
        last_seen=time.time(),
        heartbeat_count=0,
    )
    await registry.update_node(node)
    return node


class TestSelectGossipTargets:
    """Tests for gossip target selection."""

    async def test_select_gossip_targets_respects_fanout(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """Targets should be <= gossip_fanout (3) even with 5 peers."""
        await gossip_registry.register_self(gossip_config)
        for i in range(5):
            await _register_peer(
                gossip_registry,
                node_id=f"peer-{i}",
                port=5010 + i,
            )

        targets = await gossip_protocol._select_gossip_targets()
        assert len(targets) <= gossip_config.gossip_fanout
        assert len(targets) == gossip_config.gossip_fanout  # exactly 3 from 5

    async def test_select_gossip_targets_excludes_failed(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """Failed nodes should be excluded from gossip targets."""
        await gossip_registry.register_self(gossip_config)
        await _register_peer(gossip_registry, "peer-1", port=5010)
        await _register_peer(
            gossip_registry, "peer-2", port=5011, status=NodeStatus.FAILED
        )
        await _register_peer(
            gossip_registry, "peer-3", port=5012, status=NodeStatus.FAILED
        )

        # Run multiple times to ensure failed nodes are never selected
        for _ in range(20):
            targets = await gossip_protocol._select_gossip_targets()
            target_ids = {t.node_id for t in targets}
            assert "peer-2" not in target_ids
            assert "peer-3" not in target_ids

    async def test_select_gossip_targets_empty_when_alone(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """When this node is the only one registered, targets should be empty."""
        await gossip_registry.register_self(gossip_config)

        targets = await gossip_protocol._select_gossip_targets()
        assert targets == []


class TestHandleGossip:
    """Tests for incoming gossip handling."""

    async def test_handle_gossip_merges_digest(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """Handling gossip with a new node should add it to the registry."""
        await gossip_registry.register_self(gossip_config)

        new_node_dict = {
            "node_id": "new-node",
            "address": "10.0.0.50",
            "port": 5050,
            "role": "worker",
            "status": "healthy",
            "last_seen": time.time(),
            "heartbeat_count": 5,
            "suspicion_level": 0.0,
            "incarnation": 0,
        }
        message = GossipMessage(
            sender_id="some-sender",
            digest=[new_node_dict],
            timestamp=time.time(),
        )

        await gossip_protocol.handle_gossip(message)

        node = await gossip_registry.get_node("new-node")
        assert node is not None
        assert node.address == "10.0.0.50"
        assert node.port == 5050
        assert node.heartbeat_count == 5

    async def test_handle_gossip_refutes_self_suspicion(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """If gossip says we are SUSPECTED, we should refute by bumping incarnation."""
        await gossip_registry.register_self(gossip_config)

        self_suspected_dict = {
            "node_id": gossip_config.node_id,
            "address": gossip_config.address,
            "port": gossip_config.port,
            "role": "worker",
            "status": "suspected",
            "last_seen": time.time(),
            "heartbeat_count": 0,
            "suspicion_level": 3.0,
            "incarnation": 0,
        }
        message = GossipMessage(
            sender_id="some-sender",
            digest=[self_suspected_dict],
            timestamp=time.time(),
        )

        await gossip_protocol.handle_gossip(message)

        self_node = await gossip_registry.get_node(gossip_config.node_id)
        assert self_node.status == NodeStatus.HEALTHY
        assert self_node.incarnation == 1
        assert self_node.suspicion_level == 0.0

    async def test_handle_gossip_refutes_self_failure(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """If gossip says we are FAILED, we should refute by bumping incarnation."""
        await gossip_registry.register_self(gossip_config)

        self_failed_dict = {
            "node_id": gossip_config.node_id,
            "address": gossip_config.address,
            "port": gossip_config.port,
            "role": "worker",
            "status": "failed",
            "last_seen": time.time(),
            "heartbeat_count": 0,
            "suspicion_level": 10.0,
            "incarnation": 2,
        }
        message = GossipMessage(
            sender_id="some-sender",
            digest=[self_failed_dict],
            timestamp=time.time(),
        )

        await gossip_protocol.handle_gossip(message)

        self_node = await gossip_registry.get_node(gossip_config.node_id)
        assert self_node.status == NodeStatus.HEALTHY
        assert self_node.incarnation == 3  # max(0, 2) + 1
        assert self_node.suspicion_level == 0.0


class TestLifecycle:
    """Tests for gossip protocol start/stop lifecycle."""

    async def test_start_stop_lifecycle(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """start() creates a task, stop() cancels it."""
        await gossip_registry.register_self(gossip_config)

        await gossip_protocol.start()
        assert gossip_protocol._task is not None
        assert not gossip_protocol._task.done()

        await gossip_protocol.stop()
        assert gossip_protocol._task.done()


class TestDoGossipRound:
    """Tests for the single gossip round method."""

    async def test_do_gossip_round_with_mock(
        self,
        gossip_protocol: GossipProtocol,
        gossip_config: ClusterConfig,
        gossip_registry: MembershipRegistry,
    ) -> None:
        """do_gossip_round should send HTTP POSTs to peer gossip endpoints."""
        await gossip_registry.register_self(gossip_config)
        peer1 = await _register_peer(gossip_registry, "peer-1", port=5010)
        peer2 = await _register_peer(gossip_registry, "peer-2", port=5011)

        with aioresponses() as mocked:
            mocked.post(
                f"http://{peer1.address}:{peer1.port}/gossip",
                status=200,
            )
            mocked.post(
                f"http://{peer2.address}:{peer2.port}/gossip",
                status=200,
            )

            await gossip_protocol.do_gossip_round()

            # Verify that requests were made (aioresponses tracks them)
            requests = mocked.requests
            urls_called = set()
            for key, call_list in requests.items():
                for call in call_list:
                    urls_called.add(str(key[1]))

            assert f"http://{peer1.address}:{peer1.port}/gossip" in urls_called
            assert f"http://{peer2.address}:{peer2.port}/gossip" in urls_called

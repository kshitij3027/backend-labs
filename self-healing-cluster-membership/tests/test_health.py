"""Tests for the health monitor."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.config import ClusterConfig
from src.failure_detector import PhiAccrualFailureDetector
from src.health import HealthMonitor
from src.models import NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


@pytest.fixture
def health_config() -> ClusterConfig:
    """Return a config tuned for health monitor testing."""
    return ClusterConfig(
        node_id="test-node-1",
        address="127.0.0.1",
        port=5001,
        health_check_interval=0.1,
        phi_threshold=8.0,
        suspected_health_check_multiplier=0.5,
        heartbeat_window_size=10,
    )


@pytest.fixture
def health_registry() -> MembershipRegistry:
    """Return a fresh registry for health tests."""
    return MembershipRegistry()


@pytest.fixture
def health_detector(health_config: ClusterConfig) -> PhiAccrualFailureDetector:
    """Return a fresh failure detector for health tests."""
    return PhiAccrualFailureDetector(health_config)


@pytest.fixture
async def health_monitor(
    health_config: ClusterConfig,
    health_registry: MembershipRegistry,
    health_detector: PhiAccrualFailureDetector,
) -> HealthMonitor:
    """Return a HealthMonitor with no failure callback."""
    return HealthMonitor(
        config=health_config,
        registry=health_registry,
        detector=health_detector,
    )


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


class TestHandleHeartbeat:
    """Tests for handling incoming heartbeats."""

    async def test_handle_heartbeat_records_in_detector(
        self,
        health_monitor: HealthMonitor,
        health_registry: MembershipRegistry,
        health_detector: PhiAccrualFailureDetector,
    ) -> None:
        """handle_heartbeat should record the heartbeat in the failure detector."""
        await _register_peer(health_registry, "peer-1")

        # First heartbeat sets last_heartbeat but no interval yet
        await health_monitor.handle_heartbeat("peer-1")
        assert "peer-1" in health_detector._last_heartbeat

        # Second heartbeat creates an interval entry
        await health_monitor.handle_heartbeat("peer-1")
        assert "peer-1" in health_detector._windows
        assert len(health_detector._windows["peer-1"]) == 1

    async def test_handle_heartbeat_updates_registry(
        self,
        health_monitor: HealthMonitor,
        health_registry: MembershipRegistry,
    ) -> None:
        """handle_heartbeat should update last_seen and increment heartbeat_count."""
        await _register_peer(health_registry, "peer-1")
        node_before = await health_registry.get_node("peer-1")
        original_last_seen = node_before.last_seen
        original_heartbeat_count = node_before.heartbeat_count

        await health_monitor.handle_heartbeat("peer-1")

        node_after = await health_registry.get_node("peer-1")
        assert node_after.last_seen >= original_last_seen
        assert node_after.heartbeat_count == original_heartbeat_count + 1

    async def test_handle_heartbeat_resets_suspected_to_healthy(
        self,
        health_monitor: HealthMonitor,
        health_registry: MembershipRegistry,
    ) -> None:
        """A heartbeat from a SUSPECTED node should reset it to HEALTHY."""
        await _register_peer(health_registry, "peer-1")
        await health_registry.mark_suspected("peer-1")

        node = await health_registry.get_node("peer-1")
        assert node.status == NodeStatus.SUSPECTED

        await health_monitor.handle_heartbeat("peer-1")

        node = await health_registry.get_node("peer-1")
        assert node.status == NodeStatus.HEALTHY


class TestHealthCheckLogic:
    """Tests for the health check evaluation logic."""

    async def test_health_check_marks_suspected(
        self,
        health_config: ClusterConfig,
        health_registry: MembershipRegistry,
        health_detector: PhiAccrualFailureDetector,
    ) -> None:
        """A peer with phi >= 1.0 but < threshold should become SUSPECTED."""
        await health_registry.register_self(health_config)
        await _register_peer(health_registry, "peer-1")

        # Record some heartbeats to establish a baseline
        for _ in range(3):
            health_detector.record_heartbeat("peer-1")
            time.sleep(0.05)

        # Manipulate _last_heartbeat to simulate a moderate delay (phi ~ 2-3)
        mean_interval = sum(health_detector._windows["peer-1"]) / len(
            health_detector._windows["peer-1"]
        )
        # Set last_heartbeat far enough back that phi >= 1.0 but < threshold
        health_detector._last_heartbeat["peer-1"] = (
            time.time() - mean_interval * 2.0
        )

        monitor = HealthMonitor(
            config=health_config,
            registry=health_registry,
            detector=health_detector,
        )

        # Run one iteration of the health check loop manually
        peers = await health_registry.get_peers(health_config.node_id)
        for peer in peers:
            if peer.status == NodeStatus.FAILED:
                continue
            phi = health_detector.compute_phi(peer.node_id)
            if phi >= health_config.phi_threshold:
                await health_registry.mark_failed(peer.node_id)
            elif phi >= 1.0:
                if peer.status == NodeStatus.HEALTHY:
                    await health_registry.mark_suspected(peer.node_id)

        node = await health_registry.get_node("peer-1")
        assert node.status == NodeStatus.SUSPECTED

    async def test_health_check_marks_failed(
        self,
        health_config: ClusterConfig,
        health_registry: MembershipRegistry,
        health_detector: PhiAccrualFailureDetector,
    ) -> None:
        """A peer with phi >= threshold should become FAILED."""
        await health_registry.register_self(health_config)
        await _register_peer(health_registry, "peer-1")

        # Record some heartbeats to establish a baseline
        for _ in range(3):
            health_detector.record_heartbeat("peer-1")
            time.sleep(0.05)

        # Manipulate _last_heartbeat to simulate a very long gap (phi >= 8.0)
        mean_interval = sum(health_detector._windows["peer-1"]) / len(
            health_detector._windows["peer-1"]
        )
        health_detector._last_heartbeat["peer-1"] = (
            time.time() - mean_interval * 10.0
        )

        # Run one iteration of the health check logic manually
        peers = await health_registry.get_peers(health_config.node_id)
        for peer in peers:
            if peer.status == NodeStatus.FAILED:
                continue
            phi = health_detector.compute_phi(peer.node_id)
            if phi >= health_config.phi_threshold:
                await health_registry.mark_failed(peer.node_id)
            elif phi >= 1.0:
                if peer.status == NodeStatus.HEALTHY:
                    await health_registry.mark_suspected(peer.node_id)

        node = await health_registry.get_node("peer-1")
        assert node.status == NodeStatus.FAILED

    async def test_on_node_failed_callback(
        self,
        health_config: ClusterConfig,
        health_registry: MembershipRegistry,
        health_detector: PhiAccrualFailureDetector,
    ) -> None:
        """The on_node_failed callback should be called when a node is marked FAILED."""
        await health_registry.register_self(health_config)
        await _register_peer(health_registry, "peer-1")

        callback = AsyncMock()
        monitor = HealthMonitor(
            config=health_config,
            registry=health_registry,
            detector=health_detector,
            on_node_failed=callback,
        )

        # Record heartbeats and simulate long gap
        for _ in range(3):
            health_detector.record_heartbeat("peer-1")
            time.sleep(0.05)

        mean_interval = sum(health_detector._windows["peer-1"]) / len(
            health_detector._windows["peer-1"]
        )
        health_detector._last_heartbeat["peer-1"] = (
            time.time() - mean_interval * 10.0
        )

        # Start the monitor briefly to let one check cycle run
        await monitor.start()
        # Give the health check loop time to run one iteration
        await asyncio.sleep(0.2)
        await monitor.stop()

        callback.assert_called_once_with("peer-1")


class TestLifecycle:
    """Tests for monitor start/stop lifecycle."""

    async def test_start_stop_lifecycle(
        self,
        health_monitor: HealthMonitor,
        health_config: ClusterConfig,
        health_registry: MembershipRegistry,
    ) -> None:
        """start() creates tasks, stop() cancels them."""
        await health_registry.register_self(health_config)

        await health_monitor.start()
        assert health_monitor._task is not None
        assert health_monitor._heartbeat_task is not None
        assert not health_monitor._task.done()
        assert not health_monitor._heartbeat_task.done()

        await health_monitor.stop()
        assert health_monitor._task.done()
        assert health_monitor._heartbeat_task.done()


class TestSendHeartbeat:
    """Tests for outgoing heartbeat sending."""

    async def test_send_heartbeat_connection_error(
        self,
        health_monitor: HealthMonitor,
        health_registry: MembershipRegistry,
    ) -> None:
        """A connection error when sending a heartbeat should not crash the monitor."""
        peer = await _register_peer(health_registry, "peer-1", port=59999)

        with aioresponses() as mocked:
            # Mock the heartbeat URL to raise a connection error
            mocked.post(
                f"http://{peer.address}:{peer.port}/heartbeat",
                exception=aiohttp.ClientConnectionError("Connection refused"),
            )
            # Should not raise
            await health_monitor._send_heartbeat(peer)

    async def test_send_heartbeat_success(
        self,
        health_monitor: HealthMonitor,
        health_registry: MembershipRegistry,
    ) -> None:
        """A successful heartbeat send should complete without error."""
        peer = await _register_peer(health_registry, "peer-1")

        with aioresponses() as mocked:
            mocked.post(
                f"http://{peer.address}:{peer.port}/heartbeat",
                status=200,
            )
            await health_monitor._send_heartbeat(peer)

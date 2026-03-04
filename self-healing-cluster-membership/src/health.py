"""Health monitoring with adaptive phi-based failure detection."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Optional

import aiohttp

from src.config import ClusterConfig
from src.failure_detector import PhiAccrualFailureDetector
from src.models import NodeInfo, NodeStatus
from src.registry import MembershipRegistry

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Monitors cluster node health using phi accrual failure detection.

    Sends periodic heartbeats to peers and evaluates phi values to detect failures.
    Uses adaptive check intervals: suspected nodes are checked more frequently.
    """

    def __init__(
        self,
        config: ClusterConfig,
        registry: MembershipRegistry,
        detector: PhiAccrualFailureDetector,
        on_node_failed: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._detector = detector
        self._on_node_failed = on_node_failed
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the health check and heartbeat loops."""
        self._running = True
        self._task = asyncio.create_task(self._health_check_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Health monitor started")

    async def stop(self) -> None:
        """Stop the health check and heartbeat loops."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        logger.info("Health monitor stopped")

    async def _health_check_loop(self) -> None:
        """Periodically evaluate phi for each peer and update status."""
        while self._running:
            try:
                peers = await self._registry.get_peers(self._config.node_id)
                for peer in peers:
                    if peer.status == NodeStatus.FAILED:
                        continue

                    phi = self._detector.compute_phi(peer.node_id)

                    if phi >= self._config.phi_threshold:
                        # Probable failure
                        if peer.status != NodeStatus.FAILED:
                            if await self._has_majority():
                                logger.warning(
                                    "Node %s FAILED (phi=%.2f >= %.2f)",
                                    peer.node_id,
                                    phi,
                                    self._config.phi_threshold,
                                )
                                await self._registry.mark_failed(peer.node_id)
                                if self._on_node_failed:
                                    await self._on_node_failed(peer.node_id)
                            else:
                                logger.warning(
                                    "Node %s may be failed (phi=%.2f) but we "
                                    "don't have majority, skipping",
                                    peer.node_id,
                                    phi,
                                )
                    elif phi >= 1.0:
                        # Suspected
                        if peer.status == NodeStatus.HEALTHY:
                            logger.warning(
                                "Node %s SUSPECTED (phi=%.2f)",
                                peer.node_id,
                                phi,
                            )
                            await self._registry.mark_suspected(peer.node_id)

                # Adaptive interval: check faster if any node is suspected
                has_suspected = any(
                    p.status == NodeStatus.SUSPECTED for p in peers
                )
                if has_suspected:
                    interval = (
                        self._config.health_check_interval
                        * self._config.suspected_health_check_multiplier
                    )
                else:
                    interval = self._config.health_check_interval

                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in health check loop")
                await asyncio.sleep(self._config.health_check_interval)

    async def _has_majority(self) -> bool:
        """Check if this node can reach a majority of known nodes.

        A node has majority when the count of reachable (non-FAILED) nodes
        including itself exceeds total / 2. This prevents minority partitions
        from incorrectly marking nodes as FAILED.
        """
        all_nodes = await self._registry.get_all_nodes()
        total = len(all_nodes)
        if total <= 1:
            return True
        reachable = sum(
            1 for n in all_nodes.values()
            if n.status != NodeStatus.FAILED and n.node_id != self._config.node_id
        )
        # Count self as reachable
        reachable += 1
        return reachable > total / 2

    async def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats to all healthy peers."""
        while self._running:
            try:
                peers = await self._registry.get_peers(self._config.node_id)
                tasks = []
                for peer in peers:
                    if peer.status != NodeStatus.FAILED:
                        tasks.append(self._send_heartbeat(peer))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(self._config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in heartbeat loop")
                await asyncio.sleep(self._config.health_check_interval)

    async def _send_heartbeat(self, peer: NodeInfo) -> None:
        """Send a heartbeat to a single peer via HTTP POST."""
        url = f"http://{peer.address}:{peer.port}/heartbeat"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json={"sender_id": self._config.node_id, "timestamp": time.time()},
                    timeout=aiohttp.ClientTimeout(total=2.0),
                ) as resp:
                    if resp.status == 200:
                        logger.debug("Heartbeat sent to %s", peer.node_id)
                    else:
                        logger.warning(
                            "Heartbeat to %s returned %d",
                            peer.node_id,
                            resp.status,
                        )
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.debug("Failed to send heartbeat to %s", peer.node_id)

    async def handle_heartbeat(self, sender_id: str) -> None:
        """Handle a received heartbeat from another node.

        Records the heartbeat in the failure detector and updates the registry.
        If the node was SUSPECTED or FAILED, resets it to HEALTHY (it's
        clearly alive if it's sending heartbeats).
        """
        self._detector.record_heartbeat(sender_id)

        node = await self._registry.get_node(sender_id)
        if node:
            node.last_seen = time.time()
            node.heartbeat_count += 1
            await self._registry.update_node(node)

            if node.status in (NodeStatus.SUSPECTED, NodeStatus.FAILED):
                logger.info("Node %s recovered from %s to HEALTHY", sender_id, node.status.value)
                await self._registry.mark_healthy(sender_id)

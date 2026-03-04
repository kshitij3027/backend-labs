"""Main cluster member node that orchestrates all components."""

import asyncio
import logging
import time
from typing import Optional

import aiohttp
import orjson

from src.config import ClusterConfig
from src.election import LeaderElection
from src.failure_detector import PhiAccrualFailureDetector
from src.gossip import GossipProtocol
from src.health import HealthMonitor
from src.http_server import HttpServer
from src.models import NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry

logger = logging.getLogger(__name__)


class ClusterMember:
    """A single node in the self-healing cluster.

    Orchestrates: registry, failure detector, gossip, health monitor,
    leader election, and HTTP server.
    """

    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        self._registry = MembershipRegistry()
        self._detector = PhiAccrualFailureDetector(config)
        self._election = LeaderElection(self._registry)
        self._gossip = GossipProtocol(config, self._registry)
        self._health = HealthMonitor(
            config, self._registry, self._detector,
            on_node_failed=self._on_node_failed,
        )
        self._server = HttpServer(
            config, self._registry,
            gossip_handler=self._gossip.handle_gossip,
            heartbeat_handler=self._health.handle_heartbeat,
        )

    @property
    def registry(self) -> MembershipRegistry:
        return self._registry

    @property
    def config(self) -> ClusterConfig:
        return self._config

    async def start(self) -> None:
        """Start the cluster member node."""
        # Register self in the registry
        await self._registry.register_self(self._config)
        logger.info("Node %s registered (role=%s)", self._config.node_id, self._config.role)

        # Start HTTP server first so we can receive messages
        await self._server.start()

        # If this node is configured as leader, set it
        if self._config.role == "leader":
            await self._registry.set_leader(self._config.node_id)
            logger.info("Node %s is the initial leader", self._config.node_id)

        # Start gossip and health monitoring
        await self._gossip.start()
        await self._health.start()

        logger.info("Node %s fully started", self._config.node_id)

    async def join_cluster(self) -> None:
        """Join the cluster by contacting seed nodes."""
        if not self._config.seed_nodes:
            logger.info("No seed nodes configured, starting as standalone")
            return

        self_node = await self._registry.get_node(self._config.node_id)
        if self_node is None:
            logger.error("Self not registered, cannot join cluster")
            return

        joined = False
        cluster_digest = []
        for seed in self._config.seed_nodes:
            try:
                url = f"http://{seed}/join"
                body = orjson.dumps(self_node.to_dict())
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=5.0),
                    ) as resp:
                        if resp.status == 200:
                            data = orjson.loads(await resp.read())
                            cluster_digest = data.get("digest", [])
                            logger.info("Joined cluster via seed %s (got %d nodes)", seed, len(cluster_digest))
                            joined = True
                            break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logger.warning("Failed to join via seed %s", seed)
                continue

        if not joined:
            logger.warning("Could not join any seed node, will discover via gossip")

        # Before merging the cluster digest, check if the cluster has
        # a stale FAILED/SUSPECTED entry for us and bump our incarnation
        # above it so our HEALTHY status wins in SWIM merge rules.
        await self._refute_stale_status(cluster_digest)

        # Now merge the rest of the digest (peers only; our own entry
        # is already correct with the bumped incarnation)
        if cluster_digest:
            await self._registry.merge_digest(cluster_digest)

        # After merge, ensure we are still HEALTHY (in case merge
        # overwrote us with a stale entry at same incarnation)
        await self._ensure_self_healthy()

        # Trigger an immediate gossip round to spread our presence
        await self._gossip.do_gossip_round()

    async def _refute_stale_status(self, digest: list[dict]) -> None:
        """Bump our incarnation above any stale entry the cluster has for us.

        Before merging the cluster digest, we examine it for our own
        node_id. If the cluster has us as FAILED or SUSPECTED, we
        increment our incarnation above theirs so our HEALTHY gossip
        will win under SWIM merge rules (higher incarnation always wins).
        """
        if not digest:
            return

        self_node = await self._registry.get_node(self._config.node_id)
        if self_node is None:
            return

        for entry in digest:
            if entry.get("node_id") == self._config.node_id:
                cluster_incarnation = entry.get("incarnation", 0)
                cluster_status = entry.get("status", "healthy")
                if cluster_status in ("failed", "suspected") or cluster_incarnation >= self_node.incarnation:
                    new_incarnation = max(self_node.incarnation, cluster_incarnation) + 1
                    self_node.incarnation = new_incarnation
                    self_node.status = NodeStatus.HEALTHY
                    self_node.suspicion_level = 0.0
                    await self._registry.update_node(self_node)
                    logger.info(
                        "Refuted stale cluster status (%s, inc=%d) on rejoin, "
                        "incarnation bumped to %d",
                        cluster_status,
                        cluster_incarnation,
                        new_incarnation,
                    )
                break

    async def _ensure_self_healthy(self) -> None:
        """Ensure this node's own registry entry is HEALTHY after digest merge.

        A safety net: if merge_digest overwrote our entry with a stale
        status, force it back to HEALTHY with a bumped incarnation.
        """
        self_node = await self._registry.get_node(self._config.node_id)
        if self_node is None:
            return

        if self_node.status != NodeStatus.HEALTHY:
            self_node.incarnation += 1
            self_node.status = NodeStatus.HEALTHY
            self_node.suspicion_level = 0.0
            await self._registry.update_node(self_node)
            logger.info(
                "Forced self back to HEALTHY (incarnation=%d) after digest merge",
                self_node.incarnation,
            )

    async def stop(self) -> None:
        """Gracefully stop the cluster member."""
        logger.info("Node %s shutting down...", self._config.node_id)
        await self._health.stop()
        await self._gossip.stop()
        await self._server.stop()
        logger.info("Node %s stopped", self._config.node_id)

    async def _on_node_failed(self, failed_node_id: str) -> None:
        """Callback when a node is detected as failed."""
        logger.warning("Node %s detected as FAILED", failed_node_id)
        new_leader = await self._election.on_leader_failure(failed_node_id)
        if new_leader:
            logger.info("Current leader after failure handling: %s", new_leader)

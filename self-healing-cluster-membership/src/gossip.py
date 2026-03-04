"""Gossip protocol for disseminating cluster membership state."""

import asyncio
import logging
import random
import time
from typing import Optional

import aiohttp
import orjson

from src.config import ClusterConfig
from src.models import GossipMessage, NodeStatus
from src.registry import MembershipRegistry

logger = logging.getLogger(__name__)


class GossipProtocol:
    """Gossip-based membership dissemination.

    Periodically selects up to `gossip_fanout` random healthy peers and
    sends them the current membership digest. Handles incoming gossip
    by merging digests and refuting self-suspicion via incarnation bump.
    """

    def __init__(self, config: ClusterConfig, registry: MembershipRegistry) -> None:
        self._config = config
        self._registry = registry
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the gossip dissemination loop."""
        self._running = True
        self._task = asyncio.create_task(self._gossip_loop())
        logger.info(
            "Gossip protocol started (interval=%.1fs, fanout=%d)",
            self._config.gossip_interval,
            self._config.gossip_fanout,
        )

    async def stop(self) -> None:
        """Stop the gossip loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Gossip protocol stopped")

    async def _gossip_loop(self) -> None:
        """Periodically gossip with random peers."""
        while self._running:
            try:
                targets = await self._select_gossip_targets()
                if targets:
                    digest = await self._registry.get_digest()
                    message = GossipMessage(
                        sender_id=self._config.node_id,
                        digest=digest,
                        timestamp=time.time(),
                    )
                    tasks = [self._send_gossip(t, message) for t in targets]
                    await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(self._config.gossip_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in gossip loop")
                await asyncio.sleep(self._config.gossip_interval)

    async def _select_gossip_targets(self) -> list:
        """Select up to gossip_fanout random healthy peers."""
        peers = await self._registry.get_peers(self._config.node_id)
        # Include healthy and suspected peers for gossip, exclude failed
        eligible = [p for p in peers if p.status != NodeStatus.FAILED]
        if not eligible and peers:
            logger.warning("No eligible gossip targets — possible network partition")
        if not eligible:
            return []
        k = min(self._config.gossip_fanout, len(eligible))
        return random.sample(eligible, k)

    async def _send_gossip(self, target, message: GossipMessage) -> None:
        """Send gossip digest to a target node via HTTP POST."""
        url = f"http://{target.address}:{target.port}/gossip"
        try:
            body = orjson.dumps(message.to_dict())
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=2.0),
                ) as resp:
                    if resp.status == 200:
                        logger.debug("Gossip sent to %s", target.node_id)
                    else:
                        logger.warning(
                            "Gossip to %s returned %d", target.node_id, resp.status
                        )
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.debug("Failed to send gossip to %s", target.node_id)

    async def handle_gossip(self, message: GossipMessage) -> None:
        """Handle an incoming gossip message.

        1. Merge the received digest into local registry.
        2. If this node appears as SUSPECTED/FAILED in the digest,
           increment incarnation and re-register as HEALTHY (refutation).
        """
        await self._registry.merge_digest(message.digest)

        # Check if self is suspected/failed in the incoming digest and refute
        for entry in message.digest:
            if entry.get("node_id") == self._config.node_id:
                status = entry.get("status")
                if status in (NodeStatus.SUSPECTED.value, NodeStatus.FAILED.value):
                    await self._refute_suspicion(entry)
                break

    async def _refute_suspicion(self, incoming_entry: dict) -> None:
        """Refute suspicion about this node by incrementing incarnation."""
        self_node = await self._registry.get_node(self._config.node_id)
        if self_node is None:
            return

        # Increment incarnation beyond what's in the incoming gossip
        new_incarnation = max(
            self_node.incarnation,
            incoming_entry.get("incarnation", 0),
        ) + 1

        self_node.incarnation = new_incarnation
        self_node.status = NodeStatus.HEALTHY
        self_node.suspicion_level = 0.0
        await self._registry.update_node(self_node)

        logger.info(
            "Refuted suspicion: incarnation bumped to %d",
            new_incarnation,
        )

    async def do_gossip_round(self) -> None:
        """Perform a single gossip round (useful for testing and on-join)."""
        targets = await self._select_gossip_targets()
        if targets:
            digest = await self._registry.get_digest()
            message = GossipMessage(
                sender_id=self._config.node_id,
                digest=digest,
                timestamp=time.time(),
            )
            tasks = [self._send_gossip(t, message) for t in targets]
            await asyncio.gather(*tasks, return_exceptions=True)

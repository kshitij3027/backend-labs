"""HTTP server for cluster node communication."""

import logging
from typing import Optional

import orjson
from aiohttp import web

from src.config import ClusterConfig
from src.registry import MembershipRegistry

logger = logging.getLogger(__name__)


class HttpServer:
    """aiohttp-based HTTP server for cluster node endpoints.

    Routes:
        GET  /health      - Node health status
        GET  /membership  - All cluster members
        POST /gossip      - Receive gossip digest
        POST /heartbeat   - Receive heartbeat
        POST /join        - Node join request
    """

    def __init__(
        self,
        config: ClusterConfig,
        registry: MembershipRegistry,
        gossip_handler=None,     # will be set: async callable for GossipMessage
        heartbeat_handler=None,  # will be set: async callable for sender_id
    ) -> None:
        self._config = config
        self._registry = registry
        self._gossip_handler = gossip_handler
        self._heartbeat_handler = heartbeat_handler
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None

    def _create_app(self) -> web.Application:
        """Create the aiohttp application with routes."""
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/membership", self._handle_membership)
        app.router.add_post("/gossip", self._handle_gossip)
        app.router.add_post("/heartbeat", self._handle_heartbeat)
        app.router.add_post("/join", self._handle_join)
        return app

    async def start(self) -> None:
        """Start the HTTP server."""
        self._app = self._create_app()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._config.address, self._config.port)
        await site.start()
        logger.info(
            "HTTP server started on %s:%d", self._config.address, self._config.port
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
        logger.info("HTTP server stopped")

    async def _handle_health(self, request: web.Request) -> web.Response:
        """GET /health — return this node's health status."""
        self_node = await self._registry.get_node(self._config.node_id)
        if self_node is None:
            return web.Response(
                body=orjson.dumps(
                    {"status": "unknown", "node_id": self._config.node_id}
                ),
                content_type="application/json",
            )
        return web.Response(
            body=orjson.dumps(
                {
                    "status": self_node.status.value,
                    "node_id": self_node.node_id,
                    "role": self_node.role.value,
                    "incarnation": self_node.incarnation,
                    "heartbeat_count": self_node.heartbeat_count,
                }
            ),
            content_type="application/json",
        )

    async def _handle_membership(self, request: web.Request) -> web.Response:
        """GET /membership — return all known cluster members."""
        digest = await self._registry.get_digest()
        return web.Response(
            body=orjson.dumps({"nodes": digest}),
            content_type="application/json",
        )

    async def _handle_gossip(self, request: web.Request) -> web.Response:
        """POST /gossip — receive and process a gossip digest."""
        try:
            body = await request.read()
            data = orjson.loads(body)
        except Exception:
            return web.Response(
                body=orjson.dumps({"error": "invalid body"}),
                status=400,
                content_type="application/json",
            )

        if self._gossip_handler:
            from src.models import GossipMessage

            message = GossipMessage.from_dict(data)
            await self._gossip_handler(message)

        return web.Response(
            body=orjson.dumps({"status": "ok"}),
            content_type="application/json",
        )

    async def _handle_heartbeat(self, request: web.Request) -> web.Response:
        """POST /heartbeat — receive a heartbeat from a peer."""
        try:
            body = await request.read()
            data = orjson.loads(body)
        except Exception:
            return web.Response(
                body=orjson.dumps({"error": "invalid body"}),
                status=400,
                content_type="application/json",
            )

        sender_id = data.get("sender_id")
        if not sender_id:
            return web.Response(
                body=orjson.dumps({"error": "missing sender_id"}),
                status=400,
                content_type="application/json",
            )

        if self._heartbeat_handler:
            await self._heartbeat_handler(sender_id)

        return web.Response(
            body=orjson.dumps({"status": "ok"}),
            content_type="application/json",
        )

    async def _handle_join(self, request: web.Request) -> web.Response:
        """POST /join — a new node requests to join the cluster."""
        try:
            body = await request.read()
            data = orjson.loads(body)
        except Exception:
            return web.Response(
                body=orjson.dumps({"error": "invalid body"}),
                status=400,
                content_type="application/json",
            )

        # Add the joining node to registry
        from src.models import NodeInfo

        try:
            node_info = NodeInfo.from_dict(data)
            await self._registry.update_node(node_info)
            logger.info("Node %s joined the cluster", node_info.node_id)
        except (KeyError, ValueError) as e:
            return web.Response(
                body=orjson.dumps({"error": f"invalid node data: {str(e)}"}),
                status=400,
                content_type="application/json",
            )

        # Return current cluster digest
        digest = await self._registry.get_digest()
        return web.Response(
            body=orjson.dumps({"status": "ok", "digest": digest}),
            content_type="application/json",
        )

    def get_app(self) -> web.Application:
        """Return the aiohttp app (useful for testing with aiohttp test client)."""
        if self._app is None:
            self._app = self._create_app()
        return self._app

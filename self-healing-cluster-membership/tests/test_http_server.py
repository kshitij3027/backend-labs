"""Tests for the HTTP server."""

import time
from unittest.mock import AsyncMock

import orjson
import pytest
from aiohttp.test_utils import TestClient, TestServer

from src.config import ClusterConfig
from src.http_server import HttpServer
from src.models import GossipMessage, NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


@pytest.fixture
def http_config() -> ClusterConfig:
    """Return a config for HTTP server testing."""
    return ClusterConfig(
        node_id="test-node-1",
        address="127.0.0.1",
        port=5001,
    )


@pytest.fixture
def http_registry() -> MembershipRegistry:
    """Return a fresh registry for HTTP server tests."""
    return MembershipRegistry()


@pytest.fixture
async def http_client(
    http_config: ClusterConfig,
    http_registry: MembershipRegistry,
):
    """Yield (TestClient, gossip_handler, heartbeat_handler) for HTTP testing."""
    await http_registry.register_self(http_config)
    gossip_handler = AsyncMock()
    heartbeat_handler = AsyncMock()
    server = HttpServer(
        http_config, http_registry, gossip_handler, heartbeat_handler
    )
    app = server.get_app()
    async with TestClient(TestServer(app)) as client:
        yield client, gossip_handler, heartbeat_handler


@pytest.fixture
async def http_client_no_registration(
    http_config: ClusterConfig,
    http_registry: MembershipRegistry,
):
    """Yield a TestClient with no node registered (for unknown-node tests)."""
    gossip_handler = AsyncMock()
    heartbeat_handler = AsyncMock()
    server = HttpServer(
        http_config, http_registry, gossip_handler, heartbeat_handler
    )
    app = server.get_app()
    async with TestClient(TestServer(app)) as client:
        yield client


class TestHealthEndpoint:
    """Tests for GET /health."""

    async def test_health_returns_correct_json(self, http_client) -> None:
        """GET /health should return node status, role, incarnation, heartbeat_count."""
        client, _, _ = http_client
        resp = await client.get("/health")
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert data["status"] == "healthy"
        assert data["node_id"] == "test-node-1"
        assert data["role"] == "worker"
        assert data["incarnation"] == 0
        assert data["heartbeat_count"] == 0

    async def test_health_unknown_node(self, http_client_no_registration) -> None:
        """GET /health before registering should return status 'unknown'."""
        client = http_client_no_registration
        resp = await client.get("/health")
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert data["status"] == "unknown"
        assert data["node_id"] == "test-node-1"


class TestMembershipEndpoint:
    """Tests for GET /membership."""

    async def test_membership_returns_all_nodes(
        self, http_client, http_registry
    ) -> None:
        """GET /membership should return all registered nodes."""
        client, _, _ = http_client

        # Add a second node to the registry
        peer = NodeInfo(
            node_id="peer-1",
            address="10.0.0.2",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await http_registry.update_node(peer)

        resp = await client.get("/membership")
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert "nodes" in data
        node_ids = {n["node_id"] for n in data["nodes"]}
        assert node_ids == {"test-node-1", "peer-1"}


class TestGossipEndpoint:
    """Tests for POST /gossip."""

    async def test_gossip_accepts_digest(self, http_client) -> None:
        """POST /gossip with a valid GossipMessage dict should return 200."""
        client, gossip_handler, _ = http_client

        message_dict = GossipMessage(
            sender_id="remote-node",
            digest=[
                {
                    "node_id": "remote-node",
                    "address": "10.0.0.5",
                    "port": 5005,
                    "role": "worker",
                    "status": "healthy",
                    "last_seen": time.time(),
                    "heartbeat_count": 0,
                    "suspicion_level": 0.0,
                    "incarnation": 0,
                }
            ],
            timestamp=time.time(),
        ).to_dict()

        resp = await client.post(
            "/gossip",
            data=orjson.dumps(message_dict),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert data["status"] == "ok"
        gossip_handler.assert_called_once()

    async def test_gossip_invalid_body(self, http_client) -> None:
        """POST /gossip with invalid JSON should return 400."""
        client, _, _ = http_client

        resp = await client.post(
            "/gossip",
            data=b"not valid json{{{",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 400
        data = orjson.loads(await resp.read())
        assert "error" in data


class TestHeartbeatEndpoint:
    """Tests for POST /heartbeat."""

    async def test_heartbeat_records(self, http_client) -> None:
        """POST /heartbeat with sender_id should call the heartbeat handler."""
        client, _, heartbeat_handler = http_client

        resp = await client.post(
            "/heartbeat",
            data=orjson.dumps(
                {"sender_id": "peer-1", "timestamp": time.time()}
            ),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert data["status"] == "ok"
        heartbeat_handler.assert_called_once_with("peer-1")

    async def test_heartbeat_missing_sender_id(self, http_client) -> None:
        """POST /heartbeat without sender_id should return 400."""
        client, _, _ = http_client

        resp = await client.post(
            "/heartbeat",
            data=orjson.dumps({"timestamp": time.time()}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 400
        data = orjson.loads(await resp.read())
        assert "error" in data


class TestJoinEndpoint:
    """Tests for POST /join."""

    async def test_join_adds_node_returns_digest(
        self, http_client, http_registry
    ) -> None:
        """POST /join with valid NodeInfo should add the node and return digest."""
        client, _, _ = http_client

        join_data = {
            "node_id": "joining-node",
            "address": "10.0.0.99",
            "port": 5099,
            "role": "worker",
            "status": "healthy",
            "last_seen": time.time(),
            "heartbeat_count": 0,
            "suspicion_level": 0.0,
            "incarnation": 0,
        }

        resp = await client.post(
            "/join",
            data=orjson.dumps(join_data),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 200
        data = orjson.loads(await resp.read())
        assert data["status"] == "ok"
        assert "digest" in data

        # Verify the node was added to the registry
        node = await http_registry.get_node("joining-node")
        assert node is not None
        assert node.address == "10.0.0.99"
        assert node.port == 5099

        # Verify digest includes both original and new node
        digest_ids = {n["node_id"] for n in data["digest"]}
        assert "test-node-1" in digest_ids
        assert "joining-node" in digest_ids

    async def test_join_invalid_body(self, http_client) -> None:
        """POST /join with invalid body should return 400."""
        client, _, _ = http_client

        resp = await client.post(
            "/join",
            data=b"not valid json{{{",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 400
        data = orjson.loads(await resp.read())
        assert "error" in data

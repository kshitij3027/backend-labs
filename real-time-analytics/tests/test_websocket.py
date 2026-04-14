"""Tests for the WebSocket connection manager and /ws endpoint."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient

from src.main import app
from src.websocket import VALID_STREAMS, ConnectionManager


# ------------------------------------------------------------------
# Unit tests for ConnectionManager
# ------------------------------------------------------------------


class TestConnectionManagerUnit:
    """Test ConnectionManager methods using mocked WebSocket objects."""

    @pytest.mark.asyncio
    async def test_connect_disconnect(self):
        """Connecting increments active_count; disconnecting decrements it."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        client_id = await mgr.connect(ws)

        assert mgr.active_count == 1
        assert client_id  # non-empty string

        await mgr.disconnect(client_id)
        assert mgr.active_count == 0

    @pytest.mark.asyncio
    async def test_connect_sends_welcome(self):
        """The welcome message includes type, client_id, and available_streams."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        client_id = await mgr.connect(ws)

        ws.accept.assert_awaited_once()
        ws.send_json.assert_awaited_once()
        welcome = ws.send_json.call_args[0][0]
        assert welcome["type"] == "connected"
        assert welcome["client_id"] == client_id
        assert set(welcome["available_streams"]) == VALID_STREAMS

    @pytest.mark.asyncio
    async def test_subscribe(self):
        """Subscribing to valid streams records them on the client state."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        client_id = await mgr.connect(ws)

        subscribed = await mgr.subscribe(client_id, ["metrics", "alerts"])
        assert set(subscribed) == {"metrics", "alerts"}

        # Verify internal state
        state = mgr._connections[client_id]
        assert "metrics" in state.subscriptions
        assert "alerts" in state.subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_invalid_stream(self):
        """Invalid stream names are silently dropped."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        client_id = await mgr.connect(ws)

        subscribed = await mgr.subscribe(client_id, ["metrics", "bogus", "nope"])
        assert subscribed == ["metrics"]

    @pytest.mark.asyncio
    async def test_subscribe_nonexistent_client(self):
        """Subscribing an unknown client_id returns an empty list."""
        mgr = ConnectionManager()
        result = await mgr.subscribe("no-such-client", ["metrics"])
        assert result == []

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Unsubscribing removes the stream from the client's set."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        client_id = await mgr.connect(ws)
        await mgr.subscribe(client_id, ["metrics", "alerts"])

        await mgr.unsubscribe(client_id, ["metrics"])
        state = mgr._connections[client_id]
        assert "metrics" not in state.subscriptions
        assert "alerts" in state.subscriptions

    @pytest.mark.asyncio
    async def test_subscriptions_summary(self):
        """subscriptions_summary returns per-stream counts."""
        mgr = ConnectionManager()
        ws1, ws2 = AsyncMock(), AsyncMock()
        cid1 = await mgr.connect(ws1)
        cid2 = await mgr.connect(ws2)

        await mgr.subscribe(cid1, ["metrics", "alerts"])
        await mgr.subscribe(cid2, ["metrics"])

        summary = mgr.subscriptions_summary
        assert summary["metrics"] == 2
        assert summary["alerts"] == 1
        assert summary["system"] == 0

    @pytest.mark.asyncio
    async def test_broadcast_sends_to_subscribers(self):
        """broadcast sends data only to clients subscribed to the stream."""
        mgr = ConnectionManager()
        ws_sub = AsyncMock()
        ws_nosub = AsyncMock()
        cid_sub = await mgr.connect(ws_sub)
        cid_nosub = await mgr.connect(ws_nosub)

        await mgr.subscribe(cid_sub, ["alerts"])
        # ws_nosub is not subscribed to alerts

        # Reset send_json call counts (welcome messages already sent)
        ws_sub.send_json.reset_mock()
        ws_nosub.send_json.reset_mock()

        await mgr.broadcast("alerts", {"msg": "test"})

        ws_sub.send_json.assert_awaited_once()
        sent_payload = ws_sub.send_json.call_args[0][0]
        assert sent_payload["type"] == "alerts_update"
        assert sent_payload["data"] == {"msg": "test"}

        ws_nosub.send_json.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_broadcast_evicts_dead_clients(self):
        """If send_json raises, the client is disconnected."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        ws.send_json.side_effect = [None, RuntimeError("gone")]  # welcome OK, broadcast fails
        cid = await mgr.connect(ws)
        await mgr.subscribe(cid, ["metrics"])

        await mgr.broadcast("metrics", {"v": 1})
        assert mgr.active_count == 0

    @pytest.mark.asyncio
    async def test_handle_pong(self):
        """handle_pong updates the last_pong timestamp."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        cid = await mgr.connect(ws)

        before = mgr._connections[cid].last_pong
        # Tiny sleep to ensure time.time() differs
        await asyncio.sleep(0.01)
        await mgr.handle_pong(cid)
        after = mgr._connections[cid].last_pong
        assert after >= before

    @pytest.mark.asyncio
    async def test_cleanup_stale(self):
        """Clients whose last_pong is older than the timeout are evicted."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        cid = await mgr.connect(ws)

        # Manually set last_pong far in the past
        mgr._connections[cid].last_pong = time.time() - 200

        await mgr.cleanup_stale(timeout_seconds=90.0)
        assert mgr.active_count == 0

    @pytest.mark.asyncio
    async def test_cleanup_stale_keeps_fresh(self):
        """Clients with a recent last_pong are kept."""
        mgr = ConnectionManager()
        ws = AsyncMock()
        cid = await mgr.connect(ws)
        # last_pong defaults to time.time(), so it's fresh

        await mgr.cleanup_stale(timeout_seconds=90.0)
        assert mgr.active_count == 1

    @pytest.mark.asyncio
    async def test_disconnect_idempotent(self):
        """Disconnecting an unknown client_id is a no-op."""
        mgr = ConnectionManager()
        await mgr.disconnect("nonexistent")
        assert mgr.active_count == 0


# ------------------------------------------------------------------
# Integration tests using Starlette's synchronous TestClient
# ------------------------------------------------------------------


class TestWebSocketEndpoint:
    """Integration tests for the ``/ws`` WebSocket endpoint."""

    @pytest.fixture
    def client_sync(self, storage):
        """Provide a synchronous TestClient with WebSocket support.

        We manually wire ``app.state`` instead of running the real
        lifespan (which requires a live Redis).
        """
        app.state.storage = storage
        app.state.ws_manager = ConnectionManager()
        app.state.stop_event = asyncio.Event()
        yield TestClient(app)

    def test_websocket_connect(self, client_sync):
        """Connecting to /ws returns a welcome message."""
        with client_sync.websocket_connect("/ws") as ws:
            data = ws.receive_json()
            assert data["type"] == "connected"
            assert "client_id" in data
            assert "available_streams" in data
            assert set(data["available_streams"]) == VALID_STREAMS

    def test_websocket_subscribe(self, client_sync):
        """Subscribing returns a subscribed confirmation."""
        with client_sync.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume welcome
            ws.send_json({"type": "subscribe", "streams": ["metrics", "alerts"]})
            resp = ws.receive_json()
            assert resp["type"] == "subscribed"
            assert set(resp["streams"]) == {"alerts", "metrics"}

    def test_websocket_subscribe_invalid(self, client_sync):
        """Invalid stream names are filtered out during subscribe."""
        with client_sync.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume welcome
            ws.send_json({"type": "subscribe", "streams": ["bogus", "metrics"]})
            resp = ws.receive_json()
            assert resp["type"] == "subscribed"
            assert resp["streams"] == ["metrics"]

    def test_websocket_unsubscribe(self, client_sync):
        """Unsubscribing returns an unsubscribed confirmation."""
        with client_sync.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume welcome
            ws.send_json({"type": "subscribe", "streams": ["metrics"]})
            ws.receive_json()  # consume subscribed
            ws.send_json({"type": "unsubscribe", "streams": ["metrics"]})
            resp = ws.receive_json()
            assert resp["type"] == "unsubscribed"
            assert resp["streams"] == ["metrics"]

    def test_websocket_pong(self, client_sync):
        """Sending a pong message does not cause an error."""
        with client_sync.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume welcome
            ws.send_json({"type": "pong"})
            # No response expected for pong — just ensure no crash.
            # Sending another subscribe to confirm the connection is alive.
            ws.send_json({"type": "subscribe", "streams": ["system"]})
            resp = ws.receive_json()
            assert resp["type"] == "subscribed"

    def test_websocket_disconnect(self, client_sync):
        """Disconnecting cleanly does not raise."""
        with client_sync.websocket_connect("/ws") as ws:
            data = ws.receive_json()
            assert data["type"] == "connected"
        # Context manager closes — no exception means success.


class TestWSStatusEndpoint:
    """Tests for the ``GET /api/ws-status`` endpoint."""

    @pytest.fixture
    def client_sync(self, storage):
        """Provide a synchronous TestClient."""
        app.state.storage = storage
        app.state.ws_manager = ConnectionManager()
        app.state.stop_event = asyncio.Event()
        yield TestClient(app)

    def test_ws_status_no_connections(self, client_sync):
        """With no WS connections, counts are all zero."""
        resp = client_sync.get("/api/ws-status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["active_connections"] == 0
        assert data["subscriptions"]["metrics"] == 0
        assert data["subscriptions"]["alerts"] == 0
        assert data["subscriptions"]["system"] == 0

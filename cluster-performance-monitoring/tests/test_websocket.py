"""Tests for WebSocket connection manager and real-time metric streaming."""

from __future__ import annotations

import json

import pytest
from unittest.mock import AsyncMock

from src.websocket import ConnectionManager


# ---------------------------------------------------------------------------
# ConnectionManager unit tests (async, isolated)
# ---------------------------------------------------------------------------


async def test_connect_and_disconnect():
    """Connect adds a WebSocket; disconnect removes it."""
    manager = ConnectionManager()
    ws = AsyncMock()
    await manager.connect(ws)
    assert manager.active_count == 1
    ws.accept.assert_awaited_once()
    manager.disconnect(ws)
    assert manager.active_count == 0


async def test_disconnect_idempotent():
    """Disconnecting an unknown WebSocket does not raise."""
    manager = ConnectionManager()
    ws = AsyncMock()
    manager.disconnect(ws)  # should not raise
    assert manager.active_count == 0


async def test_broadcast():
    """Broadcast sends JSON data to all connected clients."""
    manager = ConnectionManager()
    ws = AsyncMock()
    await manager.connect(ws)
    await manager.broadcast({"test": "data"})
    ws.send_text.assert_awaited_once()
    sent = json.loads(ws.send_text.call_args[0][0])
    assert sent["test"] == "data"


async def test_broadcast_removes_dead_connections():
    """Dead connections are removed after a failed broadcast."""
    manager = ConnectionManager()
    ws_good = AsyncMock()
    ws_dead = AsyncMock()
    ws_dead.send_text.side_effect = RuntimeError("connection closed")
    await manager.connect(ws_good)
    await manager.connect(ws_dead)
    assert manager.active_count == 2
    await manager.broadcast({"test": "data"})
    assert manager.active_count == 1
    # The good one should have received the message
    ws_good.send_text.assert_awaited_once()


async def test_broadcast_no_connections():
    """Broadcasting with no connections is a no-op (no errors)."""
    manager = ConnectionManager()
    await manager.broadcast({"test": "data"})  # should not raise


async def test_broadcast_multiple_clients():
    """All connected clients receive the broadcast message."""
    manager = ConnectionManager()
    clients = [AsyncMock() for _ in range(5)]
    for ws in clients:
        await manager.connect(ws)
    assert manager.active_count == 5
    await manager.broadcast({"hello": "world"})
    for ws in clients:
        ws.send_text.assert_awaited_once()
        sent = json.loads(ws.send_text.call_args[0][0])
        assert sent["hello"] == "world"


# ---------------------------------------------------------------------------
# Integration test: WebSocket endpoint with the actual FastAPI app
# ---------------------------------------------------------------------------

import os
os.environ.setdefault("COLLECTION_INTERVAL", "0.5")

from starlette.testclient import TestClient  # noqa: E402
from src.server import app  # noqa: E402


def test_ws_endpoint_receives_data():
    """Connect to /ws and verify we receive a metrics_update message."""
    import time
    with TestClient(app) as client:
        time.sleep(2)  # wait for collectors to generate data
        with client.websocket_connect("/ws") as ws:
            # Wait for a message (collectors should broadcast periodically)
            data = ws.receive_json(mode="text")
            assert data["type"] == "metrics_update"
            assert "node_id" in data
            assert "metrics" in data
            assert "timestamp" in data

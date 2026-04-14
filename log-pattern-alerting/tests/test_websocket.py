"""Tests for the WebSocket ConnectionManager and dashboard endpoint."""

from __future__ import annotations

import pytest
import httpx

from src.websocket import ConnectionManager


# ---------------------------------------------------------------------------
# FakeWebSocket mock
# ---------------------------------------------------------------------------

class FakeWebSocket:
    """Minimal stand-in for a FastAPI WebSocket used in unit tests."""

    def __init__(self, raise_on_send: bool = False) -> None:
        self.accepted = False
        self.sent: list[dict] = []
        self.raise_on_send = raise_on_send

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, payload: dict) -> None:
        if self.raise_on_send:
            raise RuntimeError("simulated disconnect")
        self.sent.append(payload)


# ---------------------------------------------------------------------------
# ConnectionManager unit tests
# ---------------------------------------------------------------------------

class TestConnectionManager:
    """Unit tests for :class:`ConnectionManager`."""

    async def test_starts_empty(self) -> None:
        mgr = ConnectionManager()
        assert mgr.client_count == 0

    async def test_connect_adds_client(self) -> None:
        mgr = ConnectionManager()
        ws = FakeWebSocket()
        await mgr.connect(ws)
        assert mgr.client_count == 1
        assert ws.accepted is True

    async def test_disconnect_removes_client(self) -> None:
        mgr = ConnectionManager()
        ws = FakeWebSocket()
        await mgr.connect(ws)
        assert mgr.client_count == 1
        await mgr.disconnect(ws)
        assert mgr.client_count == 0

    async def test_broadcast_to_multiple(self) -> None:
        mgr = ConnectionManager()
        clients = [FakeWebSocket() for _ in range(3)]
        for ws in clients:
            await mgr.connect(ws)

        payload = {"type": "alert", "message": "test"}
        sent = await mgr.broadcast_json(payload)

        assert sent == 3
        for ws in clients:
            assert ws.sent == [payload]

    async def test_dead_client_eviction(self) -> None:
        mgr = ConnectionManager()
        good1 = FakeWebSocket()
        good2 = FakeWebSocket()
        dead = FakeWebSocket(raise_on_send=True)
        await mgr.connect(good1)
        await mgr.connect(good2)
        await mgr.connect(dead)
        assert mgr.client_count == 3

        payload = {"type": "alert", "message": "evict test"}
        sent = await mgr.broadcast_json(payload)

        assert sent == 2
        assert mgr.client_count == 2
        assert good1.sent == [payload]
        assert good2.sent == [payload]
        assert dead.sent == []

    async def test_broadcast_to_empty(self) -> None:
        mgr = ConnectionManager()
        sent = await mgr.broadcast_json({"msg": "nobody home"})
        assert sent == 0

    async def test_double_disconnect_safe(self) -> None:
        mgr = ConnectionManager()
        ws = FakeWebSocket()
        await mgr.connect(ws)
        await mgr.disconnect(ws)
        await mgr.disconnect(ws)  # should not raise
        assert mgr.client_count == 0


# ---------------------------------------------------------------------------
# Dashboard endpoint integration test
# ---------------------------------------------------------------------------

class TestDashboardEndpoint:
    """Integration test for the ``GET /`` dashboard route."""

    async def test_dashboard_serves_html(self) -> None:
        from src.main import app

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            resp = await client.get("/")

        assert resp.status_code == 200
        assert "Log Pattern Alerting" in resp.text

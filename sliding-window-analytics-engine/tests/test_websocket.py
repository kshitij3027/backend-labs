"""Unit tests for :class:`src.websocket.ConnectionManager`.

These tests use a tiny in-process ``FakeWebSocket`` stand-in with async
``accept`` and ``send_json`` coroutines — no real FastAPI or ASGI
server is involved. They cover the three lifecycle concerns of the
connection manager:

1. Accepting and counting connections.
2. Fan-out broadcasting to every active client.
3. Evicting dead clients whose ``send_json`` raises.
4. Explicit disconnect removing a client from the active set.
"""

from __future__ import annotations

import pytest

from src.websocket import ConnectionManager


class FakeWebSocket:
    """Minimal async WebSocket stand-in for unit tests.

    Records every payload sent via :meth:`send_json`. If ``raise_on_send``
    is true, :meth:`send_json` raises a ``RuntimeError`` to simulate a
    dead client so the manager can exercise its eviction path.
    """

    def __init__(self, raise_on_send: bool = False) -> None:
        self.accepted = False
        self.sent: list[dict] = []
        self.raise_on_send = raise_on_send

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, payload: dict) -> None:
        if self.raise_on_send:
            raise RuntimeError("simulated client disconnect")
        self.sent.append(payload)

    async def close(self) -> None:  # pragma: no cover - kept for interface parity
        pass


@pytest.mark.asyncio
async def test_connect_and_count() -> None:
    """Connecting a single client should accept it and bump active_count."""
    manager = ConnectionManager()
    ws = FakeWebSocket()

    assert manager.active_count == 0
    await manager.connect(ws)

    assert ws.accepted is True
    assert manager.active_count == 1


@pytest.mark.asyncio
async def test_broadcast_to_multiple() -> None:
    """Broadcasting once should reach every connected client exactly once."""
    manager = ConnectionManager()
    clients = [FakeWebSocket() for _ in range(3)]
    for ws in clients:
        await manager.connect(ws)

    payload = {"type": "metrics_update", "hello": "world"}
    sent = await manager.broadcast_json(payload)

    assert sent == 3
    assert manager.active_count == 3
    for ws in clients:
        assert ws.sent == [payload]


@pytest.mark.asyncio
async def test_broadcast_evicts_dead_client() -> None:
    """A client that raises during send_json should be evicted."""
    manager = ConnectionManager()
    alive_a = FakeWebSocket()
    dead = FakeWebSocket(raise_on_send=True)
    alive_b = FakeWebSocket()
    for ws in (alive_a, dead, alive_b):
        await manager.connect(ws)

    assert manager.active_count == 3

    payload = {"type": "metrics_update", "value": 1}
    sent = await manager.broadcast_json(payload)

    # Only the two healthy clients received the payload.
    assert sent == 2
    assert alive_a.sent == [payload]
    assert alive_b.sent == [payload]
    assert dead.sent == []  # never recorded because send raised.

    # The dead client is gone; the healthy ones remain.
    assert manager.active_count == 2

    # A subsequent broadcast should still target both survivors.
    sent_again = await manager.broadcast_json({"type": "metrics_update", "value": 2})
    assert sent_again == 2


@pytest.mark.asyncio
async def test_disconnect_removes_client() -> None:
    """Explicit disconnect should drop the client from the active set."""
    manager = ConnectionManager()
    ws = FakeWebSocket()
    await manager.connect(ws)
    assert manager.active_count == 1

    await manager.disconnect(ws)
    assert manager.active_count == 0

    # Disconnecting an unknown client is a no-op (idempotent).
    await manager.disconnect(ws)
    assert manager.active_count == 0

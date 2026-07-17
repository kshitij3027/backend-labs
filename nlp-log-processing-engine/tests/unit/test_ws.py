"""Unit tests for the C9 :class:`~src.ws.ConnectionManager` — the ``/ws`` fan-out registry.

Exercised in isolation against lightweight fake socket objects (no real network / ASGI). Each
async manager method is driven synchronously via ``asyncio.run(...)`` so these tests need no
event-loop plugin or marker and read straight down the page. They pin the manager's contract:

* ``connect`` accepts the handshake and registers the socket (``count`` increments);
* ``disconnect`` is idempotent — discarding a socket twice, or one that was never added, is a
  no-op rather than a ``KeyError``;
* ``broadcast`` delivers the same dict to every live socket;
* a socket whose ``send_json`` raises is *pruned* mid-broadcast and does not stop delivery to
  the healthy sockets (one dead client can't break the rest);
* broadcasting with no clients connected is a no-op.
"""

import asyncio

from src.ws import ConnectionManager


class FakeWebSocket:
    """A minimal stand-in for a Starlette ``WebSocket`` that records interactions."""

    def __init__(self) -> None:
        self.accepted = False
        self.sent: list[dict] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, message: dict) -> None:
        self.sent.append(message)


class RaisingWebSocket(FakeWebSocket):
    """A fake whose ``send_json`` always fails — used to prove dead-socket pruning."""

    async def send_json(self, message: dict) -> None:
        raise RuntimeError("dead socket")


# --- connect ----------------------------------------------------------------------


def test_connect_accepts_and_registers():
    manager = ConnectionManager()
    ws = FakeWebSocket()

    asyncio.run(manager.connect(ws))

    assert ws.accepted is True  # handshake accepted...
    assert manager.count == 1  # ...and the socket is now a live client.


def test_connect_registers_multiple_distinct_clients():
    manager = ConnectionManager()
    first, second = FakeWebSocket(), FakeWebSocket()

    asyncio.run(manager.connect(first))
    asyncio.run(manager.connect(second))

    assert manager.count == 2


# --- disconnect (idempotent) ------------------------------------------------------


def test_disconnect_removes_client():
    manager = ConnectionManager()
    ws = FakeWebSocket()
    asyncio.run(manager.connect(ws))
    assert manager.count == 1

    manager.disconnect(ws)
    assert manager.count == 0


def test_disconnect_is_idempotent():
    manager = ConnectionManager()
    ws = FakeWebSocket()
    asyncio.run(manager.connect(ws))

    manager.disconnect(ws)
    manager.disconnect(ws)  # second call is a no-op, never a KeyError
    assert manager.count == 0


def test_disconnect_unknown_socket_is_a_noop():
    manager = ConnectionManager()
    # Discarding a socket that was never connected must not raise.
    manager.disconnect(FakeWebSocket())
    assert manager.count == 0


# --- broadcast --------------------------------------------------------------------


def test_broadcast_delivers_to_all_live_sockets():
    manager = ConnectionManager()
    clients = [FakeWebSocket() for _ in range(3)]
    for ws in clients:
        asyncio.run(manager.connect(ws))

    message = {"type": "analysis", "data": {"message": "hello"}}
    asyncio.run(manager.broadcast(message))

    # Every live client received exactly the one frame.
    for ws in clients:
        assert ws.sent == [message]
    assert manager.count == 3  # all still live


def test_broadcast_empty_is_a_noop():
    manager = ConnectionManager()
    # No clients connected -> nothing sent, no error, count stays zero.
    asyncio.run(manager.broadcast({"type": "stats", "data": {}}))
    assert manager.count == 0


def test_broadcast_prunes_failed_socket_without_blocking_others():
    manager = ConnectionManager()
    good_a = FakeWebSocket()
    dead = RaisingWebSocket()
    good_b = FakeWebSocket()
    for ws in (good_a, dead, good_b):
        asyncio.run(manager.connect(ws))
    assert manager.count == 3

    message = {"type": "analysis", "data": {"message": "hi"}}
    asyncio.run(manager.broadcast(message))

    # The failing socket did not stop delivery to the healthy ones...
    assert good_a.sent == [message]
    assert good_b.sent == [message]
    # ...and it was pruned, leaving only the two live clients.
    assert manager.count == 2


def test_broadcast_survives_all_sockets_failing():
    manager = ConnectionManager()
    dead_a, dead_b = RaisingWebSocket(), RaisingWebSocket()
    asyncio.run(manager.connect(dead_a))
    asyncio.run(manager.connect(dead_b))

    # Even when every send fails, broadcast returns cleanly and prunes them all.
    asyncio.run(manager.broadcast({"type": "analysis", "data": {}}))

    assert manager.count == 0


def test_broadcast_after_pruning_reaches_survivors():
    manager = ConnectionManager()
    good = FakeWebSocket()
    dead = RaisingWebSocket()
    asyncio.run(manager.connect(good))
    asyncio.run(manager.connect(dead))

    first = {"type": "analysis", "data": {"n": 1}}
    asyncio.run(manager.broadcast(first))  # prunes `dead`
    second = {"type": "stats", "data": {"n": 2}}
    asyncio.run(manager.broadcast(second))  # only `good` remains

    assert good.sent == [first, second]
    assert manager.count == 1

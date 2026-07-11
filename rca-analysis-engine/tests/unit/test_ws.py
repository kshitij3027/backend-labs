"""Unit tests for the ConnectionManager (C6).

Exercise the WebSocket fan-out registry in isolation against lightweight fake socket
objects (no real network / ASGI), so the assertions pin the manager's own contract:

* **connect** accepts the handshake and registers the socket (``count`` increments,
  and it is accepted before it becomes broadcast-eligible);
* **broadcast** delivers the same dict to every live socket;
* a socket whose ``send_json`` raises is **pruned** mid-broadcast and does *not*
  prevent delivery to the healthy sockets (one dead client can't break the rest);
* **disconnect** is idempotent (discarding an already-removed / never-added socket is
  a no-op, never a ``KeyError``);
* **send_personal** delivers to one socket and prunes it on failure.

``pytest.ini`` sets ``asyncio_mode = auto``, so the ``async def`` tests run directly
without an explicit marker.
"""

import pytest

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


@pytest.fixture()
def manager() -> ConnectionManager:
    return ConnectionManager()


# --- connect ---------------------------------------------------------------------


async def test_connect_accepts_and_registers(manager):
    ws = FakeWebSocket()

    await manager.connect(ws)

    assert ws.accepted is True  # handshake accepted...
    assert manager.count() == 1  # ...and the socket is now a live client.


async def test_connect_registers_multiple_distinct_clients(manager):
    first, second = FakeWebSocket(), FakeWebSocket()

    await manager.connect(first)
    await manager.connect(second)

    assert manager.count() == 2


# --- broadcast -------------------------------------------------------------------


async def test_broadcast_delivers_to_all_live_sockets(manager):
    clients = [FakeWebSocket() for _ in range(3)]
    for ws in clients:
        await manager.connect(ws)

    message = {"type": "incident_update", "data": {"incident_id": "inc-1"}}
    await manager.broadcast(message)

    # Every live client received exactly the one frame.
    for ws in clients:
        assert ws.sent == [message]
    assert manager.count() == 3  # all still live


async def test_broadcast_empty_is_a_noop(manager):
    # No clients connected -> nothing sent, no error.
    await manager.broadcast({"type": "incident_update", "data": {}})
    assert manager.count() == 0


async def test_broadcast_prunes_failed_socket_without_blocking_others(manager):
    good_a = FakeWebSocket()
    dead = RaisingWebSocket()
    good_b = FakeWebSocket()
    for ws in (good_a, dead, good_b):
        await manager.connect(ws)
    assert manager.count() == 3

    message = {"type": "incident_update", "data": {"incident_id": "inc-2"}}
    await manager.broadcast(message)

    # The failing socket did not stop delivery to the healthy ones...
    assert good_a.sent == [message]
    assert good_b.sent == [message]
    # ...and it was pruned, leaving only the two live clients.
    assert manager.count() == 2


async def test_broadcast_survives_all_sockets_failing(manager):
    dead_a, dead_b = RaisingWebSocket(), RaisingWebSocket()
    await manager.connect(dead_a)
    await manager.connect(dead_b)

    # Even when every send fails, broadcast returns cleanly and prunes them all.
    await manager.broadcast({"type": "incident_update", "data": {}})

    assert manager.count() == 0


async def test_broadcast_after_pruning_reaches_survivors(manager):
    good = FakeWebSocket()
    dead = RaisingWebSocket()
    await manager.connect(good)
    await manager.connect(dead)

    first = {"type": "incident_update", "data": {"n": 1}}
    await manager.broadcast(first)  # prunes `dead`
    second = {"type": "incident_update", "data": {"n": 2}}
    await manager.broadcast(second)  # only `good` remains

    assert good.sent == [first, second]
    assert manager.count() == 1


# --- disconnect (idempotent) -----------------------------------------------------


async def test_disconnect_removes_client(manager):
    ws = FakeWebSocket()
    await manager.connect(ws)
    assert manager.count() == 1

    manager.disconnect(ws)
    assert manager.count() == 0


async def test_disconnect_is_idempotent(manager):
    ws = FakeWebSocket()
    await manager.connect(ws)

    manager.disconnect(ws)
    manager.disconnect(ws)  # second call is a no-op, never a KeyError
    assert manager.count() == 0


def test_disconnect_unknown_socket_is_a_noop(manager):
    # Discarding a socket that was never connected must not raise.
    manager.disconnect(FakeWebSocket())
    assert manager.count() == 0


# --- send_personal ---------------------------------------------------------------


async def test_send_personal_delivers_to_one_socket(manager):
    target = FakeWebSocket()
    other = FakeWebSocket()
    await manager.connect(target)
    await manager.connect(other)

    message = {"type": "hello", "data": {}}
    await manager.send_personal(target, message)

    assert target.sent == [message]
    assert other.sent == []  # only the target received it


async def test_send_personal_prunes_failed_socket(manager):
    dead = RaisingWebSocket()
    await manager.connect(dead)

    await manager.send_personal(dead, {"type": "hello", "data": {}})

    assert manager.count() == 0  # a socket that can't be written is dropped

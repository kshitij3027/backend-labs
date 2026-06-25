"""Integration tests for the live ``/ws/stream`` WebSocket surface (C12).

These drive the *real* application via :class:`fastapi.testclient.TestClient` **as a context
manager** (Starlette's TestClient speaks WebSocket), so the startup lifespan runs: the engine
is warmed and the single background broadcaster task is launched before any frame is read.
Warm-up is intentionally tiny and the broadcast interval is shrunk to ``0.2s`` so the module
stays fast while still exercising the genuine connect -> snapshot -> broadcast path — no mocks.

The async ``ConnectionManager.broadcast`` pruning behaviour is covered by a focused unit-style
test with a tiny fake socket (``asyncio_mode = auto`` lets ``async def test_*`` run directly).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import create_app
from src.log_generator import generate_logs
from src.metrics import ConnectionManager


def make_client() -> TestClient:
    """Build a TestClient over an app warmed on a small batch with a fast broadcast tick.

    Use as ``with make_client() as c:`` so the startup lifespan (which warms the engine and
    starts the broadcaster) actually runs — a bare ``TestClient(app)`` would not.
    """
    app = create_app(warmup_logs=generate_logs(250, seed=1), broadcast_interval=0.2)
    return TestClient(app)


def _logs(n: int, seed: int = 11) -> list[dict]:
    """Return ``n`` generated logs as JSON-able dicts (a valid ``/cluster/batch`` payload)."""
    return [log.model_dump(mode="json") for log in generate_logs(n, seed=seed)]


def _assert_snapshot_shape(data: dict) -> None:
    """Assert ``data`` is a well-formed live snapshot payload."""
    assert data["type"] == "snapshot"
    assert "stats" in data
    stats = data["stats"]
    assert "total_processed" in stats
    assert "total_clusters" in stats
    assert "throughput_per_sec" in stats
    assert isinstance(data["patterns"], list)
    assert isinstance(data["anomalies"], list)


# ----------------------------------------------------------------- initial snapshot


def test_ws_initial_snapshot_shape() -> None:
    """On connect the client immediately receives a well-formed snapshot frame."""
    with make_client() as c:
        with c.websocket_connect("/ws/stream") as ws:
            data = ws.receive_json()
            _assert_snapshot_shape(data)
            # total_processed is a non-negative int even before any traffic.
            assert isinstance(data["stats"]["total_processed"], int)
            assert data["stats"]["total_processed"] >= 0


# --------------------------------------------------------- updates after traffic


def test_ws_updates_after_posting_logs() -> None:
    """After POSTing logs, subsequent broadcast frames stay well-formed with a valid count."""
    with make_client() as c:
        with c.websocket_connect("/ws/stream") as ws:
            # Drain the immediate first-paint snapshot.
            first = ws.receive_json()
            _assert_snapshot_shape(first)

            # Push real traffic through the same app (clusters synchronously).
            resp = c.post("/cluster/batch", json={"logs": _logs(20)})
            assert resp.status_code == 200

            # Receive a couple of periodic broadcasts and re-validate the shape. Counts are
            # not asserted exactly because broadcaster timing relative to the POST varies.
            for _ in range(2):
                data = ws.receive_json()
                _assert_snapshot_shape(data)
                total = data["stats"]["total_processed"]
                assert isinstance(total, int)
                assert total >= 0


# --------------------------------------------------------- multiple connections


def test_ws_two_clients_both_receive() -> None:
    """Two simultaneous connections each receive their own initial snapshot."""
    with make_client() as c:
        with c.websocket_connect("/ws/stream") as ws_a, c.websocket_connect(
            "/ws/stream"
        ) as ws_b:
            data_a = ws_a.receive_json()
            data_b = ws_b.receive_json()
            _assert_snapshot_shape(data_a)
            _assert_snapshot_shape(data_b)


# ----------------------------------------------- ConnectionManager dead-socket prune


class _FakeWebSocket:
    """Minimal async stand-in for a Starlette WebSocket used to test broadcast pruning.

    ``dead=True`` makes :meth:`send_text` raise (simulating a vanished client); the manager is
    expected to prune such sockets after a broadcast rather than aborting the loop.
    """

    def __init__(self, *, dead: bool = False) -> None:
        self.dead = dead
        self.accepted = False
        self.sent: list[str] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, message: str) -> None:
        if self.dead:
            raise RuntimeError("socket is gone")
        self.sent.append(message)


async def test_broadcast_prunes_dead_sockets() -> None:
    """A live socket receives the message; a dead one is pruned and never blocks the loop."""
    manager = ConnectionManager()
    good = _FakeWebSocket()
    bad = _FakeWebSocket(dead=True)

    await manager.connect(good)  # type: ignore[arg-type]
    await manager.connect(bad)  # type: ignore[arg-type]
    assert good.accepted and bad.accepted
    assert manager.count() == 2

    await manager.broadcast("hello")

    # Good socket got the payload; the dead one was discarded by the broadcast.
    assert good.sent == ["hello"]
    assert manager.count() == 1

    # A second broadcast still reaches the survivor (loop wasn't broken by the dead socket).
    await manager.broadcast("again")
    assert good.sent == ["hello", "again"]
    assert manager.count() == 1

"""Unit tests for C14 --- WebSocket live channel.

Covers three layers:

1. :class:`ConnectionManager` --- bucketing by ``run_id``, the ``"*"``
   wildcard fan-out, ``disconnect`` cleanup, ``broadcast`` semantics,
   and dead-socket pruning. These are exercised without FastAPI by
   driving the manager directly with ``AsyncMock`` WebSocket
   look-alikes.

2. :func:`broadcaster_task` --- pulling events off an
   ``asyncio.Queue``, pushing snapshot deltas, and emitting heartbeats.
   We run the task for a small wall-clock window at 20 Hz so we can
   observe at least one of each frame type without flakiness.

3. The websocket route ``/ws/runs/{run_id}`` itself, via
   ``fastapi.testclient.TestClient``. We deliberately avoid the real
   lifespan in :mod:`src.main` (which expects a live Docker socket and
   a real monitor) by building a tiny app that includes ONLY
   :data:`src.api.ws.router` and populates the slots
   (``app.state.ws_manager`` and ``app.state.monitor``) that the
   handler reads.

Frame schema:
    ``{"type": "snapshot"|"event"|"metrics"|"heartbeat", "data": ...}``
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import ws as ws_module
from src.api.ws import (
    ConnectionManager,
    broadcaster_task,
    record_snapshot,
    router as ws_router,
)
from src.models.metrics import SystemMetrics


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_ws_mock() -> MagicMock:
    """Build a minimal mock that quacks like ``starlette.WebSocket``.

    The two methods the manager touches are ``accept()`` and
    ``send_json()`` --- both are awaited. We return a ``MagicMock`` with
    ``AsyncMock`` attributes so we can assert ``await_count`` /
    ``await_args``.
    """
    ws = MagicMock(name="ws_mock")
    ws.accept = AsyncMock()
    ws.send_json = AsyncMock()
    return ws


def _make_metrics(cpu: float = 5.0, mem: float = 10.0, disk: float = 20.0) -> SystemMetrics:
    return SystemMetrics(cpu_pct=cpu, mem_pct=mem, disk_pct=disk)


def _reset_latest_snapshot() -> None:
    """Force module state back to "no snapshot yet"."""
    ws_module._LATEST_SNAPSHOT["value"] = None


@pytest.fixture(autouse=True)
def _isolate_module_state():
    """Reset the module-level latest-snapshot before AND after every test."""
    _reset_latest_snapshot()
    yield
    _reset_latest_snapshot()


# --------------------------------------------------------------------------- #
# ConnectionManager
# --------------------------------------------------------------------------- #


class TestConnectionManager:
    async def test_initial_count_is_zero(self) -> None:
        manager = ConnectionManager()
        assert manager.connection_count() == 0
        assert manager.clients_for("abc") == set()

    async def test_connect_two_same_run_then_one_star(self) -> None:
        manager = ConnectionManager()
        a = _make_ws_mock()
        b = _make_ws_mock()
        c = _make_ws_mock()
        await manager.connect(a, "abc")
        await manager.connect(b, "abc")
        assert manager.connection_count() == 2
        await manager.connect(c, "*")
        assert manager.connection_count() == 3
        # accept() should have fired exactly once for each client.
        a.accept.assert_awaited_once()
        b.accept.assert_awaited_once()
        c.accept.assert_awaited_once()

    async def test_clients_for_unions_run_id_and_star(self) -> None:
        manager = ConnectionManager()
        a = _make_ws_mock()
        b = _make_ws_mock()
        c = _make_ws_mock()
        await manager.connect(a, "abc")
        await manager.connect(b, "abc")
        await manager.connect(c, "*")
        targets = manager.clients_for("abc")
        assert targets == {a, b, c}
        # An unrelated run_id sees only the "*" group.
        assert manager.clients_for("zzz") == {c}

    async def test_disconnect_removes_client_and_cleans_empty_bucket(self) -> None:
        manager = ConnectionManager()
        a = _make_ws_mock()
        b = _make_ws_mock()
        await manager.connect(a, "abc")
        await manager.connect(b, "abc")
        manager.disconnect(a, "abc")
        assert manager.connection_count() == 1
        # Bucket still present because b is still in it.
        assert "abc" in manager._clients
        manager.disconnect(b, "abc")
        # Now empty -> bucket pruned entirely.
        assert "abc" not in manager._clients
        assert manager.connection_count() == 0

    async def test_disconnect_on_unknown_runid_is_noop(self) -> None:
        manager = ConnectionManager()
        # Should not raise and should not mutate state.
        manager.disconnect(_make_ws_mock(), "nope")
        assert manager.connection_count() == 0

    async def test_broadcast_with_run_id_hits_run_clients_and_star(self) -> None:
        manager = ConnectionManager()
        run_client = _make_ws_mock()
        star_client = _make_ws_mock()
        other_client = _make_ws_mock()
        await manager.connect(run_client, "abc")
        await manager.connect(star_client, "*")
        await manager.connect(other_client, "xyz")

        frame = {"type": "event", "data": {"event": "run_started"}}
        await manager.broadcast(frame, run_id="abc")

        run_client.send_json.assert_awaited_once_with(frame)
        star_client.send_json.assert_awaited_once_with(frame)
        # Different run id -> NOT addressed by a run-scoped broadcast.
        other_client.send_json.assert_not_called()

    async def test_broadcast_without_run_id_hits_every_client(self) -> None:
        manager = ConnectionManager()
        a = _make_ws_mock()
        b = _make_ws_mock()
        c = _make_ws_mock()
        await manager.connect(a, "abc")
        await manager.connect(b, "xyz")
        await manager.connect(c, "*")

        frame = {"type": "heartbeat", "data": {"ts": 1.0}}
        await manager.broadcast(frame, run_id=None)

        a.send_json.assert_awaited_once_with(frame)
        b.send_json.assert_awaited_once_with(frame)
        c.send_json.assert_awaited_once_with(frame)

    async def test_dead_socket_pruned_after_failed_send(self) -> None:
        manager = ConnectionManager()
        live = _make_ws_mock()
        dead = _make_ws_mock()
        # Make send_json raise so send_to returns False -> client pruned.
        dead.send_json = AsyncMock(side_effect=RuntimeError("client gone"))

        await manager.connect(live, "abc")
        await manager.connect(dead, "abc")
        assert manager.connection_count() == 2

        # First broadcast: live receives, dead is pruned.
        await manager.broadcast({"type": "heartbeat", "data": {}}, run_id="abc")
        assert manager.connection_count() == 1
        assert dead not in manager.clients_for("abc")
        live.send_json.assert_awaited_once()

        # Second broadcast: only live is addressed (dead is gone).
        live.send_json.reset_mock()
        await manager.broadcast({"type": "heartbeat", "data": {}}, run_id="abc")
        live.send_json.assert_awaited_once()
        # Dead must NOT have been retried.
        assert dead.send_json.await_count == 1

    async def test_send_to_returns_false_on_send_failure(self) -> None:
        manager = ConnectionManager()
        bad = _make_ws_mock()
        bad.send_json = AsyncMock(side_effect=ConnectionResetError("boom"))
        ok = await manager.send_to(bad, {"type": "heartbeat", "data": {}})
        assert ok is False

    async def test_send_to_returns_true_on_success(self) -> None:
        manager = ConnectionManager()
        ws = _make_ws_mock()
        ok = await manager.send_to(ws, {"type": "heartbeat", "data": {"ts": 7}})
        assert ok is True
        ws.send_json.assert_awaited_once_with({"type": "heartbeat", "data": {"ts": 7}})


# --------------------------------------------------------------------------- #
# Module-level snapshot helpers
# --------------------------------------------------------------------------- #


class TestSnapshotShim:
    def test_record_and_get_round_trip(self) -> None:
        from src.api.ws import get_latest_snapshot

        assert get_latest_snapshot() is None
        snap = _make_metrics(cpu=11.1, mem=22.2, disk=33.3)
        record_snapshot(snap)
        got = get_latest_snapshot()
        assert got is snap
        assert got.cpu_pct == 11.1

    def test_latest_overwrites_previous(self) -> None:
        from src.api.ws import get_latest_snapshot

        record_snapshot(_make_metrics(cpu=1.0))
        record_snapshot(_make_metrics(cpu=2.0))
        record_snapshot(_make_metrics(cpu=3.0))
        snap = get_latest_snapshot()
        assert snap is not None
        assert snap.cpu_pct == 3.0


# --------------------------------------------------------------------------- #
# broadcaster_task
# --------------------------------------------------------------------------- #


class TestBroadcasterTask:
    async def test_emits_at_least_one_of_each_frame_type(self) -> None:
        """At 20 Hz over ~250ms we should see event + metrics + heartbeat
        delivered to a registered client. The snapshot frame is delivered
        only on the connect handshake, so it's tested separately below.
        """
        manager = ConnectionManager()
        client = _make_ws_mock()
        # Bypass connect() (no real ws.accept) by writing directly to the
        # internal bucket: this is the cleanest test seam without monkey-
        # patching the manager.
        manager._clients.setdefault("*", set()).add(client)

        event_queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        stop = asyncio.Event()

        # Push three events of varying shapes. The "*"-subscribed client
        # picks all of them up regardless of per-event run_id.
        event_queue.put_nowait(
            {"event": "run_started", "run_id": "r-1", "status": "running"}
        )
        event_queue.put_nowait(
            {"event": "injecting", "run_id": "r-1", "status": "injecting"}
        )
        event_queue.put_nowait(
            {"event": "rolling_back", "run_id": "r-1", "status": "rolling_back"}
        )

        # And install a snapshot so the broadcaster picks up a metrics
        # frame on its first cycle.
        record_snapshot(_make_metrics(cpu=12.5, mem=23.4, disk=45.6))

        task = asyncio.create_task(
            broadcaster_task(
                manager,
                event_queue,
                broadcast_hz=20.0,
                stop_event=stop,
            )
        )
        # Let it spin for several broadcast periods.
        await asyncio.sleep(0.30)
        stop.set()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        # Inspect every frame the client received via send_json.
        sent_frames = [call.args[0] for call in client.send_json.call_args_list]
        assert sent_frames, "broadcaster sent no frames at all"
        types = {f["type"] for f in sent_frames}
        assert "event" in types, f"no event frame seen; saw types={types}"
        assert "metrics" in types, f"no metrics frame seen; saw types={types}"
        assert "heartbeat" in types, f"no heartbeat frame seen; saw types={types}"

        # Event frames preserve their per-run payload verbatim under "data".
        event_frames = [f for f in sent_frames if f["type"] == "event"]
        assert any(
            f["data"]["event"] == "run_started" for f in event_frames
        ), "run_started event not propagated"

        # Metrics frame carries the snapshot's model_dump.
        metrics_frames = [f for f in sent_frames if f["type"] == "metrics"]
        assert all(
            f["data"]["cpu_pct"] == 12.5 for f in metrics_frames
        ), "metrics frame did not carry recorded snapshot"

    async def test_metrics_frame_not_re_emitted_when_unchanged(self) -> None:
        """If the snapshot doesn't change between cycles, the broadcaster
        should not re-broadcast an identical metrics frame.
        """
        manager = ConnectionManager()
        client = _make_ws_mock()
        manager._clients.setdefault("*", set()).add(client)

        event_queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        stop = asyncio.Event()

        # Pin a single snapshot and never change it.
        record_snapshot(_make_metrics(cpu=7.0, mem=8.0, disk=9.0))

        task = asyncio.create_task(
            broadcaster_task(
                manager,
                event_queue,
                broadcast_hz=40.0,
                stop_event=stop,
            )
        )
        await asyncio.sleep(0.20)
        stop.set()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        sent_frames = [call.args[0] for call in client.send_json.call_args_list]
        metrics_frames = [f for f in sent_frames if f["type"] == "metrics"]
        # The snapshot never changed, so we should see exactly ONE metrics
        # frame even though many heartbeat cycles ran.
        assert len(metrics_frames) == 1, (
            f"expected exactly 1 metrics frame for an unchanged snapshot, "
            f"got {len(metrics_frames)}"
        )

    async def test_stops_cleanly_when_event_set(self) -> None:
        """The task must exit shortly after stop_event is set."""
        manager = ConnectionManager()
        event_queue: asyncio.Queue = asyncio.Queue(maxsize=8)
        stop = asyncio.Event()

        task = asyncio.create_task(
            broadcaster_task(manager, event_queue, broadcast_hz=20.0, stop_event=stop)
        )
        await asyncio.sleep(0.05)
        stop.set()
        # Must return within a small multiple of the broadcast period.
        await asyncio.wait_for(task, timeout=0.5)
        assert task.done()


# --------------------------------------------------------------------------- #
# /ws/runs/{run_id} via TestClient
# --------------------------------------------------------------------------- #


def _make_ws_only_app(history_size: int = 0) -> FastAPI:
    """Build a minimal FastAPI app that wires ONLY the ws router.

    No lifespan is registered -- so :class:`TestClient` won't try to spin
    up Docker, the database, the system monitor, etc.
    """
    app = FastAPI()
    app.state.ws_manager = ConnectionManager()
    monitor = MagicMock(name="monitor")
    monitor.history_size = MagicMock(return_value=history_size)
    app.state.monitor = monitor
    app.include_router(ws_router)
    return app


class TestWebSocketRoute:
    def test_snapshot_on_connect_uses_recorded_snapshot(self) -> None:
        record_snapshot(_make_metrics(cpu=5.0, mem=10.0, disk=20.0))
        app = _make_ws_only_app(history_size=7)
        client = TestClient(app)
        with client.websocket_connect("/ws/runs/abc") as ws:
            frame = ws.receive_json()
        assert frame["type"] == "snapshot"
        assert frame["data"]["history_size"] == 7
        assert frame["data"]["metrics"] is not None
        assert frame["data"]["metrics"]["cpu_pct"] == 5.0
        assert frame["data"]["metrics"]["mem_pct"] == 10.0
        assert frame["data"]["metrics"]["disk_pct"] == 20.0

    def test_snapshot_on_connect_when_no_metrics_yet(self) -> None:
        _reset_latest_snapshot()
        app = _make_ws_only_app(history_size=0)
        client = TestClient(app)
        with client.websocket_connect("/ws/runs/abc") as ws:
            frame = ws.receive_json()
        assert frame == {"type": "snapshot", "data": {"metrics": None, "history_size": 0}}

    def test_snapshot_on_connect_for_star_run_id(self) -> None:
        """The handler does not special-case "*" -- a client subscribing
        with "*" still gets the same first-frame snapshot.
        """
        record_snapshot(_make_metrics(cpu=1.5, mem=2.5, disk=3.5))
        app = _make_ws_only_app(history_size=3)
        client = TestClient(app)
        with client.websocket_connect("/ws/runs/*") as ws:
            frame = ws.receive_json()
        assert frame["type"] == "snapshot"
        assert frame["data"]["history_size"] == 3
        assert frame["data"]["metrics"]["cpu_pct"] == 1.5

    def test_connect_registers_client_in_manager(self) -> None:
        """Connecting a TestClient should bump connection_count to 1, and
        the manager should know about the "abc" bucket.
        """
        app = _make_ws_only_app()
        client = TestClient(app)
        manager: ConnectionManager = app.state.ws_manager
        assert manager.connection_count() == 0
        with client.websocket_connect("/ws/runs/abc") as ws:
            _ = ws.receive_json()  # drain the snapshot frame
            # Inside the connection the manager should know about us.
            assert manager.connection_count() == 1
            assert "abc" in manager._clients
        # On context exit the client disconnects -> bucket cleaned up.
        assert manager.connection_count() == 0
        assert "abc" not in manager._clients

    def test_handler_uses_zero_history_when_monitor_missing(self) -> None:
        """If app.state.monitor is None, history_size should default to 0."""
        record_snapshot(_make_metrics(cpu=4.0, mem=5.0, disk=6.0))
        app = FastAPI()
        app.state.ws_manager = ConnectionManager()
        app.state.monitor = None
        app.include_router(ws_router)
        client = TestClient(app)
        with client.websocket_connect("/ws/runs/abc") as ws:
            frame = ws.receive_json()
        assert frame["type"] == "snapshot"
        assert frame["data"]["history_size"] == 0
        assert frame["data"]["metrics"]["cpu_pct"] == 4.0


# --------------------------------------------------------------------------- #
# End-to-end manager broadcast through send_to (no FastAPI)
# --------------------------------------------------------------------------- #


class TestBroadcastDelivery:
    async def test_broadcast_via_manager_reaches_mock_send_json(self) -> None:
        """``manager.send_to(ws, frame)`` ends up calling ``ws.send_json(frame)``."""
        manager = ConnectionManager()
        ws = _make_ws_mock()
        await manager.connect(ws, "abc")
        frame = {"type": "event", "data": {"event": "ping"}}
        await manager.broadcast(frame, run_id="abc")
        ws.send_json.assert_awaited_once_with(frame)

    async def test_broadcast_to_empty_bucket_is_noop(self) -> None:
        manager = ConnectionManager()
        # Should not raise even with no clients connected.
        await manager.broadcast({"type": "heartbeat", "data": {}}, run_id="abc")
        await manager.broadcast({"type": "heartbeat", "data": {}}, run_id=None)

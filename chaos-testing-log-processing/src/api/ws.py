"""WebSocket live channel: engine events + monitor snapshots.

The shape of every outbound frame is a JSON object with two top-level
keys:

* ``type`` — one of ``"snapshot"``, ``"event"``, ``"metrics"``, ``"heartbeat"``
* ``data`` — the per-type payload

A single :class:`ConnectionManager` owns the set of connected sockets.
The :func:`broadcaster_task` coroutine, started in the FastAPI lifespan,
pulls frames from the engine's ``asyncio.Queue`` and from the latest
monitor snapshot, and dispatches them to all connected clients at
``broadcast_hz`` cadence (default 4 Hz / every 250ms).

On send failure (client gone), the manager prunes that socket without
disturbing other broadcasts. Snapshot-on-connect is sent synchronously
when a new client joins.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..models.metrics import SystemMetrics

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Tracks active WebSocket connections grouped by run_id ("*" = all)."""

    def __init__(self) -> None:
        self._clients: dict[str, set[WebSocket]] = {}

    def connection_count(self) -> int:
        return sum(len(s) for s in self._clients.values())

    def clients_for(self, run_id: str) -> set[WebSocket]:
        """Return the union of clients listening to this run_id plus the "*" group."""
        return self._clients.get(run_id, set()) | self._clients.get("*", set())

    async def connect(self, ws: WebSocket, run_id: str) -> None:
        await ws.accept()
        self._clients.setdefault(run_id, set()).add(ws)
        logger.info("ws connected: run_id=%s total=%d", run_id, self.connection_count())

    def disconnect(self, ws: WebSocket, run_id: str) -> None:
        bucket = self._clients.get(run_id)
        if bucket is not None:
            bucket.discard(ws)
            if not bucket:
                self._clients.pop(run_id, None)
        logger.info("ws disconnected: run_id=%s total=%d", run_id, self.connection_count())

    async def send_to(self, ws: WebSocket, frame: dict[str, Any]) -> bool:
        """Send a single frame; return False (and let caller prune) on failure."""
        try:
            await ws.send_json(frame)
            return True
        except Exception:
            return False

    async def broadcast(self, frame: dict[str, Any], run_id: str | None = None) -> None:
        """Send a frame to every client listening to ``run_id`` (or all clients if None)."""
        targets: Iterable[WebSocket]
        if run_id is None:
            targets = {ws for group in self._clients.values() for ws in group}
        else:
            targets = self.clients_for(run_id)
        dead: list[tuple[WebSocket, str]] = []
        for ws in list(targets):
            ok = await self.send_to(ws, frame)
            if not ok:
                # Find the bucket key for pruning.
                for key, group in self._clients.items():
                    if ws in group:
                        dead.append((ws, key))
                        break
        for ws, key in dead:
            self.disconnect(ws, key)


# A shim the SystemMonitor's add_listener accepts — captures the latest
# snapshot in module state so the broadcaster can read it without polling
# psutil itself. Mutating singleton, OK for a single-worker uvicorn.
_LATEST_SNAPSHOT: dict[str, SystemMetrics | None] = {"value": None}


def record_snapshot(snap: SystemMetrics) -> None:
    _LATEST_SNAPSHOT["value"] = snap


def get_latest_snapshot() -> SystemMetrics | None:
    return _LATEST_SNAPSHOT["value"]


async def broadcaster_task(
    manager: ConnectionManager,
    event_queue: asyncio.Queue,
    broadcast_hz: float = 4.0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Pull engine events + latest metrics; broadcast at the configured cadence."""
    period_s = 1.0 / max(broadcast_hz, 0.1)
    last_metrics_payload: dict | None = None
    stop = stop_event or asyncio.Event()

    while not stop.is_set():
        # 1. Drain whatever engine events are queued right now (non-blocking).
        while not event_queue.empty():
            event = event_queue.get_nowait()
            run_id = event.get("run_id")
            await manager.broadcast({"type": "event", "data": event}, run_id=run_id)

        # 2. If the latest metrics frame has changed, push it.
        snap = get_latest_snapshot()
        if snap is not None:
            payload = snap.model_dump(mode="json")
            if payload != last_metrics_payload:
                last_metrics_payload = payload
                await manager.broadcast({"type": "metrics", "data": payload})

        # 3. Heartbeat: helps tests that count frames see steady traffic.
        await manager.broadcast({"type": "heartbeat", "data": {"ts": asyncio.get_event_loop().time()}})

        try:
            await asyncio.wait_for(stop.wait(), timeout=period_s)
        except asyncio.TimeoutError:
            pass


router = APIRouter(tags=["ws"])


@router.websocket("/ws/runs/{run_id}")
async def ws_run_channel(websocket: WebSocket, run_id: str) -> None:
    manager: ConnectionManager = websocket.app.state.ws_manager
    await manager.connect(websocket, run_id)

    # Snapshot on connect.
    snap = get_latest_snapshot()
    snap_data = snap.model_dump(mode="json") if snap is not None else None
    monitor = getattr(websocket.app.state, "monitor", None)
    history_size = monitor.history_size() if monitor is not None else 0

    # Late-connect backfill: when this WS is scoped to a specific run id
    # (not the "*" wildcard), include the current ExperimentRun state in
    # the snapshot frame so the dashboard can populate its Recovery
    # report panel immediately — even if it connected after the engine
    # already emitted run_completed.
    run_data = None
    if run_id != "*":
        run_manager = getattr(websocket.app.state, "run_manager", None)
        if run_manager is not None:
            try:
                run_obj = run_manager.get_run(run_id)
            except Exception:
                run_obj = None
            if run_obj is not None:
                try:
                    run_data = run_obj.model_dump(mode="json")
                except Exception:
                    run_data = None

    await manager.send_to(
        websocket,
        {
            "type": "snapshot",
            "data": {
                "metrics": snap_data,
                "history_size": history_size,
                "run": run_data,
            },
        },
    )

    try:
        # We never read messages from the client; we just hold the connection
        # open until they disconnect.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket, run_id)

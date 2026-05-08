"""WebSocket broadcaster for live metrics."""
from __future__ import annotations
import asyncio
import logging
import os
import time
from contextlib import suppress

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Tracks active WebSocket connections and broadcasts to all of them."""

    def __init__(self) -> None:
        self.active: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self.active.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self.active.discard(ws)

    async def broadcast(self, payload: dict) -> None:
        async with self._lock:
            websockets = list(self.active)
        dead: list[WebSocket] = []
        for ws in websockets:
            try:
                await ws.send_json(payload)
            except Exception as exc:
                logger.debug("dropping ws on send failure: %s", exc)
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self.active.discard(ws)

    def count(self) -> int:
        return len(self.active)


def build_metrics_snapshot(registry, processor) -> dict:
    """Build the snapshot dict used by both /api/metrics and the broadcaster."""
    return {
        "circuits": registry.metrics_snapshot(),
        "processing": processor.get_processing_stats(),
        "generated_at": time.time(),
    }


async def metrics_broadcaster(app, interval: float = 2.0) -> None:
    """Background task: pushes a metrics snapshot to all WebSocket clients every ``interval`` seconds."""
    manager: ConnectionManager = app.state.manager
    history = app.state.history
    while True:
        try:
            snapshot = build_metrics_snapshot(app.state.registry, app.state.processor)
            history.append(snapshot)
            await manager.broadcast(snapshot)
        except Exception as exc:
            logger.warning("broadcaster iteration failed: %s", exc)
        await asyncio.sleep(interval)


def get_broadcast_interval() -> float:
    return float(os.getenv("WEBSOCKET_BROADCAST_INTERVAL", "2.0"))

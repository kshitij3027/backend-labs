"""WebSocket connection manager + broadcast loop for the live dashboard.

The :class:`ConnectionManager` owns the set of currently-connected
WebSocket clients. The :func:`broadcast_loop` coroutine periodically
snapshots the :class:`WindowManager` and fan-outs the serialised result
to every connected client.

Design notes:
- A single ``asyncio.Lock`` guards the active-clients set so concurrent
  ``connect``/``disconnect``/``broadcast`` calls don't race. The lock is
  released around the per-client ``send_json`` to avoid blocking the
  entire set if one client is slow — we snapshot the set under the lock
  then iterate the snapshot outside it.
- Dead clients (any exception during ``send_json``) are evicted so a
  single disconnected client never blocks the broadcast loop.
- The broadcast loop honours a shared ``asyncio.Event`` (the same
  ``stop_event`` used to shut down the generator task) so shutdown is
  prompt — ``wait_for(stop_event.wait(), timeout=interval)`` doubles as
  the interval sleep *and* the shutdown signal.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Tracks active WebSocket clients and broadcasts messages to all of them.

    Evicts dead clients on send failure so a single disconnected client
    never blocks the broadcast loop.
    """

    def __init__(self) -> None:
        self._active: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        """Accept ``ws`` and add it to the active set."""
        await ws.accept()
        async with self._lock:
            self._active.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        """Remove ``ws`` from the active set (idempotent)."""
        async with self._lock:
            self._active.discard(ws)

    async def broadcast_json(self, payload: dict) -> int:
        """Send ``payload`` as JSON to every active client.

        Returns the number of successful sends. Any client whose
        ``send_json`` raises is evicted from the active set.
        """
        async with self._lock:
            clients = list(self._active)
        dead: list[WebSocket] = []
        sent = 0
        for ws in clients:
            try:
                await ws.send_json(payload)
                sent += 1
            except Exception as exc:
                logger.debug("broadcast send failed, evicting client: %s", exc)
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._active.discard(ws)
        return sent

    @property
    def active_count(self) -> int:
        """Current number of connected clients."""
        return len(self._active)


async def broadcast_loop(
    manager: ConnectionManager,
    window_manager,
    interval: float,
    stop_event: asyncio.Event,
) -> None:
    """Periodically snapshot ``window_manager`` and broadcast to all clients.

    The loop runs until ``stop_event`` is set. Each iteration:
    1. Snapshots the :class:`WindowManager` at the current wall clock.
    2. Serialises the nested ``{metric: {resolution: WindowResult}}``
       structure to plain dicts.
    3. Fans the payload out to every connected client via the
       :class:`ConnectionManager`.
    4. Sleeps up to ``interval`` seconds, waking early on ``stop_event``.

    Any exception is logged and the loop continues — a broken snapshot
    should never take the dashboard down permanently.
    """
    while not stop_event.is_set():
        try:
            snapshot = window_manager.snapshot_all(time.time())
            metrics = {
                metric: {
                    resolution: asdict(result)
                    for resolution, result in resolutions.items()
                }
                for metric, resolutions in snapshot.items()
            }
            payload = {
                "type": "metrics_update",
                "timestamp": time.time(),
                "active_windows": window_manager.active_count,
                "metrics": metrics,
            }
            await manager.broadcast_json(payload)
        except Exception:
            logger.exception("broadcast_loop error")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

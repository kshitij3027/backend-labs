"""WebSocket connection manager with subscription-based broadcasting.

The :class:`ConnectionManager` tracks connected clients and their stream
subscriptions (metrics, alerts, system).  Two coroutines —
:func:`heartbeat_loop` and :func:`broadcast_loop` — run as background
tasks to keep connections alive and push real-time metric data.

Design notes:
- An ``asyncio.Lock`` guards the ``_connections`` dict so concurrent
  connect/disconnect/broadcast calls don't race.
- Dead clients are collected during broadcast and evicted afterwards
  (never modify the dict during iteration).
- Both loops honour a shared ``asyncio.Event`` via
  ``asyncio.wait_for(stop_event.wait(), timeout=interval)`` so shutdown
  is prompt.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field

from fastapi import WebSocket

logger = logging.getLogger(__name__)

VALID_STREAMS = {"metrics", "alerts", "system"}


@dataclass
class WebSocketState:
    """Per-client state tracked by the :class:`ConnectionManager`."""

    ws: WebSocket
    client_id: str
    subscriptions: set = field(default_factory=set)
    last_pong: float = field(default_factory=time.time)


class ConnectionManager:
    """Manages WebSocket connections with per-client stream subscriptions."""

    def __init__(self) -> None:
        self._connections: dict[str, WebSocketState] = {}
        self._lock = asyncio.Lock()

    @property
    def active_count(self) -> int:
        return len(self._connections)

    @property
    def subscriptions_summary(self) -> dict:
        summary: dict[str, int] = {s: 0 for s in VALID_STREAMS}
        for state in self._connections.values():
            for s in state.subscriptions:
                summary[s] = summary.get(s, 0) + 1
        return summary

    async def connect(self, ws: WebSocket) -> str:
        """Accept *ws*, assign a client id, send the welcome message."""
        await ws.accept()
        client_id = str(uuid.uuid4())[:8]
        async with self._lock:
            self._connections[client_id] = WebSocketState(
                ws=ws, client_id=client_id,
            )
        logger.info(
            "WS client %s connected (total: %d)", client_id, self.active_count,
        )
        # Send welcome message
        await ws.send_json({
            "type": "connected",
            "client_id": client_id,
            "available_streams": sorted(VALID_STREAMS),
        })
        return client_id

    async def disconnect(self, client_id: str) -> None:
        """Remove *client_id* from the active set and close its socket."""
        async with self._lock:
            state = self._connections.pop(client_id, None)
        if state:
            try:
                await state.ws.close()
            except Exception:
                pass
            logger.info(
                "WS client %s disconnected (total: %d)",
                client_id,
                self.active_count,
            )

    async def subscribe(self, client_id: str, streams: list[str]) -> list[str]:
        """Subscribe *client_id* to *streams*.

        Returns the list of streams that were actually subscribed (invalid
        stream names are silently dropped).
        """
        async with self._lock:
            state = self._connections.get(client_id)
        if not state:
            return []
        valid = [s for s in streams if s in VALID_STREAMS]
        state.subscriptions.update(valid)
        return valid

    async def unsubscribe(self, client_id: str, streams: list[str]) -> None:
        """Remove *streams* from *client_id*'s subscriptions."""
        async with self._lock:
            state = self._connections.get(client_id)
        if state:
            state.subscriptions -= set(streams)

    async def broadcast(self, stream: str, payload: dict) -> None:
        """Send *payload* to every client subscribed to *stream*.

        Dead clients are collected and disconnected after iteration.
        """
        dead: list[str] = []
        async with self._lock:
            targets = [
                (cid, s)
                for cid, s in self._connections.items()
                if stream in s.subscriptions
            ]
        for client_id, state in targets:
            try:
                await state.ws.send_json({
                    "type": f"{stream}_update",
                    "data": payload,
                })
            except Exception:
                dead.append(client_id)
        for cid in dead:
            await self.disconnect(cid)

    async def broadcast_alert(self, payload: dict) -> None:
        """Convenience helper — broadcast *payload* to ``alerts`` subscribers."""
        await self.broadcast("alerts", payload)

    async def handle_pong(self, client_id: str) -> None:
        """Update the last-pong timestamp for *client_id*."""
        async with self._lock:
            state = self._connections.get(client_id)
        if state:
            state.last_pong = time.time()

    async def cleanup_stale(self, timeout_seconds: float = 90.0) -> None:
        """Disconnect clients that have not responded to a heartbeat."""
        now = time.time()
        stale: list[str] = []
        async with self._lock:
            for cid, state in self._connections.items():
                if now - state.last_pong > timeout_seconds:
                    stale.append(cid)
        for cid in stale:
            logger.warning("Cleaning up stale WS client %s", cid)
            await self.disconnect(cid)


# ------------------------------------------------------------------
# Background coroutines
# ------------------------------------------------------------------


async def heartbeat_loop(
    manager: ConnectionManager,
    interval: float,
    stop_event: asyncio.Event,
) -> None:
    """Send periodic pings to all connected clients.

    Also runs :meth:`ConnectionManager.cleanup_stale` each tick to evict
    clients that never replied with a ``pong``.
    """
    while not stop_event.is_set():
        if manager.active_count > 0:
            dead: list[str] = []
            for cid, state in list(manager._connections.items()):
                try:
                    await state.ws.send_json({
                        "type": "ping",
                        "timestamp": time.time(),
                    })
                except Exception:
                    dead.append(cid)
            for cid in dead:
                await manager.disconnect(cid)
            await manager.cleanup_stale()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def broadcast_loop(
    manager: ConnectionManager,
    storage,
    config,
    stop_event: asyncio.Event,
) -> None:
    """Periodically broadcast the latest metrics to ``metrics`` subscribers.

    Each tick queries Redis for all known services/metrics, computes a
    trend for each one, and fans the result out through the manager.
    """
    while not stop_event.is_set():
        if (
            manager.active_count > 0
            and manager.subscriptions_summary.get("metrics", 0) > 0
        ):
            try:
                # Import inside function to avoid circular imports.
                from src.engine.trends import calculate_trend

                services = await storage.get_services()
                all_summaries: list[dict] = []
                now = time.time()
                for svc in services:
                    metric_names = await storage.get_metric_names(svc)
                    for mname in metric_names:
                        points = await storage.get_metrics(
                            svc, mname, now - 300, now,
                        )
                        if points:
                            trend = calculate_trend(
                                points, config.trend_window_minutes,
                            )
                            all_summaries.append({
                                "service": svc,
                                "metric_name": mname,
                                "latest_value": points[-1].value,
                                "data_point_count": len(points),
                                "trend": trend,
                            })
                if all_summaries:
                    await manager.broadcast("metrics", {
                        "timestamp": now,
                        "metrics": all_summaries,
                    })
            except Exception as e:
                logger.error("Broadcast loop error: %s", e)
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=config.ws_broadcast_interval,
            )
        except asyncio.TimeoutError:
            pass

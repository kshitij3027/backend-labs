"""WebSocket fan-out + live-snapshot assembly for the dashboard (Commit 12).

This module holds the two pieces that turn the warmed :class:`~src.engine.ClusteringEngine`
into a *live* picture for the React dashboard (C14-C17):

1. :class:`ConnectionManager` — a minimal, thread-safe-ish WebSocket registry. It accepts
   connections, forgets them on disconnect, and :meth:`broadcast`-s a single pre-serialized
   JSON **string** to every live socket, tolerating sockets that died mid-flight (one dead
   client must never break the loop or take the others down with it). It is driven by exactly
   one coroutine — the periodic broadcaster task in :mod:`src.api` — so there is no concurrent
   send contention on a given socket; the internal :class:`asyncio.Lock` only guards mutation
   of the active set so a disconnect during a broadcast iteration is safe.

2. :func:`build_snapshot_payload` — assembles the JSON-serializable dict the dashboard
   receives. It reads the engine's stats / quality / patterns / anomalies and is deliberately
   **defensive**: each piece is wrapped so a transient error in one (e.g. a metric that is
   momentarily uncomputable) yields a partial payload rather than crashing the broadcaster.

The engine is the single source of truth — this module never re-runs any clustering work, it
only reads already-computed views. The broadcaster task, the ``/ws/stream`` route and the
periodic-refit scheduling all live in :mod:`src.api`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    from fastapi import WebSocket

    from src.engine import ClusteringEngine

logger = logging.getLogger(__name__)

#: Cap on patterns included in a live snapshot (the dashboard's pattern panel shows the top N).
_MAX_PATTERNS = 12
#: Cap on anomalies included in a live snapshot (recent-first alert feed).
_MAX_ANOMALIES = 20


class ConnectionManager:
    """Tracks live ``/ws/stream`` clients and fans a JSON string out to all of them.

    Kept intentionally tiny. The periodic broadcaster in :mod:`src.api` is the *only* coroutine
    that calls :meth:`broadcast`, so there is no concurrent-send contention on a given socket.
    :meth:`broadcast` iterates a snapshot copy of the active set and prunes any socket whose
    send fails, so a client that vanished between broadcasts can never break the loop for the
    others.

    Attributes:
        _active: The currently-connected :class:`fastapi.WebSocket` clients.
        _lock: Guards mutation of :attr:`_active` (connect/disconnect vs. broadcast iteration).
    """

    def __init__(self) -> None:
        """Start with no connected clients."""
        self._active: set["WebSocket"] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: "WebSocket") -> None:
        """Accept the handshake and register ``ws`` as an active client.

        Args:
            ws: The incoming :class:`fastapi.WebSocket` to accept and track.
        """
        await ws.accept()
        async with self._lock:
            self._active.add(ws)

    def disconnect(self, ws: "WebSocket") -> None:
        """Deregister ``ws`` if present (idempotent; never raises).

        Synchronous so it can be called from a ``finally:`` in the endpoint and from
        :meth:`broadcast`'s prune step without awaiting. ``set.discard`` is a no-op when the
        socket is already gone, so a double-disconnect is harmless.

        Args:
            ws: The socket to forget.
        """
        self._active.discard(ws)

    async def broadcast(self, message: str) -> None:
        """Send the JSON ``message`` to every live client, pruning dead sockets.

        Iterates a snapshot copy of the active set (taken under :attr:`_lock`) so removals
        during the loop are safe, and calls ``ws.send_text(message)`` on each. Any socket that
        raises is collected and discarded afterwards rather than aborting the broadcast — one
        dead client must not starve the rest.

        Args:
            message: A pre-serialized JSON string (the same bytes go to every client).
        """
        async with self._lock:
            targets = list(self._active)

        dead: list["WebSocket"] = []
        for ws in targets:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001 - any send failure means the socket is gone
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._active.discard(ws)

    def count(self) -> int:
        """Return the number of currently-connected clients."""
        return len(self._active)


def build_snapshot_payload(engine: "ClusteringEngine") -> dict[str, Any]:
    """Assemble the live snapshot dict broadcast to every dashboard.

    Reads the engine's already-computed stats / quality / patterns / anomalies (no clustering
    work is re-run here) into a JSON-serializable dict. Each piece is wrapped in its own
    ``try/except`` so a transient failure in one section degrades to an empty/typed default
    rather than crashing the broadcaster — a partial snapshot is always better than a dead loop.

    Args:
        engine: The warmed :class:`~src.engine.ClusteringEngine` to read from.

    Returns:
        A dict shaped as::

            {
              "type": "snapshot",
              "stats": {<StatsSnapshot fields>},   # {} on a transient read error
              "quality": {<metric>: <float|None>}, # {} on a transient read error
              "patterns": [ {<PatternRecord fields>}, ... ],  # up to 12
              "anomalies": [ {<AnomalyAlert fields>}, ... ],  # up to 20, newest first
            }
    """
    payload: dict[str, Any] = {"type": "snapshot"}

    try:
        payload["stats"] = engine.stats_snapshot().model_dump(mode="json")
    except Exception:  # noqa: BLE001 - partial snapshot beats a dead broadcaster
        logger.warning("build_snapshot_payload: stats unavailable", exc_info=True)
        payload["stats"] = {}

    try:
        payload["quality"] = engine.quality_metrics()
    except Exception:  # noqa: BLE001
        logger.warning("build_snapshot_payload: quality unavailable", exc_info=True)
        payload["quality"] = {}

    try:
        payload["patterns"] = [
            p.model_dump(mode="json") for p in engine.get_patterns()[:_MAX_PATTERNS]
        ]
    except Exception:  # noqa: BLE001
        logger.warning("build_snapshot_payload: patterns unavailable", exc_info=True)
        payload["patterns"] = []

    try:
        payload["anomalies"] = [
            a.model_dump(mode="json") for a in engine.get_anomalies(_MAX_ANOMALIES)
        ]
    except Exception:  # noqa: BLE001
        logger.warning("build_snapshot_payload: anomalies unavailable", exc_info=True)
        payload["anomalies"] = []

    return payload

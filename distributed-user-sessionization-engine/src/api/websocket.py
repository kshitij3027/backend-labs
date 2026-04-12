"""WebSocket connection manager and real-time broadcast loop."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages active WebSocket connections with thread-safe broadcast."""

    def __init__(self):
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)
        logger.info("WebSocket client connected (%d total)", len(self._clients))

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)
        logger.info("WebSocket client disconnected (%d total)", len(self._clients))

    async def broadcast_json(self, data: dict) -> None:
        """Send JSON to all connected clients, removing dead connections."""
        async with self._lock:
            dead = set()
            for ws in self._clients:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            self._clients -= dead

    @property
    def client_count(self) -> int:
        return len(self._clients)


async def broadcast_loop(
    manager: ConnectionManager,
    engine,
    interval: float,
    stop_event: asyncio.Event,
) -> None:
    """Push session metrics to all WebSocket clients at regular intervals."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        sessions = list(engine.active_sessions.values())

        # Duration distribution buckets
        duration_dist = {"0-30s": 0, "30-60s": 0, "1-5m": 0, "5-15m": 0, "15-30m": 0, "30m+": 0}
        device_breakdown = {}
        engagement_breakdown = {"bounce": 0, "low": 0, "moderate": 0, "high": 0}
        session_type_breakdown: dict[str, int] = {}
        funnel_counts = {"none": 0, "viewed": 0, "carted": 0, "purchased": 0}
        anomaly_dist = {"normal": 0, "suspicious": 0, "anomalous": 0}
        live_sessions = []

        total_duration = 0.0
        for s in sessions:
            dur = (s.last_event_time - s.start_time).total_seconds()
            total_duration += dur

            # Duration buckets
            if dur < 30:
                duration_dist["0-30s"] += 1
            elif dur < 60:
                duration_dist["30-60s"] += 1
            elif dur < 300:
                duration_dist["1-5m"] += 1
            elif dur < 900:
                duration_dist["5-15m"] += 1
            elif dur < 1800:
                duration_dist["15-30m"] += 1
            else:
                duration_dist["30m+"] += 1

            # Device
            device_breakdown[s.device_type] = device_breakdown.get(s.device_type, 0) + 1

            # Engagement
            eng = s.engagement
            if eng in engagement_breakdown:
                engagement_breakdown[eng] += 1

            # Live session data (top 10 by recency)
            live_sessions.append({
                "session_id": s.session_id[:8],
                "user_id": s.user_id,
                "duration": round(dur, 1),
                "event_count": s.event_count,
                "quality_score": s.quality_score,
                "state": s.state.value,
                "device_type": s.device_type,
                "anomaly_score": s.anomaly_score,
            })

            # Session type breakdown
            st = s.session_type
            session_type_breakdown[st] = session_type_breakdown.get(st, 0) + 1

            # Funnel stage counts
            fs = s.funnel_stage
            if fs in funnel_counts:
                funnel_counts[fs] += 1

            # Anomaly distribution
            score = s.anomaly_score
            if score <= 30:
                anomaly_dist["normal"] += 1
            elif score <= 60:
                anomaly_dist["suspicious"] += 1
            else:
                anomaly_dist["anomalous"] += 1

        # Sort by most recent activity, take top 10
        live_sessions.sort(key=lambda x: x["duration"], reverse=True)
        live_sessions = live_sessions[:10]

        # Compute funnel conversion rates as percentages
        total = max(len(sessions), 1)
        funnel_rates = {k: round((v / total) * 100, 1) for k, v in funnel_counts.items()}

        payload = {
            "type": "session_update",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "active_sessions": len(sessions),
            "avg_duration": round(total_duration / max(len(sessions), 1), 2),
            "total_events": engine.total_events,
            "duration_distribution": duration_dist,
            "device_breakdown": device_breakdown,
            "engagement_breakdown": engagement_breakdown,
            "live_sessions": live_sessions,
            "session_type_breakdown": session_type_breakdown,
            "funnel_conversion_rates": funnel_rates,
            "anomaly_distribution": anomaly_dist,
        }

        await manager.broadcast_json(payload)

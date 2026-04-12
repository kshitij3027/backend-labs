"""Analytics API endpoint with pre-computed cache."""
from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api")

# Pre-computed analytics cache
_analytics_cache: dict = {}


@router.get("/analytics")
async def get_analytics(request: Request):
    """Return aggregate session statistics."""
    engine = request.app.state.session_engine
    return _compute_analytics(engine)


def _compute_analytics(engine) -> dict:
    """Compute analytics from current session state."""
    sessions = list(engine.active_sessions.values())

    if not sessions:
        return {
            "active_sessions": 0,
            "avg_duration": 0.0,
            "device_breakdown": {},
            "engagement_distribution": {"bounce": 0, "low": 0, "moderate": 0, "high": 0},
            "total_events": engine.total_events,
        }

    total_duration = 0.0
    device_breakdown: dict[str, int] = {}
    engagement_dist = {"bounce": 0, "low": 0, "moderate": 0, "high": 0}

    for s in sessions:
        duration = (s.last_event_time - s.start_time).total_seconds()
        total_duration += duration
        device_breakdown[s.device_type] = device_breakdown.get(s.device_type, 0) + 1
        eng = s.engagement
        if eng in engagement_dist:
            engagement_dist[eng] += 1

    return {
        "active_sessions": len(sessions),
        "avg_duration": round(total_duration / len(sessions), 2),
        "device_breakdown": device_breakdown,
        "engagement_distribution": engagement_dist,
        "total_events": engine.total_events,
    }

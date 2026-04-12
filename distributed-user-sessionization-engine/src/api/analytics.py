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
            "session_type_breakdown": {},
            "funnel_conversion_rates": {"none": 0.0, "viewed": 0.0, "carted": 0.0, "purchased": 0.0},
            "anomaly_distribution": {"normal": 0, "suspicious": 0, "anomalous": 0},
        }

    total_duration = 0.0
    device_breakdown: dict[str, int] = {}
    engagement_dist = {"bounce": 0, "low": 0, "moderate": 0, "high": 0}
    session_type_counts: dict[str, int] = {}
    funnel_counts = {"none": 0, "viewed": 0, "carted": 0, "purchased": 0}
    anomaly_dist = {"normal": 0, "suspicious": 0, "anomalous": 0}

    for s in sessions:
        duration = (s.last_event_time - s.start_time).total_seconds()
        total_duration += duration
        device_breakdown[s.device_type] = device_breakdown.get(s.device_type, 0) + 1
        eng = s.engagement
        if eng in engagement_dist:
            engagement_dist[eng] += 1
        # Session type breakdown
        st = s.session_type
        session_type_counts[st] = session_type_counts.get(st, 0) + 1
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

    # Compute funnel conversion rates as percentages
    total = len(sessions)
    funnel_rates = {k: round((v / total) * 100, 1) for k, v in funnel_counts.items()}

    return {
        "active_sessions": total,
        "avg_duration": round(total_duration / total, 2),
        "device_breakdown": device_breakdown,
        "engagement_distribution": engagement_dist,
        "total_events": engine.total_events,
        "session_type_breakdown": session_type_counts,
        "funnel_conversion_rates": funnel_rates,
        "anomaly_distribution": anomaly_dist,
    }

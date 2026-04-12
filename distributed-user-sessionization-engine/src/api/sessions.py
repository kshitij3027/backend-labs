"""Session query API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from src.models import Session

router = APIRouter(prefix="/api")


@router.get("/sessions/{user_id}")
async def get_user_sessions(user_id: str, request: Request):
    """Get all sessions for a user (active + historical from Redis)."""
    engine = request.app.state.session_engine
    sessions = await engine.get_user_sessions(user_id)
    return [_session_summary(s) for s in sessions]


@router.get("/sessions/{user_id}/{session_id}")
async def get_session_detail(user_id: str, session_id: str, request: Request):
    """Get full detail for a specific session."""
    engine = request.app.state.session_engine
    sessions = await engine.get_user_sessions(user_id)
    for s in sessions:
        if s.session_id == session_id:
            return s.model_dump(mode="json")
    return JSONResponse({"error": "Session not found"}, status_code=404)


def _session_summary(session: Session) -> dict:
    return {
        "session_id": session.session_id,
        "user_id": session.user_id,
        "state": session.state.value,
        "start_time": session.start_time.isoformat(),
        "last_event_time": session.last_event_time.isoformat(),
        "event_count": session.event_count,
        "device_type": session.device_type,
        "quality_score": session.quality_score,
        "engagement": session.engagement,
    }

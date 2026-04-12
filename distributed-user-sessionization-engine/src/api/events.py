"""Event ingestion API endpoint."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from src.models import Event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


class EventResponse(BaseModel):
    success: bool
    session_id: str
    analysis: dict


class BatchEventResponse(BaseModel):
    success: bool
    processed: int


@router.post("/events")
async def ingest_event(request: Request):
    """Ingest a single event or a batch of events."""
    engine = request.app.state.session_engine
    body = await request.json()

    # Check if batch (list) or single event (dict)
    if isinstance(body, list):
        count = 0
        for item in body:
            try:
                event = Event(**item)
            except ValidationError as exc:
                return JSONResponse(
                    {"error": "Validation error", "detail": exc.errors()},
                    status_code=422,
                )
            await engine.enqueue_event(event)
            count += 1
        return BatchEventResponse(success=True, processed=count)
    else:
        try:
            event = Event(**body)
        except ValidationError as exc:
            return JSONResponse(
                {"error": "Validation error", "detail": exc.errors()},
                status_code=422,
            )
        session, analysis = await engine.enqueue_event(event)
        return EventResponse(
            success=True,
            session_id=session.session_id,
            analysis={
                "quality_score": analysis.quality_score,
                "engagement": analysis.engagement,
            },
        )

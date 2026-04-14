"""Log injection API for testing the alert pipeline."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.models import LogEntry
from src.schemas import LogEntryCreate

router = APIRouter()
logger = structlog.get_logger(__name__)


@router.post("/test/inject_log")
async def inject_log(
    body: LogEntryCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Inject a log entry and run it through the alert pipeline.

    Creates the log entry in the database, then passes it through
    pattern matching, rate limiting, correlation, and WebSocket broadcast.
    """
    # Create the log entry
    log_entry = LogEntry(
        message=body.message,
        level=body.level,
        source=body.source,
        metadata_=body.metadata,
    )
    db.add(log_entry)
    await db.commit()
    await db.refresh(log_entry)

    # Run through the alert pipeline
    pipeline = request.app.state.pipeline
    alerts = await pipeline.process(log_entry, db)

    logger.info(
        "log_injected",
        log_id=log_entry.id,
        patterns_matched=len(alerts),
    )

    return {
        "status": "processed",
        "log_id": log_entry.id,
        "patterns_matched": len(alerts),
        "alerts": [a.id for a in alerts],
    }

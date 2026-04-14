import logging

from fastapi import APIRouter, Request
from sqlalchemy import text

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
async def health_check(request: Request):
    """Check database and Redis connectivity."""
    db_status = "disconnected"
    redis_status = "disconnected"

    # Check database
    try:
        async_session = request.app.state.async_session
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as exc:
        logger.warning("Health check - database error: %s", exc)

    # Check Redis
    try:
        redis_client = request.app.state.redis
        await redis_client.ping()
        redis_status = "connected"
    except Exception as exc:
        logger.warning("Health check - redis error: %s", exc)

    overall = "healthy" if db_status == "connected" and redis_status == "connected" else "unhealthy"

    return {
        "status": overall,
        "database": db_status,
        "redis": redis_status,
    }

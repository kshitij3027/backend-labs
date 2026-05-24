"""GET /api/statistics — system-wide compliance + tracking aggregates."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
from src.services.stats_service import compute_statistics


router = APIRouter(prefix="/api", tags=["statistics"])


@router.get("/statistics")
async def get_statistics(session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    return await compute_statistics(session)

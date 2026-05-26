"""``GET /dashboard/stats`` — JSON form of the dashboard counters.

This is the JSON-only sibling of the HTMX dashboard partial that lands
in commit 15. The HTMX route uses :func:`compute_dashboard_stats` too,
so the two views stay in lockstep.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.stats_service import compute_dashboard_stats
from .dependencies import get_session
from .schemas import DashboardStats


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats(
    session: AsyncSession = Depends(get_session),
) -> DashboardStats:
    """Return the dashboard counter payload."""
    return await compute_dashboard_stats(session)

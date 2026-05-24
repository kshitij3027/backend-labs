"""Statistics aggregator — read-only summary across the three core tables."""
from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.persistence.models import ErasureRequest, RequestState, UserDataMapping


async def compute_statistics(session: AsyncSession) -> dict[str, Any]:
    """Return system-wide compliance + tracking metrics.

    Schema:
        total_mappings (int)        — count of rows in user_data_mappings
        unique_users (int)          — distinct user_ids across the registry
        completion_rate (float)     — completed_requests / total_requests; 0.0 if no requests
        data_type_counts (dict)     — {data_type: count} across all mappings
    """
    total_mappings = (
        await session.execute(select(func.count()).select_from(UserDataMapping))
    ).scalar_one() or 0

    unique_users = (
        await session.execute(
            select(func.count(func.distinct(UserDataMapping.user_id)))
        )
    ).scalar_one() or 0

    total_requests = (
        await session.execute(select(func.count()).select_from(ErasureRequest))
    ).scalar_one() or 0
    completed_requests = (
        await session.execute(
            select(func.count())
            .select_from(ErasureRequest)
            .where(ErasureRequest.state == RequestState.COMPLETED)
        )
    ).scalar_one() or 0

    completion_rate = (
        round(completed_requests / total_requests, 4) if total_requests > 0 else 0.0
    )

    type_rows = (
        await session.execute(
            select(UserDataMapping.data_type, func.count(UserDataMapping.id))
            .group_by(UserDataMapping.data_type)
            .order_by(UserDataMapping.data_type)
        )
    ).all()
    data_type_counts = {data_type: int(count) for data_type, count in type_rows}

    return {
        "total_mappings": int(total_mappings),
        "unique_users": int(unique_users),
        "completion_rate": float(completion_rate),
        "data_type_counts": data_type_counts,
    }

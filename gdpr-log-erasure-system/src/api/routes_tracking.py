"""User data tracking endpoints.

POST /api/user-data-tracking — register a user data mapping (idempotent on
  the (user_id, data_type, storage_location, data_path) unique tuple;
  duplicate POSTs return the existing row instead of 409).
GET  /api/data-locations/{user_id} — list all mappings for a user.
"""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
from src.api.schemas import UserDataMappingCreate, UserDataMappingResponse
from src.persistence.models import UserDataMapping


router = APIRouter(prefix="/api", tags=["tracking"])


@router.post(
    "/user-data-tracking",
    response_model=UserDataMappingResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_mapping(
    payload: UserDataMappingCreate,
    session: AsyncSession = Depends(get_session),
) -> UserDataMapping:
    """Idempotent register-or-return-existing on the unique location tuple."""
    existing_q = select(UserDataMapping).where(
        UserDataMapping.user_id == payload.user_id,
        UserDataMapping.data_type == payload.data_type,
        UserDataMapping.storage_location == payload.storage_location,
        UserDataMapping.data_path.is_(payload.data_path) if payload.data_path is None
        else UserDataMapping.data_path == payload.data_path,
    )
    existing = (await session.execute(existing_q)).scalar_one_or_none()
    if existing is not None:
        return existing

    mapping = UserDataMapping(
        user_id=payload.user_id,
        data_type=payload.data_type,
        storage_location=payload.storage_location,
        data_path=payload.data_path,
        metadata_json=payload.metadata,
    )
    session.add(mapping)
    await session.commit()
    await session.refresh(mapping)
    return mapping


@router.get(
    "/data-locations/{user_id}",
    response_model=List[UserDataMappingResponse],
)
async def list_locations(
    user_id: str,
    session: AsyncSession = Depends(get_session),
) -> list[UserDataMapping]:
    rows = (
        await session.execute(
            select(UserDataMapping)
            .where(UserDataMapping.user_id == user_id)
            .order_by(UserDataMapping.created_at)
        )
    ).scalars().all()
    return list(rows)

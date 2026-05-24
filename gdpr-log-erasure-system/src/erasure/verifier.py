"""Per-location post-erasure verifier."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.erasure.anonymization import is_anonymized
from src.persistence.models import UserDataMapping


async def verify_erasure(
    session: AsyncSession,
    *,
    mapping_id: int,
    action: str,
) -> bool:
    """Confirm the per-location erasure landed.

    DELETE → mapping row must no longer exist.
    ANONYMIZE → mapping row exists, metadata_json carries the _anonymized marker.
    """
    row = (
        await session.execute(
            select(UserDataMapping).where(UserDataMapping.id == mapping_id)
        )
    ).scalar_one_or_none()

    if action == "DELETE":
        return row is None
    if action == "ANONYMIZE":
        return row is not None and is_anonymized(row.metadata_json)
    return False

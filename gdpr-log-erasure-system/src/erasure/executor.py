"""Per-location erasure executor.

erase_location handles ONE mapping: DELETE removes the row, ANONYMIZE
overwrites metadata_json with scrubbed identifiers (keeps the row so
downstream systems still know "something existed here"). Wrapped with
tenacity for transient retry per the project settings.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.erasure.anonymization import anonymize_mapping_payload
from src.persistence.models import UserDataMapping


class TransientErasureError(RuntimeError):
    """Marker for retry-able per-location failures."""


async def _do_erase(
    session: AsyncSession,
    *,
    mapping: UserDataMapping,
    action: str,
    salt: str,
) -> None:
    if action == "DELETE":
        # Re-fetch by PK and delete via session.delete so cascade works.
        await session.execute(
            delete(UserDataMapping).where(UserDataMapping.id == mapping.id)
        )
    elif action == "ANONYMIZE":
        # Update in place — keeps the row but scrubs identifiers.
        mapping.metadata_json = anonymize_mapping_payload(mapping.metadata_json, salt)
        session.add(mapping)
    else:
        raise ValueError(f"Unsupported action: {action!r}")


async def erase_location(
    session: AsyncSession,
    *,
    mapping: UserDataMapping,
    action: str,
    salt: str,
    retry_count: int,
    retry_backoff_seconds: float,
) -> dict[str, Any]:
    """Erase one mapping. Returns a result dict; never raises out."""
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max(1, retry_count)),
            wait=wait_exponential(multiplier=retry_backoff_seconds, min=retry_backoff_seconds),
            retry=retry_if_exception_type(TransientErasureError),
            reraise=True,
        ):
            with attempt:
                await _do_erase(session, mapping=mapping, action=action, salt=salt)
        return {
            "mapping_id": mapping.id,
            "data_type": mapping.data_type,
            "storage_location": mapping.storage_location,
            "action": action,
            "result": "ok",
        }
    except RetryError as e:
        return {
            "mapping_id": mapping.id,
            "data_type": mapping.data_type,
            "storage_location": mapping.storage_location,
            "action": action,
            "result": "failed",
            "error": f"retry-exhausted: {e!r}",
        }
    except Exception as e:  # final fall-through: non-retryable
        return {
            "mapping_id": mapping.id,
            "data_type": mapping.data_type,
            "storage_location": mapping.storage_location,
            "action": action,
            "result": "failed",
            "error": repr(e),
        }

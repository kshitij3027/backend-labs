"""Unit-style coordinator tests against in-memory SQLite."""
from __future__ import annotations

import pytest
from sqlalchemy import select

from src.audit.verifier import verify_chain
from src.erasure.anonymization import is_anonymized
from src.erasure.coordinator import ErasureCoordinator
from src.persistence.models import (
    ErasureRequest, RequestState, RequestType, UserDataMapping,
)
from src.settings import Settings


def _settings(verification: bool = True) -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        anonymization_hash_salt="test-salt",
        max_parallel_location_erasures=4,
        erasure_retry_count=1,  # no retries in tests for speed
        erasure_retry_backoff_seconds=0,
        verification_enabled=verification,
    )


@pytest.mark.asyncio
async def test_coordinator_anonymize_happy_path(session_factory):
    async with session_factory() as s:
        for dtype in ("system_logs", "analytics_events", "personal_profile"):
            s.add(UserDataMapping(
                user_id="u-h", data_type=dtype, storage_location=f"loc-{dtype}",
                metadata_json={"user_id": "u-h", "ip": "10.1.2.3", "level": "INFO"},
            ))
        req = ErasureRequest(user_id="u-h", request_type=RequestType.ANONYMIZE)
        s.add(req)
        await s.commit()
        rid = req.id

    coord = ErasureCoordinator(session_factory, _settings(verification=True))
    await coord.process(rid)

    async with session_factory() as s:
        req = (await s.execute(
            select(ErasureRequest).where(ErasureRequest.id == rid)
        )).scalar_one()
        assert req.state == RequestState.COMPLETED
        assert req.completed_at is not None

        # system_logs + analytics_events are in the allowlist → anonymised (rows survive)
        # personal_profile is NOT in the allowlist → DELETE fallback (row gone)
        rows = (await s.execute(
            select(UserDataMapping).where(UserDataMapping.user_id == "u-h")
        )).scalars().all()
        by_type = {r.data_type: r for r in rows}
        assert "system_logs" in by_type and is_anonymized(by_type["system_logs"].metadata_json)
        assert "analytics_events" in by_type and is_anonymized(by_type["analytics_events"].metadata_json)
        assert "personal_profile" not in by_type

        ok, bad = await verify_chain(s)
        assert ok is True, f"chain broken at {bad}"


@pytest.mark.asyncio
async def test_coordinator_delete_all_locations(session_factory):
    async with session_factory() as s:
        for i in range(3):
            s.add(UserDataMapping(
                user_id="u-d", data_type="system_logs", storage_location=f"loc-{i}",
            ))
        req = ErasureRequest(user_id="u-d", request_type=RequestType.DELETE)
        s.add(req)
        await s.commit()
        rid = req.id

    coord = ErasureCoordinator(session_factory, _settings(verification=True))
    await coord.process(rid)

    async with session_factory() as s:
        req = (await s.execute(
            select(ErasureRequest).where(ErasureRequest.id == rid)
        )).scalar_one()
        assert req.state == RequestState.COMPLETED
        remaining = (await s.execute(
            select(UserDataMapping).where(UserDataMapping.user_id == "u-d")
        )).scalars().all()
        assert remaining == []


@pytest.mark.asyncio
async def test_coordinator_no_mappings_still_completes(session_factory):
    """A user with no registered locations should COMPLETE cleanly."""
    async with session_factory() as s:
        req = ErasureRequest(user_id="u-none", request_type=RequestType.DELETE)
        s.add(req)
        await s.commit()
        rid = req.id

    coord = ErasureCoordinator(session_factory, _settings())
    await coord.process(rid)

    async with session_factory() as s:
        req = (await s.execute(
            select(ErasureRequest).where(ErasureRequest.id == rid)
        )).scalar_one()
        assert req.state == RequestState.COMPLETED


@pytest.mark.asyncio
async def test_coordinator_fail_path_on_unknown_request_id(session_factory):
    coord = ErasureCoordinator(session_factory, _settings())
    # never raises out of `process`
    await coord.process("non-existent-uuid")
    # nothing to assert beyond "did not raise" — log captures the failure


@pytest.mark.asyncio
async def test_coordinator_verification_disabled_skips_to_completed(session_factory):
    async with session_factory() as s:
        s.add(UserDataMapping(user_id="u-v", data_type="system_logs", storage_location="loc"))
        req = ErasureRequest(user_id="u-v", request_type=RequestType.DELETE)
        s.add(req)
        await s.commit()
        rid = req.id

    coord = ErasureCoordinator(session_factory, _settings(verification=False))
    await coord.process(rid)

    async with session_factory() as s:
        req = (await s.execute(
            select(ErasureRequest).where(ErasureRequest.id == rid)
        )).scalar_one()
        assert req.state == RequestState.COMPLETED

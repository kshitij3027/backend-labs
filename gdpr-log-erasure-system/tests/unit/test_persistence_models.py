"""Unit tests for the three ORM models + ``init_db`` genesis seeding.

Covers, per the C2 plan:

  1. Round-trip a ``UserDataMapping``.
  2. Unique constraint on ``(user_id, data_type, storage_location,
     data_path)`` rejects an exact duplicate insert.
  3. Round-trip an ``ErasureRequest`` — UUID PK is a string, ``state``
     defaults to ``PENDING``, ``RequestType`` enum survives a fetch.
  4. Round-trip an ``ErasureAuditLog`` linked to an ``ErasureRequest``
     and confirm the back-populating ``audit_entries`` relationship
     returns it.
  5. Confirm ``init_db`` seeds exactly one genesis row with
     ``sequence=0``, ``request_id IS NULL``, ``event_type="GENESIS"``,
     and ``prev_hash == "0" * 64``.

The ``session_factory`` fixture comes from ``tests/conftest.py`` and
points at an in-memory SQLite engine that's already had ``init_db``
called against it (so the genesis row is present from the start).
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from src.audit.chain import GENESIS_PREV_HASH, GENESIS_SEQUENCE
from src.persistence.models import (
    ErasureAuditLog,
    ErasureRequest,
    RequestState,
    RequestType,
    UserDataMapping,
)


@pytest.mark.asyncio
async def test_create_and_fetch_user_data_mapping(session_factory):
    """A fresh ``UserDataMapping`` insert lands with PK + ``created_at`` populated."""
    async with session_factory() as session:
        mapping = UserDataMapping(
            user_id="user-1",
            data_type="system_logs",
            storage_location="loc-a",
            data_path="/logs/user-1",
            metadata_json={"region": "eu-west-1"},
        )
        session.add(mapping)
        await session.commit()
        await session.refresh(mapping)

        assert mapping.id is not None
        assert mapping.user_id == "user-1"
        assert mapping.data_type == "system_logs"
        assert mapping.storage_location == "loc-a"
        assert mapping.data_path == "/logs/user-1"
        assert mapping.metadata_json == {"region": "eu-west-1"}
        assert isinstance(mapping.created_at, dt.datetime)


@pytest.mark.asyncio
async def test_user_data_mapping_unique_constraint(session_factory):
    """Inserting the same (user, type, location, path) twice raises IntegrityError."""
    async with session_factory() as session:
        session.add(
            UserDataMapping(
                user_id="user-2",
                data_type="analytics_events",
                storage_location="loc-b",
                data_path="/events/user-2",
            )
        )
        await session.commit()

    # Second insert with identical key fields should fail at commit
    # time. Use a new session because the first one is already closed
    # and we want a clean transactional boundary for the failure.
    with pytest.raises(IntegrityError):
        async with session_factory() as session:
            session.add(
                UserDataMapping(
                    user_id="user-2",
                    data_type="analytics_events",
                    storage_location="loc-b",
                    data_path="/events/user-2",
                )
            )
            await session.commit()


@pytest.mark.asyncio
async def test_create_and_fetch_erasure_request(session_factory):
    """``ErasureRequest`` rows mint a UUID PK and default to PENDING."""
    async with session_factory() as session:
        request = ErasureRequest(
            user_id="user-3",
            request_type=RequestType.DELETE,
        )
        session.add(request)
        await session.commit()
        await session.refresh(request)

        assert isinstance(request.id, str)
        assert len(request.id) == 36  # standard UUID4 string length
        assert request.user_id == "user-3"
        assert request.request_type == RequestType.DELETE
        assert request.state == RequestState.PENDING
        assert request.error_message is None
        assert isinstance(request.created_at, dt.datetime)
        assert request.started_at is None
        assert request.completed_at is None

        # Round-trip via a fresh fetch to confirm the enum survives the DB layer.
        fetched = await session.get(ErasureRequest, request.id)
        assert fetched is not None
        assert fetched.request_type == RequestType.DELETE
        assert fetched.state == RequestState.PENDING


@pytest.mark.asyncio
async def test_audit_log_entry_links_back_to_request(session_factory):
    """An ``ErasureAuditLog`` row shows up under ``request.audit_entries``."""
    # Build truncated 64-char hashes from short repeated patterns so the
    # values are deterministic and easy to eyeball in failure output.
    prev_hash = ("abc" * 24)[:64]
    entry_hash = ("def" * 24)[:64]
    assert len(prev_hash) == 64
    assert len(entry_hash) == 64

    async with session_factory() as session:
        request = ErasureRequest(
            user_id="user-4",
            request_type=RequestType.ANONYMIZE,
        )
        session.add(request)
        await session.commit()
        await session.refresh(request)

        audit = ErasureAuditLog(
            request_id=request.id,
            sequence=1,
            event_type="STATE_TRANSITION",
            payload_json={"from": "PENDING", "to": "DISCOVERING"},
            prev_hash=prev_hash,
            entry_hash=entry_hash,
        )
        session.add(audit)
        await session.commit()
        await session.refresh(audit)

        assert audit.id is not None
        assert audit.request_id == request.id
        assert audit.sequence == 1
        assert audit.event_type == "STATE_TRANSITION"
        assert audit.payload_json == {"from": "PENDING", "to": "DISCOVERING"}
        assert audit.prev_hash == prev_hash
        assert audit.entry_hash == entry_hash

    # Fresh session to force the relationship to actually load the row
    # from the DB, not return a cached object on the prior session.
    async with session_factory() as session:
        result = await session.execute(
            select(ErasureRequest).where(ErasureRequest.id == request.id)
        )
        fetched = result.scalar_one()
        # Touch the relationship under the async session so it loads.
        await session.refresh(fetched, attribute_names=["audit_entries"])
        assert len(fetched.audit_entries) == 1
        assert fetched.audit_entries[0].sequence == 1
        assert fetched.audit_entries[0].event_type == "STATE_TRANSITION"


@pytest.mark.asyncio
async def test_init_db_seeds_exactly_one_genesis_row(session_factory):
    """After ``init_db`` (called by the engine fixture) the genesis row is present."""
    async with session_factory() as session:
        result = await session.execute(
            select(ErasureAuditLog).where(
                ErasureAuditLog.sequence == GENESIS_SEQUENCE
            )
        )
        genesis = result.scalar_one()  # raises if not exactly one row

        assert genesis.sequence == GENESIS_SEQUENCE
        assert genesis.request_id is None
        assert genesis.event_type == "GENESIS"
        assert genesis.prev_hash == GENESIS_PREV_HASH
        assert genesis.prev_hash == "0" * 64
        assert len(genesis.entry_hash) == 64  # SHA-256 hex digest

        # Sanity: only one row with sequence=0, regardless of any other
        # entries that future tests in the same session might add.
        count_result = await session.execute(
            select(func.count())
            .select_from(ErasureAuditLog)
            .where(ErasureAuditLog.sequence == GENESIS_SEQUENCE)
        )
        assert count_result.scalar_one() == 1

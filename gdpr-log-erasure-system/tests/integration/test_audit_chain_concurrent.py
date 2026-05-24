"""Integration test: 10 concurrent appends against real Postgres preserve a contiguous chain.

The point is to confirm that ``with_for_update()`` actually serialises
concurrent writers and produces a contiguous sequence with no gaps and
no duplicate sequences. SQLite's single-writer semantics already give
us this in the unit tests, but Postgres needs the row-level lock to
prevent two transactions from reading the same "last" row and both
deciding their next sequence is ``last.sequence + 1``.

Gated by the ``DATABASE_URL`` env var: skipped unless it points at a
real Postgres database.
"""
from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.audit.chain import append_audit_entry
from src.audit.verifier import verify_chain
from src.persistence.db import init_db, make_engine, make_session_factory
from src.persistence.models import ErasureAuditLog


DATABASE_URL = os.getenv("DATABASE_URL", "")
REQUIRES_POSTGRES = pytest.mark.skipif(
    "postgresql" not in DATABASE_URL,
    reason="concurrent chain test requires real Postgres (DATABASE_URL unset or not postgres)",
)


@pytest_asyncio.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    """Per-test Postgres engine with the audit log truncated + genesis re-seeded."""
    eng = make_engine(DATABASE_URL)
    # Truncate the audit log before each run so sequences start fresh.
    async with eng.begin() as conn:
        from sqlalchemy import text

        await conn.execute(
            text("TRUNCATE TABLE erasure_audit_log RESTART IDENTITY CASCADE")
        )
    await init_db(eng)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def pg_session_factory(pg_engine: AsyncEngine) -> async_sessionmaker:
    """Async session factory bound to the per-test Postgres engine fixture."""
    return make_session_factory(pg_engine)


@REQUIRES_POSTGRES
@pytest.mark.asyncio
async def test_concurrent_appends_preserve_chain(pg_session_factory):
    """10 parallel appends produce sequences 0..10 with a valid chain."""

    async def worker(i: int):
        async with pg_session_factory() as s:
            await append_audit_entry(
                s,
                request_id=None,
                event_type="WORKER",
                payload={"i": i},
            )
            await s.commit()

    await asyncio.gather(*[worker(i) for i in range(10)])

    async with pg_session_factory() as s:
        rows = (
            await s.execute(
                select(ErasureAuditLog).order_by(ErasureAuditLog.sequence)
            )
        ).scalars().all()
        sequences = [r.sequence for r in rows]
        # genesis (0) + 10 workers (1..10) → contiguous 0..10
        assert sequences == list(range(0, 11))
        ok, bad = await verify_chain(s)
        assert ok is True, f"chain broken at sequence {bad}"

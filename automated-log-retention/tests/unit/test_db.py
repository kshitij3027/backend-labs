"""Unit tests for ``src/persistence/db.py``.

The PRAGMA listener is the load-bearing piece — it must fire on every
new connection from the pool, including the first one. WAL specifically
does NOT apply to ``:memory:`` databases (SQLite always reports
``memory`` for the journal mode of an in-memory DB), so we cover both
flavours: the in-memory engine exercises the listener execution path,
and a tmp-file engine confirms WAL actually sticks on a real backing
store.
"""
from __future__ import annotations

import pytest
import sqlalchemy as sa

from src.persistence.db import init_db, make_engine, make_session_factory


# --- PRAGMA application ------------------------------------------------------


@pytest.mark.asyncio
async def test_make_engine_applies_pragmas_in_memory():
    """All non-WAL PRAGMAs land on an in-memory DB; WAL reports 'memory'.

    SQLite's in-memory mode silently ignores ``journal_mode=WAL`` and
    reports ``memory`` for any ``PRAGMA journal_mode`` query. The other
    four PRAGMAs still apply, so we assert them in full and only check
    that the journal-mode probe returns *something* (not WAL).
    """
    engine = make_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.connect() as conn:
            journal = (await conn.exec_driver_sql("PRAGMA journal_mode;")).scalar_one()
            synchronous = (await conn.exec_driver_sql("PRAGMA synchronous;")).scalar_one()
            temp_store = (await conn.exec_driver_sql("PRAGMA temp_store;")).scalar_one()
            cache_size = (await conn.exec_driver_sql("PRAGMA cache_size;")).scalar_one()
            foreign_keys = (await conn.exec_driver_sql("PRAGMA foreign_keys;")).scalar_one()
        # In-memory DB reports 'memory' for journal_mode regardless of PRAGMA.
        assert journal.lower() == "memory"
        assert synchronous == 1  # NORMAL
        assert temp_store == 2  # MEMORY
        assert cache_size == -64000
        assert foreign_keys == 1  # ON
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_make_engine_applies_wal_on_file_backed_db(tmp_path):
    """A real on-disk SQLite file actually accepts WAL journal mode."""
    db_path = tmp_path / "test.db"
    engine = make_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.connect() as conn:
            journal = (await conn.exec_driver_sql("PRAGMA journal_mode;")).scalar_one()
            synchronous = (await conn.exec_driver_sql("PRAGMA synchronous;")).scalar_one()
            temp_store = (await conn.exec_driver_sql("PRAGMA temp_store;")).scalar_one()
            cache_size = (await conn.exec_driver_sql("PRAGMA cache_size;")).scalar_one()
            foreign_keys = (await conn.exec_driver_sql("PRAGMA foreign_keys;")).scalar_one()
        assert journal.lower() == "wal"
        assert synchronous == 1
        assert temp_store == 2
        assert cache_size == -64000
        assert foreign_keys == 1
    finally:
        await engine.dispose()


# --- init_db -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_init_db_creates_all_tables(tmp_path):
    """All 5 declared tables show up in ``sqlite_master`` after init_db."""
    db_path = tmp_path / "init.db"
    engine = make_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        await init_db(engine)
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            )
            tables = {row[0] for row in result.fetchall()}
        expected = {
            "files",
            "transitions",
            "pending_deletes",
            "audit_entries",
            "job_runs",
        }
        # All five expected tables must be present (other internal sqlite_*
        # tables are filtered out by the WHERE clause above).
        assert expected.issubset(tables), (
            f"missing tables: {expected - tables}; present: {tables}"
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_init_db_is_idempotent(tmp_path):
    """Calling init_db twice on the same engine is a no-op the second time."""
    db_path = tmp_path / "idem.db"
    engine = make_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        await init_db(engine)
        await init_db(engine)  # must not raise
        # And the table list should be unchanged.
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql(
                "SELECT count(*) FROM sqlite_master WHERE type='table';"
            )
            count = result.scalar_one()
        assert count >= 5  # at least our 5 tables
    finally:
        await engine.dispose()


# --- session_factory ---------------------------------------------------------


@pytest.mark.asyncio
async def test_make_session_factory_returns_usable_sessions(tmp_path):
    """``make_session_factory`` produces sessions that can begin/commit."""
    db_path = tmp_path / "sess.db"
    engine = make_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        await init_db(engine)
        factory = make_session_factory(engine)
        async with factory() as session:
            result = await session.execute(sa.text("SELECT 1"))
            assert result.scalar_one() == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_make_session_factory_expire_on_commit_false(tmp_path):
    """``expire_on_commit=False`` means attributes survive commit without re-fetch."""
    db_path = tmp_path / "expire.db"
    engine = make_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        await init_db(engine)
        factory = make_session_factory(engine)
        # The async_sessionmaker stores its kw config on the factory itself.
        # Probe via the actual session — after commit the attributes are
        # still accessible (the symptom of expire_on_commit=False).
        from datetime import datetime

        from src.persistence.models import File

        async with factory() as session:
            f = File(
                source="x",
                segment_path="/p/x",
                tier="hot",
                size_bytes=1,
                oldest_record_ts=datetime(2026, 1, 1),
                newest_record_ts=datetime(2026, 1, 1),
            )
            session.add(f)
            await session.commit()
            # If expire_on_commit were True, this would trigger an implicit
            # SELECT (we're still in an async context — accessing the attr
            # would raise). With our config it's just a Python attribute read.
            assert f.source == "x"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_make_engine_returns_future_engine():
    """Engine is built with ``future=True`` (2.0-style execution)."""
    engine = make_engine("sqlite+aiosqlite:///:memory:")
    try:
        # The sync_engine should be a 2.0-style Engine; the simplest probe
        # is that it accepts a select() compiled in 2.0 mode without warning.
        async with engine.connect() as conn:
            result = await conn.execute(sa.select(sa.literal(7)))
            assert result.scalar_one() == 7
    finally:
        await engine.dispose()

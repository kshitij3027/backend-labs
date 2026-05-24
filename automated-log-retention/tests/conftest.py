"""Shared pytest fixtures for the automated-log-retention test suite.

Two reusable fixtures land here in C02:

  * ``engine`` — a fresh in-memory ``AsyncEngine`` per test with
    ``init_db`` already called and the PRAGMA listener active. Disposed
    on teardown so each test starts from a clean schema.
  * ``session_factory`` — an ``async_sessionmaker`` bound to that engine,
    suitable for ``async with session_factory() as session:`` blocks.

Both use ``pytest_asyncio.fixture`` for explicit async-fixture semantics
even though ``pytest.ini`` sets ``asyncio_mode = auto``.

NOTE on in-memory pooling: the aiosqlite dialect ships ``StaticPool``
as the default pool class for ``:memory:`` URLs (see
``sqlalchemy.dialects.sqlite.aiosqlite.SQLiteDialect_aiosqlite.get_pool_class``)
so every session checkout reuses the single underlying connection —
which is what makes the schema from one ``init_db`` call visible to
later session checkouts in the same test. ``make_engine`` therefore
needs no special pool argument; the default does the right thing here.
"""
from __future__ import annotations

from typing import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.persistence.db import init_db, make_engine, make_session_factory


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """Fresh in-memory async engine with all tables created and disposed on teardown."""
    eng = make_engine("sqlite+aiosqlite:///:memory:")
    await init_db(eng)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Async session factory bound to the per-test ``engine`` fixture."""
    return make_session_factory(engine)

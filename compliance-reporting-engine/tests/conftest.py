"""Shared pytest fixtures: in-memory SQLite engine + session factory.

Two fixtures land in C2:

  * ``engine`` — a fresh in-memory ``AsyncEngine`` per test with
    ``init_db`` already called so all tables exist. Disposed on
    teardown so each test starts from a clean schema.
  * ``session_factory`` — an ``async_sessionmaker`` bound to that
    engine, suitable for ``async with session_factory() as session:``
    blocks.

Both use ``pytest_asyncio.fixture`` for explicit async-fixture
semantics even when the rest of the test suite leans on
``asyncio_mode = auto`` from ``pytest.ini``.

NOTE on in-memory pooling: ``StaticPool`` makes every connection
checkout reuse a single underlying SQLite connection, which is what
keeps the schema from ``init_db`` visible to later session checkouts
in the same test. Without it, each checkout opens a brand-new
``:memory:`` DB and the tables vanish between calls.
"""
from __future__ import annotations

from typing import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from src.persistence.db import init_db
from src.persistence.models import Base  # noqa: F401  (ensures table metadata loads)


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """Fresh in-memory async engine with all tables created and disposed on teardown."""
    eng = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
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
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

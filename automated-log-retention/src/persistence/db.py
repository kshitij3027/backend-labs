"""Async SQLite engine factory, session factory, and DB initialization.

The engine boots in WAL journal mode (concurrent readers + single writer)
with NORMAL durability, an in-memory temp store, a 64 MiB page cache,
and foreign-key enforcement on. These PRAGMAs are applied via a connect
listener so they take effect on every new sqlite3 connection the pool
hands out — not just the first.

PRAGMAs run inside the listener via ``dbapi_connection.cursor()`` because
SQLAlchemy's ``connect`` event fires on the synchronous DBAPI connection
underneath the async wrapper (aiosqlite still hands the listener a
synchronous sqlite3 connection from the pool).

``init_db`` is idempotent: ``create_all`` is a no-op on already-existing
tables, so it's safe to call on every app startup. The audit chain
genesis row is NOT inserted here — that lands in C13 alongside the
appender, which has the canonicalisation + hashing logic the genesis
row depends on.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.persistence.models import Base

log = logging.getLogger(__name__)


def make_engine(database_url: str) -> AsyncEngine:
    """Create the async engine and register a PRAGMA listener.

    The listener fires on every new SQLite connection and applies the
    five tuning PRAGMAs the project relies on:

      * ``journal_mode=WAL`` — concurrent readers + single writer (the
        right shape for the scheduler-driven workload).
      * ``synchronous=NORMAL`` — durable enough for our use case while
        avoiding the per-commit fsync stall of FULL.
      * ``temp_store=MEMORY`` — keep temporary tables/indexes off disk.
      * ``cache_size=-64000`` — negative value means KiB, so 64 MiB of
        page cache; comfortably bigger than the working set on the
        target workload.
      * ``foreign_keys=ON`` — SQLite ships FKs disabled by default; we
        rely on them for the ``transitions`` and ``pending_deletes``
        relationships.
    """
    engine = create_async_engine(database_url, echo=False, future=True)

    @event.listens_for(engine.sync_engine, "connect")
    def _apply_pragmas(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
            cursor.execute("PRAGMA temp_store = MEMORY;")
            cursor.execute("PRAGMA cache_size = -64000;")
            cursor.execute("PRAGMA foreign_keys = ON;")
        finally:
            cursor.close()

    return engine


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Bound async session factory. Use as ``async with factory() as session:``.

    ``expire_on_commit=False`` keeps ORM attributes accessible after a
    commit without a fresh SELECT — important when callers want to read
    fields off the just-committed row (e.g., the catalog repo returning
    a freshly-inserted ``File``).
    """
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(engine: AsyncEngine) -> None:
    """Create all tables declared on ``Base.metadata``.

    Idempotent: ``create_all`` skips tables that already exist. Safe to
    call on every boot; the lifespan in C12 will do exactly that.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("init_db: all tables created (or already present)")

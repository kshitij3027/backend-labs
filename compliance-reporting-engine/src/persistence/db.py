"""Async SQLAlchemy engine + session factory + ``init_db``.

The engine and session factory are constructed once at app startup (see
:func:`src.main.lifespan`) and stashed on ``app.state`` so request
handlers can grab a session via dependency injection without re-creating
the connection pool per call.

Two databases share this module: PostgreSQL in production / docker
(``postgresql+asyncpg://...``), and in-memory SQLite
(``sqlite+aiosqlite:///:memory:``) inside the test suite. The
cross-dialect ``GUID`` / JSON types defined in :mod:`.models` are what
make the same schema work on both engines without drift.

``init_db`` is intentionally minimal: it just calls
``Base.metadata.create_all`` inside an ``engine.begin()`` block. No
genesis-row seeding is needed for this project — the reports + log
events tables hold no anchor rows, unlike the GDPR sibling's audit
chain.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(database_url: str) -> AsyncEngine:
    """Build the async engine with pool pre-ping for long-lived deployments.

    ``pool_pre_ping=True`` issues a lightweight ``SELECT 1`` before
    handing out a pooled connection, which avoids ``OperationalError``
    when Postgres or its proxy has dropped an idle connection. Cheap on
    every checkout, paid back many times over in production.
    """
    return create_async_engine(
        database_url, echo=False, future=True, pool_pre_ping=True
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Bound async session factory.

    ``expire_on_commit=False`` keeps ORM attributes readable after
    ``await session.commit()`` without a fresh ``SELECT`` round-trip,
    which matters for the request handlers that want to read fields off
    a row they just inserted.
    """
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(engine: AsyncEngine) -> None:
    """Create all tables idempotently.

    Called once from the FastAPI lifespan on startup. Safe to call
    multiple times against the same DB: ``create_all`` skips tables
    that already exist.
    """
    # Local import so model registration with ``Base.metadata`` happens
    # at call time (not at module import) — this avoids any circular
    # import between ``db`` and ``models`` at startup.
    from .models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

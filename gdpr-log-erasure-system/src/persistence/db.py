"""Async SQLAlchemy engine + session factory + ``init_db``.

The engine and session factory are constructed once at app startup (see
:func:`src.main.lifespan`) and stashed on ``app.state`` so request
handlers can grab a session via dependency injection without re-creating
the connection pool per call.

Two databases share this module: PostgreSQL in production / docker, and
in-memory SQLite (``sqlite+aiosqlite:///:memory:``) inside the test
suite. The sibling ``automated-log-retention`` project ships SQLite
PRAGMA listeners; we deliberately omit them here because PostgreSQL
does not need WAL / synchronous tuning and the in-memory SQLite engine
the tests use is fine on defaults.

``init_db`` does two things:

  1. Creates all tables via ``Base.metadata.create_all`` (idempotent;
     ``create_all`` is a no-op when tables already exist).
  2. Seeds the audit-chain genesis row (``sequence=0``) inside a
     separate transaction, idempotently. The genesis row is the
     anchor every subsequent ``ErasureAuditLog`` row chains back to,
     so a fresh DB is immediately ready for ``append_audit_entry``
     calls (which arrive in commit 5).
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


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
    """Create all tables and seed the audit-chain genesis row idempotently.

    Called once from the FastAPI lifespan on startup. Safe to call
    multiple times against the same DB: ``create_all`` skips existing
    tables, and the genesis-seed branch checks for an existing
    ``sequence=0`` row before inserting.
    """
    # Imports are local so model registration with Base.metadata happens
    # at call time (not at module import) — this avoids a circular
    # import between db.py and models.py at startup.
    import datetime as _dt
    import json as _json

    from sqlalchemy import select

    from src.audit.chain import (
        GENESIS_PREV_HASH,
        GENESIS_SEQUENCE,
        compute_entry_hash,
    )
    from src.persistence import models  # noqa: F401  (registers tables with Base)
    from src.persistence.models import ErasureAuditLog

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Seed genesis row in a separate transaction so the schema-create
    # commit lands first regardless of whether the genesis insert is a
    # no-op or an actual write.
    factory = make_session_factory(engine)
    async with factory() as session:
        existing = await session.execute(
            select(ErasureAuditLog).where(ErasureAuditLog.sequence == GENESIS_SEQUENCE)
        )
        if existing.scalar_one_or_none() is None:
            # Genesis payload is deterministic, so two boots against the
            # same DB never disagree about sequence=0. ``microsecond=0``
            # keeps the ISO string stable across the SQLite/Postgres
            # round-trip.
            created_at = _dt.datetime.utcnow().replace(microsecond=0)
            payload = {"event": "genesis", "system": "gdpr-log-erasure"}
            payload_json_str = _json.dumps(payload, sort_keys=True)
            entry_hash = compute_entry_hash(
                prev_hash=GENESIS_PREV_HASH,
                sequence=GENESIS_SEQUENCE,
                event_type="GENESIS",
                payload_json_str=payload_json_str,
                created_at_iso=created_at.isoformat(),
            )
            session.add(
                ErasureAuditLog(
                    request_id=None,
                    sequence=GENESIS_SEQUENCE,
                    event_type="GENESIS",
                    payload_json=payload,
                    prev_hash=GENESIS_PREV_HASH,
                    entry_hash=entry_hash,
                    created_at=created_at,
                )
            )
            await session.commit()

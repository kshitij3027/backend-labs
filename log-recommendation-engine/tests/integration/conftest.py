"""Fixtures for the integration suite (runs against the REAL Postgres+pgvector).

These tests need a live PostgreSQL with the ``vector`` extension (JSONB, text[],
tz-aware datetimes and the ``vector(384)`` column + HNSW index cannot be faked
with SQLite). ``DATABASE_URL`` is supplied by the compose ``test`` service and
points at the ``postgres`` service.

Schema application
------------------
A **session-scoped** fixture applies the Alembic migrations programmatically
(``command.upgrade(cfg, "head")``) exactly once per test session. ``env.py``
injects the real ``DATABASE_URL`` from settings, so no URL is passed here. The
upgrade is idempotent — re-running against an already-migrated DB is a no-op.

Isolation
---------
Each test gets a ``db_session`` bound to a single connection inside an outer
transaction that is **rolled back** at teardown, so writes never persist across
tests and the migrated schema (tables + HNSW index) is never dropped. A SAVEPOINT
(``begin_nested``) is restarted after each inner ``commit()`` so repository
helpers that commit still see rollback-on-teardown semantics.
"""

from __future__ import annotations

import os
from typing import Iterator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.orm import Session

from src.db.session import SessionLocal, get_engine

# alembic.ini lives at the project root, which is the WORKDIR (/app) in the test
# image and the repo root for a local run. Resolve it relative to this file so the
# fixture works regardless of the pytest invocation directory.
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_ALEMBIC_INI = os.path.join(_PROJECT_ROOT, "alembic.ini")


@pytest.fixture(scope="session", autouse=True)
def _migrated_db() -> None:
    """Apply Alembic migrations to the test database once per session.

    ``env.py`` sets ``sqlalchemy.url`` from ``get_settings().database_url``, so the
    real compose ``DATABASE_URL`` is used. Idempotent: a no-op if already at head.
    """
    cfg = Config(_ALEMBIC_INI)
    command.upgrade(cfg, "head")


@pytest.fixture
def db_session() -> Iterator[Session]:
    """Yield a session wrapped in a transaction rolled back after the test.

    Binds a session to a dedicated connection with an open outer transaction; a
    restarting SAVEPOINT keeps everything undone at teardown even when repository
    helpers call ``commit()``. The connection is closed last, discarding all writes.
    """
    connection = get_engine().connect()
    transaction = connection.begin()
    session = SessionLocal()
    session.bind = connection  # type: ignore[assignment]
    session.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def _restart_savepoint(sess: Session, trans) -> None:  # noqa: ANN001
        # When the SAVEPOINT (nested) ends and the outer transaction is still
        # active, open a fresh SAVEPOINT so subsequent commits stay contained.
        if trans.nested and not trans._parent.nested:
            if connection.in_transaction():
                sess.begin_nested()

    try:
        yield session
    finally:
        event.remove(session, "after_transaction_end", _restart_savepoint)
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()


@pytest.fixture
def raw_connection() -> Iterator:
    """Yield a plain DBAPI-level SQLAlchemy connection for catalog queries.

    Used by tests that inspect ``pg_extension`` / ``pg_indexes`` (schema-level
    assertions) rather than exercising the ORM.
    """
    connection = get_engine().connect()
    try:
        yield connection
    finally:
        connection.close()


# Re-export ``text`` so tests can import it from the conftest if convenient.
__all__ = ["db_session", "raw_connection", "text"]

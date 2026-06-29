"""Engine + session machinery (SQLAlchemy 2.0 style).

The database URL is sourced exclusively from :func:`src.config.get_settings`
(``database_url``) — credentials are never hardcoded here. The engine and
``SessionLocal`` factory are created lazily and cached so the configuration is
read once per process and tests can monkeypatch settings before first use.

Two access patterns are provided:

* :func:`get_session` — a context manager for scripts / workers / tasks::

      with get_session() as session:
          repository.add_incident(session, ...)

* :func:`get_db` — a FastAPI dependency that yields a session and always closes
  it::

      @app.get("/x")
      def handler(db: Session = Depends(get_db)):
          ...
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

_engine: Engine | None = None
_SessionFactory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy :class:`Engine`, creating it once.

    ``pool_pre_ping`` guards against stale connections (e.g. Postgres dropping
    idle ones), which matters for long-lived server processes.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            future=True,
        )
    return _engine


def _get_session_factory() -> sessionmaker[Session]:
    """Return the cached ``sessionmaker`` bound to the shared engine."""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )
    return _SessionFactory


def SessionLocal() -> Session:  # noqa: N802 - kept PascalCase per SQLAlchemy convention
    """Create a new :class:`~sqlalchemy.orm.Session`.

    Callable factory so existing ``SessionLocal()`` call sites read naturally;
    the caller owns the session lifecycle (close / commit / rollback).
    """
    return _get_session_factory()()


@contextmanager
def get_session() -> Iterator[Session]:
    """Context manager yielding a session, rolling back on error, always closing.

    The caller is responsible for committing successful work (repository helpers
    accept ``commit=True`` to do so). On an exception the transaction is rolled
    back before the session is closed.
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Iterator[Session]:
    """FastAPI dependency: yield a session and guarantee it is closed.

    Usage: ``def handler(db: Session = Depends(get_db)): ...``.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

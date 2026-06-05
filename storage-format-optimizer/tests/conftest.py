"""Shared pytest fixtures for the storage-format-optimizer test suite.

Provides the empty-run -> exit-0 hook, a fresh (non-cached) ``settings``
fixture so tests get defaults regardless of the environment, a ``tmp_data_dir``
fixture giving each storage test an isolated directory, and an isolated ASGI
``client`` fixture that drives the wired FastAPI app (full object graph +
ingest/query routers) against a per-test temporary data dir.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.settings import Settings


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def settings() -> Settings:
    """Return a fresh, non-cached Settings instance for tests (defaults only)."""
    return Settings()


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Return an isolated, existing data directory for storage tests."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Yield a ``TestClient`` driving the wired app against an isolated data dir.

    Points ``DATA_DIR`` / ``LOG_DIR`` at per-test ``tmp_path`` subdirs and pins
    ``MIGRATION_INTERVAL_SECONDS`` to an hour so the background migration loop
    stays quiet — API tests then observe deterministic state with no migrations
    firing underneath them. The ``get_settings`` LRU cache is cleared on the way
    in (so these env overrides take effect) and on the way out (so a later test's
    settings are rebuilt fresh).

    Entering the ``TestClient`` context manager runs the app's lifespan, which
    builds the full object graph on ``app.state``; exiting it tears the migration
    loop down.
    """
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("MIGRATION_INTERVAL_SECONDS", "3600")  # don't auto-migrate during tests
    from src.settings import get_settings

    get_settings.cache_clear()
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c
    get_settings.cache_clear()

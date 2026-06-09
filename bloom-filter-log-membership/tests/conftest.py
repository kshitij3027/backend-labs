"""Shared pytest fixtures for the bloom-filter-log-membership test suite.

Provides the empty-run -> exit-0 hook (keeps ``make test-e2e`` green while
``tests/e2e`` has no tests yet — pytest exits 5 on an empty collection), a
fresh non-cached ``settings`` fixture, a ``tmp_data_dir`` fixture that
points ``DATA_DIR`` at an isolated per-test directory with the
``get_settings`` cache cleared around the test, and on top of those the
``api_env`` / ``client`` fixtures that integration tests use to run the
full ASGI app (lifespan included) against that isolated environment.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.settings import Settings, get_settings


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def settings() -> Settings:
    """Return a fresh, non-cached Settings instance (bypasses the LRU cache)."""
    return Settings()


@pytest.fixture
def tmp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point ``DATA_DIR`` at an isolated, existing per-test directory.

    The ``get_settings`` LRU cache is cleared on the way in (so the env
    override is visible to any code resolving settings lazily) and on the way
    out (so a later test's settings get rebuilt from the restored environment
    rather than served stale from the cache).
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    get_settings.cache_clear()
    yield data_dir
    get_settings.cache_clear()


@pytest.fixture
def api_env(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    """Full environment isolation for app-level (lifespan-running) tests.

    Layers on ``tmp_data_dir`` (isolated ``DATA_DIR``) and parks both
    background-task intervals at an hour so neither the snapshot loop nor
    the rotation loop can ever fire mid-test — tests that *want* the loops
    to fire override the env again (and re-clear the settings cache)
    before opening their TestClient. Yields the data dir so tests can
    assert on snapshot files. The cache is cleared on the way out as well,
    so the next test cannot inherit these settings.
    """
    monkeypatch.setenv("SNAPSHOT_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("ROTATION_CHECK_INTERVAL_SECONDS", "3600")
    get_settings.cache_clear()
    yield tmp_data_dir
    get_settings.cache_clear()


@pytest.fixture
def client(api_env: Path) -> Iterator[TestClient]:
    """A TestClient over the real app with its lifespan running.

    Entering the context runs startup (settings resolution, manager build,
    snapshot reload, background tasks) against the ``api_env`` isolation;
    leaving it runs shutdown (task cancellation + final snapshot into the
    tmp data dir). The app module is imported inside the fixture — settings
    resolve at lifespan time, not import time, but importing after the env
    is staged keeps the fixture safe even if that ever changes.
    """
    from src.api import app

    with TestClient(app) as test_client:
        yield test_client

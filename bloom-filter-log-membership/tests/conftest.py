"""Shared pytest fixtures for the bloom-filter-log-membership test suite.

Provides the empty-run -> exit-0 hook (keeps ``make test-e2e`` green while
``tests/e2e`` has no tests yet — pytest exits 5 on an empty collection), a
fresh non-cached ``settings`` fixture, and a ``tmp_data_dir`` fixture that
points ``DATA_DIR`` at an isolated per-test directory with the
``get_settings`` cache cleared around the test.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

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

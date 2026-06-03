"""Shared pytest fixtures for the storage-format-optimizer test suite.

Provides the empty-run -> exit-0 hook, a fresh (non-cached) ``settings``
fixture so tests get defaults regardless of the environment, and a
``tmp_data_dir`` fixture giving each storage test an isolated directory.

An ASGI ``client`` fixture is intentionally NOT defined here yet — the FastAPI
app does not exist until a later commit. It will be added then.
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

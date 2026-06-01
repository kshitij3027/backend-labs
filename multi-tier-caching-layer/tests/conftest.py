"""Shared pytest fixtures for the multi-tier caching layer test suite.

Kept intentionally minimal for C1; later commits extend this with real
Redis/Postgres fixtures and an in-process ASGI ``client`` fixture.
"""
from __future__ import annotations

import pytest

from src.settings import Settings


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def settings() -> Settings:
    """Return a fresh, non-cached Settings instance for tests."""
    return Settings()

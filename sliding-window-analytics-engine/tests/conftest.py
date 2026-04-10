"""Shared pytest fixtures for the sliding-window-analytics-engine test suite.

Commit 1 has no real unit tests yet (those arrive in Commit 2). The
`pytest_sessionfinish` hook below normalises pytest's "no tests collected"
exit code (5) to 0 so that `docker compose run --rm test pytest` returns a
successful status while the suite is still empty.
"""

from __future__ import annotations

import pytest


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit code 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


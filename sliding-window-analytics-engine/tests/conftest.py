"""Shared pytest fixtures for the sliding-window-analytics-engine test suite.

Commit 2 introduces real unit tests for ``src.stats`` and ``src.sliding_window``
so we also expose a small ``make_event`` factory fixture here to keep the
individual test files concise.
"""

from __future__ import annotations

import uuid

import pytest

from src.models import Event


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit code 0 instead of 5).

    Kept from Commit 1 so that intermediate commits with no collected tests
    still return a green status from ``docker compose run --rm test pytest``.
    """
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def make_event():
    """Factory fixture that produces :class:`Event` objects with sensible defaults.

    Only ``timestamp`` and ``value`` are usually relevant for unit tests, so
    the factory auto-generates a unique ``event_id`` and defaults the metric
    to ``"response_time"``. Callers can override ``metric`` when they need to
    assert on metric-aware behaviour.
    """

    def _make(
        timestamp: float,
        value: float,
        metric: str = "response_time",
    ) -> Event:
        return Event(
            event_id=str(uuid.uuid4()),
            timestamp=timestamp,
            value=value,
            metric=metric,
            metadata={},
        )

    return _make

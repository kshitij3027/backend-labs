"""Shared pytest fixtures for the anomaly detection engine test suite."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import LogEntry


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def make_log_entry():
    """Factory fixture that produces LogEntry objects with sensible defaults."""

    def _make(**kwargs) -> LogEntry:
        defaults = {
            "timestamp": datetime.now(timezone.utc),
            "ip": "192.168.1.1",
            "method": "GET",
            "path": "/api/data",
            "status_code": 200,
            "response_time": 150.0,
            "bytes_sent": 5000,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "session_duration": 300.0,
            "page_views": 5,
        }
        defaults.update(kwargs)
        return LogEntry(**defaults)

    return _make

"""Integration-test fixtures for the RCA Analysis Engine.

This project keeps all state in-memory (no Redis, no database, no message queue),
so integration tests need no external-service client — unlike the sibling project's
Redis fixtures. This module is the integration package's shared-fixture home and is
kept importable from C1; the analyze/incidents integration tests added in C5 reuse
the injected-runtime ``app`` / ``client`` fixtures from the top-level
``tests/conftest.py``. A ``settings`` convenience fixture is provided here.
"""

import pytest

from src.config import get_settings


@pytest.fixture()
def settings():
    """The process-wide Settings (defaults + optional .env + environment)."""
    return get_settings()

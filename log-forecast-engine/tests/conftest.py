"""Shared pytest fixtures for the Predictive Log Analytics Engine test suite.

C0 keeps fixtures intentionally light: a ``client`` backed by FastAPI's TestClient
over a freshly-built app (no DB/Redis needed for the dependency-free /health route).
Integration DB/Redis fixtures arrive in C1.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api import create_app


@pytest.fixture
def client() -> TestClient:
    """Return a TestClient wrapping a fresh app instance.

    A new app per test keeps cases isolated; the C0 app has no shared mutable state.
    """
    return TestClient(create_app())

"""Integration tests for the FastAPI app surface delivered in Commit 2.

These exercise the wired application end-to-end via FastAPI's ``TestClient``,
which is used as a context manager so the ``lifespan`` runs (and therefore the
Settings object graph is built on ``app.state``).
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app


def test_health():
    """GET /health returns 200 with exactly the healthy body."""
    with TestClient(app) as client:
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "healthy"}


def test_lifespan_sets_settings():
    """The lifespan stashes a Settings instance on app.state with defaults."""
    with TestClient(app) as client:  # noqa: F841 - context triggers lifespan
        assert app.state.settings is not None
        assert app.state.settings.api_port == 8000


def test_openapi_available():
    """The OpenAPI schema is served, confirming the app is fully wired."""
    with TestClient(app) as client:
        assert client.get("/openapi.json").status_code == 200

"""Integration test for the health endpoint over the full ASGI app.

Entering the TestClient context manager runs the app's lifespan (settings
resolution + logging setup), so this exercises the same startup path the
Docker HEALTHCHECK depends on — not just the route handler.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import app


def test_health_returns_healthy() -> None:
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

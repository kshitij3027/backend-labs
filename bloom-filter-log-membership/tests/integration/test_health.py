"""Integration test for the health endpoint over the full ASGI app.

The ``client`` fixture runs the app's lifespan (settings resolution, filter
manager build, snapshot reload, background tasks), so this exercises the
same startup path the Docker HEALTHCHECK depends on — not just the route
handler. Since C8 the lifespan also touches DATA_DIR (reload + final
snapshot), so the fixture's isolated environment is required: a bare
``TestClient(app)`` would write real snapshot files into ``./data``.
"""
from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_returns_healthy(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

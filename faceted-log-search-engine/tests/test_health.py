"""HTTP-level tests for the /health endpoint.

Uses httpx.ASGITransport to exercise the FastAPI app in-process. Because
httpx 0.28's ASGITransport does not auto-run the lifespan context, we
manually enter ``app.router.lifespan_context(app)`` inside the test so
startup opens/migrates the SQLite DB and shutdown closes it cleanly.

To isolate tests from the app's default ``/data/logs.db`` volume path,
we override ``settings.db_path`` to point at pytest's ``tmp_path`` for
the duration of the test.
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings
from src.main import app


async def test_health_returns_ok(tmp_path, monkeypatch):
    """GET /health returns 200 with status=ok, db=connected, redis_url set."""
    # Point the shared settings at a tmp sqlite file so the lifespan
    # startup creates and migrates a clean DB for this test.
    db_file = str(tmp_path / "health.db")
    monkeypatch.setattr(settings, "db_path", db_file)

    transport = ASGITransport(app=app)

    # Manually drive the lifespan so app.state.db is populated before the request.
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("status") == "ok"
    assert body.get("db") == "connected"
    # redis_url comes from settings; must be a non-empty string.
    assert isinstance(body.get("redis_url"), str) and body["redis_url"], (
        f"expected redis_url to be a non-empty string, got {body.get('redis_url')!r}"
    )

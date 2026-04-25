"""Unit tests for the dashboard HTML and static-file routes.

The Chrome MCP smoke tests live separately and are run by the main
thread — these tests just confirm the route plumbing is wired and the
template renders without falling over.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from src.main import app, reset_app_state


@pytest_asyncio.fixture(autouse=True)
async def _fresh_state():
    reset_app_state(app)
    yield


@pytest.mark.asyncio
async def test_dashboard_renders(async_client) -> None:
    """``GET /`` returns 200 with the search input present."""
    resp = await async_client.get("/")
    assert resp.status_code == 200
    body = resp.text
    assert 'id="search-input"' in body
    assert 'id="results"' in body
    assert 'id="suggestions"' in body
    assert 'id="seed-btn"' in body
    # Header text from context
    assert "Log Search" in body


@pytest.mark.asyncio
async def test_dashboard_static_css(async_client) -> None:
    """``GET /static/app.css`` serves the stylesheet (200, non-empty)."""
    resp = await async_client.get("/static/app.css")
    assert resp.status_code == 200
    assert "background" in resp.text   # sanity — it's CSS


@pytest.mark.asyncio
async def test_dashboard_static_js(async_client) -> None:
    """``GET /static/app.js`` serves the JS (200, non-empty)."""
    resp = await async_client.get("/static/app.js")
    assert resp.status_code == 200
    assert "fetch" in resp.text   # sanity — the JS uses fetch


@pytest.mark.asyncio
async def test_seed_endpoint_admits_500(async_client) -> None:
    resp = await async_client.post("/api/sample/seed?count=500")
    assert resp.status_code == 200
    body = resp.json()
    assert body["accepted"] == 500
    assert body["first_doc_id"] == 0
    assert body["last_doc_id"] == 499
    assert body["index_version"] >= 1


@pytest.mark.asyncio
async def test_seed_endpoint_oversize_rejected(async_client) -> None:
    resp = await async_client.post("/api/sample/seed?count=99999")
    assert resp.status_code == 422

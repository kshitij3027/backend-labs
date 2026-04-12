"""Tests for the session query API endpoints."""
from __future__ import annotations

import os

os.environ["DISABLE_SIMULATOR"] = "1"

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_sessions_for_user(client: AsyncClient):
    """GET /api/sessions/{user_id} returns sessions after events are posted."""
    # Create events for the user
    await client.post("/api/events", json={
        "user_id": "session_query_user",
        "event_type": "page_view",
        "page_url": "/home",
    })
    await client.post("/api/events", json={
        "user_id": "session_query_user",
        "event_type": "click",
        "page_url": "/products",
    })

    resp = await client.get("/api/sessions/session_query_user")
    assert resp.status_code == 200
    sessions = resp.json()
    assert isinstance(sessions, list)
    assert len(sessions) > 0
    # Each session summary should have expected keys
    first = sessions[0]
    assert "session_id" in first
    assert "user_id" in first
    assert first["user_id"] == "session_query_user"
    assert "state" in first
    assert "event_count" in first


@pytest.mark.asyncio
async def test_get_specific_session(client: AsyncClient):
    """GET /api/sessions/{user_id}/{session_id} returns full session detail."""
    # Create an event and capture the session_id
    resp = await client.post("/api/events", json={
        "user_id": "detail_user",
        "event_type": "page_view",
        "page_url": "/home",
    })
    assert resp.status_code == 200
    session_id = resp.json()["session_id"]

    # Fetch full session detail
    detail_resp = await client.get(f"/api/sessions/detail_user/{session_id}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["session_id"] == session_id
    assert detail["user_id"] == "detail_user"
    assert "events" in detail
    assert "pages_visited" in detail


@pytest.mark.asyncio
async def test_get_sessions_unknown_user(client: AsyncClient):
    """GET /api/sessions/{unknown_user} returns empty list."""
    resp = await client.get("/api/sessions/nonexistent_user_xyz")
    assert resp.status_code == 200
    sessions = resp.json()
    assert isinstance(sessions, list)
    assert len(sessions) == 0

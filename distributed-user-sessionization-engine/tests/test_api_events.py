"""Tests for the event ingestion API endpoint."""
from __future__ import annotations

import os

os.environ["DISABLE_SIMULATOR"] = "1"

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_post_single_event(client: AsyncClient):
    """POST /api/events with a valid single event returns success."""
    payload = {
        "user_id": "test_user_1",
        "event_type": "page_view",
        "page_url": "/home",
        "device_type": "desktop",
    }
    resp = await client.post("/api/events", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert "session_id" in body
    assert "analysis" in body
    assert "quality_score" in body["analysis"]
    assert "engagement" in body["analysis"]


@pytest.mark.asyncio
async def test_post_batch_events(client: AsyncClient):
    """POST /api/events with a list of events returns batch response."""
    payload = [
        {"user_id": "batch_user_1", "event_type": "page_view", "page_url": "/a"},
        {"user_id": "batch_user_2", "event_type": "click", "page_url": "/b"},
        {"user_id": "batch_user_3", "event_type": "search", "page_url": "/c"},
    ]
    resp = await client.post("/api/events", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["processed"] == 3


@pytest.mark.asyncio
async def test_post_invalid_event(client: AsyncClient):
    """POST /api/events with empty dict (missing user_id) returns 422."""
    resp = await client.post("/api/events", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_post_force_end_event(client: AsyncClient):
    """A logout event ends the current session, so the next event gets a new session."""
    # First event: page_view
    resp1 = await client.post("/api/events", json={
        "user_id": "force_end_user",
        "event_type": "page_view",
        "page_url": "/home",
    })
    assert resp1.status_code == 200
    session_id_1 = resp1.json()["session_id"]

    # Second event: logout (force-end boundary)
    resp2 = await client.post("/api/events", json={
        "user_id": "force_end_user",
        "event_type": "logout",
        "page_url": "/logout",
    })
    assert resp2.status_code == 200
    session_id_2 = resp2.json()["session_id"]

    # Logout creates a new session (force-end closes the previous one first)
    assert session_id_1 != session_id_2

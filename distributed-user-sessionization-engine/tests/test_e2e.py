"""Comprehensive end-to-end tests for the Distributed User Sessionization Engine."""
import os

os.environ["DISABLE_SIMULATOR"] = "1"

import pytest
import asyncio


@pytest.mark.asyncio
async def test_full_event_lifecycle(client):
    """POST event -> session created -> GET session -> analytics reflect it."""
    # POST single event
    resp = await client.post("/api/events", json={
        "user_id": "e2e_lifecycle", "event_type": "page_view",
        "page_url": "/products", "device_type": "desktop"
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    session_id = data["session_id"]

    # GET session for user
    resp = await client.get("/api/sessions/e2e_lifecycle")
    assert resp.status_code == 200
    sessions = resp.json()
    assert len(sessions) >= 1
    assert any(s["session_id"] == session_id for s in sessions)

    # GET analytics should reflect the event
    resp = await client.get("/api/analytics")
    assert resp.status_code == 200
    analytics = resp.json()
    assert analytics["active_sessions"] >= 1
    assert analytics["total_events"] >= 1


@pytest.mark.asyncio
async def test_batch_ingestion(client):
    """POST batch of events, verify all processed."""
    events = [
        {"user_id": f"batch_user_{i}", "event_type": "page_view", "page_url": f"/page{i}", "device_type": "desktop"}
        for i in range(10)
    ]
    resp = await client.post("/api/events", json=events)
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["processed"] == 10


@pytest.mark.asyncio
async def test_concurrent_users(client):
    """POST events for multiple users, verify separate sessions created."""
    user_ids = [f"concurrent_{i}" for i in range(5)]
    session_ids = set()
    for uid in user_ids:
        resp = await client.post("/api/events", json={
            "user_id": uid, "event_type": "page_view", "page_url": "/home", "device_type": "desktop"
        })
        data = resp.json()
        session_ids.add(data["session_id"])

    # All 5 users should have different sessions
    assert len(session_ids) == 5


@pytest.mark.asyncio
async def test_dashboard_html_serves(client):
    """GET / returns valid HTML with Chart.js and WebSocket code."""
    resp = await client.get("/")
    assert resp.status_code == 200
    html = resp.text
    assert "chart.js" in html.lower()
    assert "ws/dashboard" in html
    assert "session_update" in html


@pytest.mark.asyncio
async def test_analytics_complete_shape(client):
    """Verify analytics returns all fields including behavioral analytics."""
    # Post some diverse events first
    events = [
        {"user_id": "shape_user", "event_type": "page_view", "page_url": "/home", "device_type": "desktop"},
        {"user_id": "shape_user", "event_type": "search", "page_url": "/search", "device_type": "desktop"},
        {"user_id": "shape_user", "event_type": "add_to_cart", "page_url": "/cart", "device_type": "desktop"},
    ]
    for e in events:
        await client.post("/api/events", json=e)

    resp = await client.get("/api/analytics")
    data = resp.json()

    # Core fields
    assert "active_sessions" in data
    assert "avg_duration" in data
    assert "device_breakdown" in data
    assert "engagement_distribution" in data
    assert "total_events" in data

    # Extended behavioral fields
    assert "session_type_breakdown" in data
    assert "funnel_conversion_rates" in data
    assert "anomaly_distribution" in data

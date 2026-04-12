"""Tests for the analytics API endpoint."""
from __future__ import annotations

import os

os.environ["DISABLE_SIMULATOR"] = "1"

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_analytics_empty(client: AsyncClient):
    """GET /api/analytics before any events returns correct shape with zeros."""
    resp = await client.get("/api/analytics")
    assert resp.status_code == 200
    body = resp.json()
    assert "active_sessions" in body
    assert "avg_duration" in body
    assert "device_breakdown" in body
    assert "engagement_distribution" in body
    assert "total_events" in body
    # Engagement distribution should always have these keys
    eng = body["engagement_distribution"]
    assert "bounce" in eng
    assert "low" in eng
    assert "moderate" in eng
    assert "high" in eng


@pytest.mark.asyncio
async def test_analytics_after_events(client: AsyncClient):
    """GET /api/analytics after posting events shows active sessions and events."""
    # Post several events for different users
    for i in range(5):
        await client.post("/api/events", json={
            "user_id": f"analytics_user_{i}",
            "event_type": "page_view",
            "page_url": f"/page_{i}",
        })

    resp = await client.get("/api/analytics")
    assert resp.status_code == 200
    body = resp.json()
    assert body["active_sessions"] > 0
    assert body["total_events"] > 0


@pytest.mark.asyncio
async def test_analytics_device_breakdown(client: AsyncClient):
    """Device breakdown reflects device types from posted events."""
    await client.post("/api/events", json={
        "user_id": "device_user_desktop",
        "event_type": "page_view",
        "device_type": "desktop",
    })
    await client.post("/api/events", json={
        "user_id": "device_user_mobile",
        "event_type": "page_view",
        "device_type": "mobile",
    })

    resp = await client.get("/api/analytics")
    assert resp.status_code == 200
    breakdown = resp.json()["device_breakdown"]
    assert isinstance(breakdown, dict)
    assert len(breakdown) > 0


@pytest.mark.asyncio
async def test_analytics_engagement_distribution(client: AsyncClient):
    """Engagement distribution has bounce/low/moderate/high keys."""
    # Post an event to ensure non-empty analytics
    await client.post("/api/events", json={
        "user_id": "engagement_user",
        "event_type": "page_view",
        "page_url": "/home",
    })

    resp = await client.get("/api/analytics")
    assert resp.status_code == 200
    eng = resp.json()["engagement_distribution"]
    assert "bounce" in eng
    assert "low" in eng
    assert "moderate" in eng
    assert "high" in eng

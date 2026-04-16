"""Tests for the FastAPI health endpoint."""

import pytest


@pytest.mark.asyncio
async def test_health_endpoint(client):
    """GET /health returns 200 with healthy status and index counters."""
    response = await client.get("/health")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert "documents" in data
    assert "terms" in data

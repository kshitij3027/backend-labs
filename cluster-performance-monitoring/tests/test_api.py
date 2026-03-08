"""Tests for the FastAPI application endpoints."""

from __future__ import annotations

import asyncio
import os

import pytest
import httpx
from httpx import ASGITransport

# Set fast collection interval before importing the app so Config.load() picks it up
os.environ["COLLECTION_INTERVAL"] = "0.5"

from src import server  # noqa: E402
from src.server import app, lifespan  # noqa: E402


@pytest.fixture
async def client():
    """Create an async test client for the FastAPI app.

    Manually invokes the lifespan context manager so that simulators,
    collectors, aggregator, analyzer, and reporter are initialised before
    any requests are made.  Waits briefly for metric data to accumulate.
    """
    # Reset module-level globals so the lifespan starts fresh
    server.config = None  # type: ignore[assignment]
    server.store = None  # type: ignore[assignment]
    server.simulators = []
    server.collectors = []
    server.aggregator = None  # type: ignore[assignment]
    server.analyzer = None  # type: ignore[assignment]
    server.reporter = None  # type: ignore[assignment]

    async with lifespan(app):
        transport = ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            # Wait a moment for collectors to gather some data
            await asyncio.sleep(1.5)
            yield ac


async def test_health(client: httpx.AsyncClient):
    """GET /health returns 200 with status ok."""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "nodes" in data
    assert data["nodes"] == 3


async def test_get_metrics(client: httpx.AsyncClient):
    """GET /api/metrics returns 200 with cluster-wide metric totals."""
    response = await client.get("/api/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "avg_cpu_usage" in data
    assert "avg_memory_usage" in data
    assert "total_throughput" in data
    assert "active_nodes" in data


async def test_get_nodes(client: httpx.AsyncClient):
    """GET /api/nodes returns 200 with a list of 3 nodes."""
    response = await client.get("/api/nodes")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3
    # Check node structure
    for node in data:
        assert "node_id" in node
        assert "role" in node
        assert "host" in node
        assert "port" in node


async def test_get_node_metrics(client: httpx.AsyncClient):
    """GET /api/nodes/node-1/metrics returns 200."""
    response = await client.get("/api/nodes/node-1/metrics")
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == "node-1"
    assert "metrics" in data


async def test_get_alerts(client: httpx.AsyncClient):
    """GET /api/alerts returns 200 with alerts and count keys."""
    response = await client.get("/api/alerts")
    assert response.status_code == 200
    data = response.json()
    assert "alerts" in data
    assert "count" in data
    assert isinstance(data["alerts"], list)
    assert isinstance(data["count"], int)


async def test_get_report_none(client: httpx.AsyncClient):
    """GET /api/report returns 200 (either report or no-reports message)."""
    response = await client.get("/api/report")
    assert response.status_code == 200
    data = response.json()
    # Either a report with report_id or a message saying none yet
    assert "report_id" in data or "message" in data


async def test_generate_report(client: httpx.AsyncClient):
    """POST /api/report/generate returns 200 with a report_id field."""
    response = await client.post("/api/report/generate")
    assert response.status_code == 200
    data = response.json()
    assert "report_id" in data
    assert data["report_id"].startswith("perf_report_")

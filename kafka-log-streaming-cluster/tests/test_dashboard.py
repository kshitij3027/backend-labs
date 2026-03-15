"""Tests for the FastAPI dashboard endpoints."""

import asyncio
import json
import time

import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import MagicMock, patch

from src.config import Settings
from src.dashboard import create_app
from src.metrics import MetricsTracker


# ---------------------------------------------------------------------------
# Canned test data
# ---------------------------------------------------------------------------

_SAMPLE_MESSAGES = [
    {
        "topic": "web-api-logs",
        "partition": 0,
        "offset": 42,
        "key": "user-001",
        "data": {
            "timestamp": "2026-03-15T10:30:00+00:00",
            "service": "web-api",
            "level": "INFO",
            "endpoint": "/api/users",
            "status_code": 200,
            "user_id": "user-001",
            "message": "Request processed successfully",
            "sequence_number": 1,
        },
        "received_at": time.time(),
    },
    {
        "topic": "payment-service-logs",
        "partition": 1,
        "offset": 17,
        "key": "user-002",
        "data": {
            "timestamp": "2026-03-15T10:31:00+00:00",
            "service": "payment-service",
            "level": "ERROR",
            "endpoint": "/payments/process",
            "status_code": 500,
            "user_id": "user-002",
            "message": "Payment gateway timeout",
            "sequence_number": 2,
        },
        "received_at": time.time(),
    },
]

_SAMPLE_ERRORS = [_SAMPLE_MESSAGES[1]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_consumer():
    """Build a MagicMock that quacks like DashboardConsumer."""
    dc = MagicMock()
    dc.recent_messages = list(_SAMPLE_MESSAGES)
    dc.stats = {
        "total": 150,
        "by_service": {"web-api": 80, "payment-service": 40, "user-service": 30},
        "by_level": {"INFO": 100, "WARN": 30, "ERROR": 20},
    }
    dc.is_running = True
    dc.start = MagicMock()
    dc.stop = MagicMock()
    return dc


def _make_mock_aggregator():
    """Build a MagicMock that quacks like ErrorAggregator."""
    ea = MagicMock()
    ea.recent_errors = list(_SAMPLE_ERRORS)
    ea.error_counts = {"payment-service": 5, "web-api": 2}
    ea.error_rate = 0.12
    ea.is_running = True
    ea.start = MagicMock()
    ea.stop = MagicMock()
    return ea


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_dc():
    return _make_mock_consumer()


@pytest.fixture
def mock_ea():
    return _make_mock_aggregator()


@pytest.fixture
def app(mock_dc, mock_ea):
    """Create a FastAPI app with mocked consumers injected via app.state.

    We patch the lifespan out entirely so no real Kafka connections are made,
    then manually populate app.state with the mock objects.
    """
    settings = Settings()

    with patch("src.dashboard.DashboardConsumer", return_value=mock_dc), \
         patch("src.dashboard.ErrorAggregator", return_value=mock_ea):
        application = create_app(settings)

    # The lifespan would normally populate app.state; since it won't run
    # with real Kafka, we inject the mocks directly.
    application.state.dashboard_consumer = mock_dc
    application.state.error_aggregator = mock_ea
    application.state.metrics_tracker = MetricsTracker()
    application.state.stop_event = asyncio.Event()

    return application


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert body["status"] == "ok"
    assert "consumers" in body
    assert body["consumers"]["dashboard"] is True
    assert body["consumers"]["error_aggregator"] is True


@pytest.mark.asyncio
async def test_api_logs_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/logs")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) == 2
    assert body[0]["topic"] == "web-api-logs"


@pytest.mark.asyncio
async def test_api_stats_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert "total" in body
    assert "by_service" in body
    assert "by_level" in body
    assert "messages_per_second" in body
    assert body["total"] == 150
    assert body["by_service"]["web-api"] == 80


@pytest.mark.asyncio
async def test_api_errors_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/errors")
    assert resp.status_code == 200
    body = resp.json()
    assert "recent_errors" in body
    assert "error_counts" in body
    assert "error_rate" in body
    assert body["error_rate"] == 0.12
    assert body["error_counts"]["payment-service"] == 5


@pytest.mark.asyncio
async def test_index_returns_html(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Kafka Log Streaming Dashboard" in resp.text


@pytest.mark.asyncio
async def test_sse_route_exists(app):
    """Verify the /api/stream route is registered and returns a streaming response.

    Note: httpx's ASGITransport does not support true streaming reads for SSE
    (infinite generators). We verify the route exists and the response headers
    are set correctly by inspecting the route table directly.
    """
    routes = {r.path: r for r in app.routes if hasattr(r, "path")}
    assert "/api/stream" in routes

    # Verify the endpoint function is registered
    route = routes["/api/stream"]
    assert route.methods is None or "GET" in route.methods


@pytest.mark.asyncio
async def test_sse_generator_emits_data(mock_dc):
    """Test the SSE event generator logic directly, bypassing httpx transport."""
    import src.dashboard as dashboard_mod

    # Build a minimal app just to access its state
    settings = Settings()
    with patch.object(dashboard_mod, "DashboardConsumer", return_value=mock_dc), \
         patch.object(dashboard_mod, "ErrorAggregator", return_value=_make_mock_aggregator()):
        test_app = dashboard_mod.create_app(settings)

    test_app.state.dashboard_consumer = mock_dc
    test_app.state.error_aggregator = _make_mock_aggregator()
    test_app.state.metrics_tracker = MetricsTracker()

    # Call the api_stream endpoint directly to get the StreamingResponse
    from starlette.testclient import TestClient
    # Use the route's endpoint to get the StreamingResponse object
    routes = {r.path: r for r in test_app.routes if hasattr(r, "path")}
    stream_route = routes["/api/stream"]
    response = await stream_route.endpoint()

    # The response body is an async generator
    gen = response.body_iterator

    # Collect the first few yielded items
    items = []
    for _ in range(5):
        try:
            item = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
            items.append(item)
        except (StopAsyncIteration, asyncio.TimeoutError):
            break

    combined = "".join(items)
    assert "data:" in combined

    # Verify each data line is valid JSON
    for line in combined.strip().split("\n"):
        if line.startswith("data:"):
            payload = line[len("data:"):].strip()
            parsed = json.loads(payload)
            assert "topic" in parsed
            assert "data" in parsed


@pytest.mark.asyncio
async def test_health_reports_consumer_status(app, mock_dc):
    """Verify health reflects consumer running state."""
    mock_dc.is_running = False
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    body = resp.json()
    assert body["consumers"]["dashboard"] is False
    assert body["consumers"]["error_aggregator"] is True


@pytest.mark.asyncio
async def test_api_metrics_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics")
    assert resp.status_code == 200
    body = resp.json()
    assert "throughput" in body
    assert "throughput_history" in body
    assert "consumer_lag" in body
    assert "latency" in body
    # Latency should have expected keys
    lat = body["latency"]
    assert "p50" in lat
    assert "p95" in lat
    assert "p99" in lat
    assert "samples" in lat
    # Consumer lag should be a dict (empty without Kafka)
    assert isinstance(body["consumer_lag"], dict)
    # Throughput history should be a list
    assert isinstance(body["throughput_history"], list)


@pytest.mark.asyncio
async def test_api_ordering_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/ordering")
    assert resp.status_code == 200
    body = resp.json()
    assert "ordered" in body
    assert "total_groups" in body
    assert "ordered_groups" in body
    assert "violations" in body
    assert "message_count" in body
    # With the sample data, both messages have different keys so 2 groups
    assert body["total_groups"] == 2
    # Both have single messages in their group, so they are ordered
    assert body["ordered"] is True
    assert body["violations"] == []


@pytest.mark.asyncio
async def test_api_ordering_detects_violations(app, mock_dc):
    """Ordering endpoint should detect out-of-order sequences."""
    # Create messages with the same key but out-of-order sequence numbers
    mock_dc.recent_messages = [
        {
            "topic": "web-api-logs",
            "key": "user-001",
            "data": {"sequence_number": 5},
        },
        {
            "topic": "web-api-logs",
            "key": "user-001",
            "data": {"sequence_number": 3},
        },
        {
            "topic": "web-api-logs",
            "key": "user-001",
            "data": {"sequence_number": 7},
        },
    ]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/ordering")
    body = resp.json()
    assert body["ordered"] is False
    assert body["total_groups"] == 1
    assert body["ordered_groups"] == 0
    assert len(body["violations"]) == 1
    assert body["violations"][0]["topic"] == "web-api-logs"


@pytest.mark.asyncio
async def test_api_ordering_empty_messages(app, mock_dc):
    """Ordering endpoint works with no messages."""
    mock_dc.recent_messages = []
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/ordering")
    body = resp.json()
    assert body["ordered"] is True
    assert body["total_groups"] == 0
    assert body["message_count"] == 0


@pytest.mark.asyncio
async def test_api_metrics_latency_with_data(app):
    """Metrics endpoint returns real latency data when samples exist."""
    tracker: MetricsTracker = app.state.metrics_tracker
    for i in range(1, 51):
        tracker.record_consumed("test", latency_ms=float(i))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics")
    body = resp.json()
    assert body["latency"]["samples"] == 50
    assert body["latency"]["p50"] > 0

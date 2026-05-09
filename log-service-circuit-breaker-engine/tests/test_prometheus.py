"""Tests for /metrics Prometheus endpoint."""
import pytest
from fastapi.testclient import TestClient
from src.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def test_metrics_endpoint_returns_200_text(client):
    """GET /metrics returns 200 and a text/plain content-type."""
    r = client.get("/metrics")
    assert r.status_code == 200
    content_type = r.headers.get("content-type", "")
    assert content_type.startswith("text/plain")


def test_metrics_contains_breaker_gauges(client):
    """The body contains the expected gauge names and breaker labels."""
    r = client.get("/metrics")
    assert r.status_code == 200
    body = r.content
    assert b"circuit_breaker_state" in body
    assert b"circuit_breaker_success_rate" in body
    for name in (b"database_primary", b"database_backup", b"queue_main", b"external_api"):
        assert name in body


def test_metrics_state_zero_for_closed(client):
    """Each breaker starts CLOSED, so its state gauge should read 0.0."""
    r = client.get("/metrics")
    assert r.status_code == 200
    text = r.text
    assert 'circuit_breaker_state{name="database_primary"} 0.0' in text

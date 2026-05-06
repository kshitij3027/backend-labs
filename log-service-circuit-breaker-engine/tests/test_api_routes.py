"""Tests for the REST GET endpoints."""
import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def test_health_returns_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["uptime_seconds"] >= 0


def test_metrics_returns_4_circuits(client):
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    body = resp.json()
    circuits = body["circuits"]
    assert isinstance(circuits, dict)
    for name in ("database_primary", "database_backup", "queue_main", "external_api"):
        assert name in circuits, f"missing circuit {name}"
        assert circuits[name]["state"] == "CLOSED"


def test_metrics_processing_initial_zeroes(client):
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    body = resp.json()
    processing = body["processing"]
    assert processing["total_processed"] == 0
    assert processing["successful_processed"] == 0
    assert processing["fallback_responses"] == 0


def test_metrics_history_initially_empty(client):
    resp = client.get("/api/metrics/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"history": []}


def test_root_returns_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "Circuit Breaker" in resp.text

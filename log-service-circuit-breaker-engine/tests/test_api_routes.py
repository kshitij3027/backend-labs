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


def test_metrics_history_endpoint_shape(client):
    # The broadcaster (Commit 12) appends a snapshot on its first iteration,
    # so the buffer may have 0+ entries by the time this test reads it.
    # We just verify the endpoint shape is correct.
    resp = client.get("/api/metrics/history")
    assert resp.status_code == 200
    body = resp.json()
    assert "history" in body
    assert isinstance(body["history"], list)


def test_root_returns_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "Circuit Breaker" in resp.text


def test_process_logs_returns_summary(client):
    r = client.post("/api/process/logs", json={"count": 5})
    assert r.status_code == 200
    body = r.json()
    assert body["processed"] == 5
    assert body["successful"] >= 0
    assert "duration_ms" in body
    # Verify metrics reflect the batch
    metrics = client.get("/api/metrics").json()
    assert metrics["processing"]["total_processed"] == 5


def test_process_logs_validates_count(client):
    # count must be >= 1
    r = client.post("/api/process/logs", json={"count": 0})
    assert r.status_code == 422


def test_simulate_failures_unknown_target(client):
    r = client.post("/api/simulate/failures", json={"target": "nope", "duration": 1, "failure_rate": 0.5})
    assert r.status_code == 400


def test_simulate_failures_accepts_known_target(client):
    r = client.post(
        "/api/simulate/failures",
        json={"target": "database_primary", "duration": 1, "failure_rate": 0.5},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["simulating"] == "database_primary"
    assert body["duration"] == 1


def test_reset_breakers_returns_circuit_names(client):
    r = client.post("/api/reset")
    assert r.status_code == 200
    body = r.json()
    assert body["reset"] is True
    assert "database_primary" in body["circuits"]
    assert "database_backup" in body["circuits"]
    assert "queue_main" in body["circuits"]
    assert "external_api" in body["circuits"]

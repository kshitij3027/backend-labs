"""Unit tests for the health endpoint.

The C0 endpoint returned a flat 3-key payload; C11 enhanced ``GET /health`` into a
degraded-safe report (deployed-model count, per-subsystem booleans, performance
snapshot) that always returns HTTP 200. These tests assert the enhanced contract's
*stable* fields without pinning the volatile performance numbers. They run in the
profile-gated `test` container against the real Postgres/Redis services.
"""

from __future__ import annotations


def test_health_returns_200(client) -> None:
    """GET /health responds with HTTP 200 (never 500)."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_payload(client) -> None:
    """GET /health returns the enhanced C11 status/subsystems/performance JSON."""
    body = client.get("/health").json()
    assert body["status"] in {"ok", "degraded"}
    assert body["service"] == "log-forecast-engine"
    assert body["version"] == "0.1.0"
    assert isinstance(body["deployed_models"], int)
    assert set(body["subsystems"]) == {"database", "redis"}
    assert "rss_mb" in body["performance"]
    assert "uptime_seconds" in body["performance"]

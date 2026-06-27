"""Unit tests for the C0 health endpoint.

These are dependency-free (no Postgres/Redis): they only exercise the FastAPI app's
``GET /health`` route, so they pass in the profile-gated `test` container without any
running services.
"""

from __future__ import annotations


def test_health_returns_200(client) -> None:
    """GET /health responds with HTTP 200."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_payload(client) -> None:
    """GET /health returns the expected status/service/version JSON."""
    response = client.get("/health")
    assert response.json() == {
        "status": "ok",
        "service": "log-forecast-engine",
        "version": "0.1.0",
    }

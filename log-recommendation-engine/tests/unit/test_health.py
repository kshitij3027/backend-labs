"""Unit tests for the health endpoint.

The C1 endpoint is a dependency-free liveness probe returning a flat 3-key payload
(``status`` / ``service`` / ``version``). A deep readiness ``/health`` with
per-subsystem booleans arrives in C13; these tests assert only the C1 contract and
run in the profile-gated `test` container.
"""

from __future__ import annotations


def test_health_returns_200(client) -> None:
    """GET /health responds with HTTP 200 (never 500)."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_payload(client) -> None:
    """GET /health returns the C1 status/service/version JSON."""
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["service"] == "log-recommendation-engine"
    assert body["version"] == "0.1.0"

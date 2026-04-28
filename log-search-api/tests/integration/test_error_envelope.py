from __future__ import annotations

import os

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("API_URL"),
    reason="API_URL not set; integration test requires a live API",
)


@pytest.fixture(scope="module")
def base_url() -> str:
    return os.environ["API_URL"].rstrip("/")


@pytest.fixture(scope="module")
def http_client(base_url: str) -> httpx.Client:
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        yield client


def test_validation_error_returns_envelope_with_field_suggestions(
    http_client: httpx.Client,
) -> None:
    response = http_client.post(
        "/api/v1/auth/token",
        data={},
    )
    assert response.status_code == 422, response.text

    request_id_header = response.headers.get("X-Request-ID")
    assert request_id_header, "X-Request-ID header missing on 422 response"

    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert isinstance(body["error"]["message"], str)
    assert isinstance(body["error"]["suggestions"], list)
    assert len(body["error"]["suggestions"]) >= 1

    assert "details" in body
    assert isinstance(body["details"], list)
    assert len(body["details"]) >= 1
    for entry in body["details"]:
        assert "field" in entry
        assert "message" in entry

    assert body.get("request_id") == request_id_header


@pytest.mark.skipif(
    os.getenv("DEBUG_ENDPOINTS_ENABLED", "false").lower() != "true",
    reason="DEBUG_ENDPOINTS_ENABLED must be true to exercise the boom route",
)
def test_unhandled_exception_returns_safe_500_envelope(
    http_client: httpx.Client,
) -> None:
    response = http_client.get("/api/v1/_debug/boom")
    assert response.status_code == 500, response.text

    request_id_header = response.headers.get("X-Request-ID")
    assert request_id_header, "X-Request-ID header missing on 500 response"

    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "INTERNAL_ERROR"
    assert body["error"]["message"] == "unexpected server error"
    assert isinstance(body["error"]["suggestions"], list)
    assert len(body["error"]["suggestions"]) >= 1

    raw = response.text
    assert "boom for tests" not in raw
    assert "RuntimeError" not in raw
    assert "Traceback" not in raw

    assert body.get("request_id") == request_id_header

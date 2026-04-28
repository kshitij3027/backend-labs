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
def origin() -> str:
    return os.getenv("TEST_CORS_ORIGIN", "http://localhost:3000")


@pytest.fixture(scope="module")
def http_client(base_url: str) -> httpx.Client:
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        yield client


def test_preflight_request_returns_cors_headers(
    http_client: httpx.Client, origin: str
) -> None:
    response = http_client.request(
        "OPTIONS",
        "/api/v1/auth/token",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )
    assert response.status_code in (200, 204), response.text

    assert response.headers.get("access-control-allow-origin") == origin
    allow_methods = response.headers.get("access-control-allow-methods", "")
    assert "POST" in allow_methods or "*" in allow_methods
    allow_headers = response.headers.get("access-control-allow-headers", "")
    assert "content-type" in allow_headers.lower() or "*" in allow_headers
    assert response.headers.get("access-control-allow-credentials") == "true"


def test_simple_get_with_origin_returns_allow_origin(
    http_client: httpx.Client, origin: str
) -> None:
    response = http_client.get(
        "/api/v1/health",
        headers={"Origin": origin},
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == origin
    assert response.headers.get("access-control-allow-credentials") == "true"

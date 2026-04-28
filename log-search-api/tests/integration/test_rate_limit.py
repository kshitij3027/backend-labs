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
def credentials() -> tuple[str, str]:
    return (
        os.getenv("TEST_USERNAME", "demo"),
        os.getenv("TEST_PASSWORD", "demo"),
    )


@pytest.fixture(scope="module")
def http_client(base_url: str) -> httpx.Client:
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="module")
def rate_limit_threshold() -> int:
    return int(os.getenv("RATE_LIMIT_REQUESTS", "100"))


@pytest.fixture(scope="module")
def access_token(http_client: httpx.Client, credentials: tuple[str, str]) -> str:
    username, password = credentials
    response = http_client.post(
        "/api/v1/auth/token",
        data={"username": username, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def test_rate_limit_triggers_429_with_envelope_and_headers(
    http_client: httpx.Client,
    access_token: str,
    rate_limit_threshold: int,
) -> None:
    headers = {"Authorization": f"Bearer {access_token}"}
    last_response: httpx.Response | None = None
    triggered_at: int | None = None
    total = rate_limit_threshold + 5

    for i in range(total):
        last_response = http_client.get("/api/v1/auth/me", headers=headers)
        if last_response.status_code == 429:
            triggered_at = i + 1
            break

    assert last_response is not None
    assert triggered_at is not None, (
        f"expected a 429 within {total} requests; "
        f"last status={last_response.status_code if last_response else 'n/a'}"
    )
    assert last_response.status_code == 429

    assert "X-RateLimit-Limit" in last_response.headers
    assert "X-RateLimit-Remaining" in last_response.headers
    assert "Retry-After" in last_response.headers

    body = last_response.json()
    assert "error" in body
    assert body["error"]["code"] == "RATE_LIMITED"
    assert "request_id" in body


def test_health_endpoints_are_exempt_from_rate_limit(
    http_client: httpx.Client,
    rate_limit_threshold: int,
) -> None:
    burst = max(rate_limit_threshold + 20, 50)
    statuses: list[int] = []
    for _ in range(burst):
        response = http_client.get("/api/v1/health")
        statuses.append(response.status_code)

    bad = [s for s in statuses if s != 200]
    assert not bad, f"expected all /health requests to be 200; got non-200: {bad[:5]}"

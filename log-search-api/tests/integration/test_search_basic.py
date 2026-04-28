from __future__ import annotations

import os
import random
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("API_URL"),
    reason="API_URL not set; integration test requires a live API",
)


_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
_SERVICES = [
    "payment-service",
    "auth-service",
    "order-service",
    "notification-service",
]
_MESSAGES = [
    "transaction failed for user 42 with code TXN-500",
    "payment processed successfully for order 9001",
    "auth token expired for session sid-2025",
    "order created for customer cust-7",
    "notification dispatch error retry attempt 3",
    "ERROR: database connection refused on port 5432",
    "WARN slow query detected on logs index",
    "info: heartbeat ok from worker-3",
    "critical disk usage above 95 percent",
    "payment refund issued for order 9100",
]


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
    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        yield client


def _fetch_token(client: httpx.Client, username: str, password: str) -> str:
    response = client.post(
        "/api/v1/auth/token",
        data={"username": username, "password": password},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    token = body.get("access_token")
    assert isinstance(token, str) and token
    return token


@pytest.fixture(scope="module")
def auth_headers(http_client: httpx.Client, credentials: tuple[str, str]) -> dict[str, str]:
    username, password = credentials
    token = _fetch_token(http_client, username, password)
    return {"Authorization": f"Bearer {token}"}


def _build_seed_entries(prefix: str, count: int = 50) -> list[dict[str, object]]:
    rng = random.Random(13)
    now = datetime.now(UTC)
    entries: list[dict[str, object]] = []
    for i in range(count):
        offset = timedelta(minutes=rng.randint(0, 60 * 6))
        ts = now - offset
        entries.append(
            {
                "id": f"{prefix}-{int(now.timestamp())}-{i:04d}",
                "timestamp": ts.isoformat(),
                "level": rng.choice(_LEVELS),
                "service_name": rng.choice(_SERVICES),
                "message": rng.choice(_MESSAGES),
                "content": {"i": i, "trace": f"t-{i}"},
            }
        )
    return entries


@pytest.fixture(scope="module", autouse=True)
def seed_data(http_client: httpx.Client, auth_headers: dict[str, str]) -> None:
    entries = _build_seed_entries(prefix="search-basic", count=50)
    response = http_client.post(
        "/api/v1/logs/bulk",
        json={"entries": entries},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("created", 0) >= 1
    time.sleep(6)


def test_search_text_error_returns_results(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"q": "error", "limit": 25},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["total_hits"] >= 1
    assert isinstance(body["results"], list)
    assert body["execution_time_ms"] > 0

    scores = [r["score"] for r in body["results"] if r.get("score") is not None]
    assert scores == sorted(scores, reverse=True)

    aggs = body["aggregations"]
    assert isinstance(aggs["levels"], list)
    assert isinstance(aggs["services"], list)
    assert isinstance(aggs["timeline"], list)


def test_search_text_payment_returns_payment_message(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"q": "payment", "limit": 25},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["total_hits"] >= 1
    found = any("payment" in r["message"].lower() for r in body["results"])
    assert found, body["results"]


def test_search_get_post_equivalence(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    post_response = http_client.post(
        "/api/v1/logs/search",
        json={"q": "error", "limit": 10, "offset": 0},
        headers=auth_headers,
    )
    assert post_response.status_code == 200, post_response.text

    get_response = http_client.get(
        "/api/v1/logs/search",
        params={"q": "error", "limit": 10, "offset": 0},
        headers=auth_headers,
    )
    assert get_response.status_code == 200, get_response.text

    assert post_response.json()["total_hits"] == get_response.json()["total_hits"]


def test_search_requires_authentication(http_client: httpx.Client) -> None:
    response = http_client.post("/api/v1/logs/search", json={"q": "error"})
    assert response.status_code == 401, response.text

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
    rng = random.Random(99)
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
    entries = _build_seed_entries(prefix="search-filt", count=50)
    response = http_client.post(
        "/api/v1/logs/bulk",
        json={"entries": entries},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("created", 0) >= 1
    time.sleep(6)


def test_level_filter_returns_only_matching_level(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"levels": ["ERROR"], "limit": 50},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["total_hits"] >= 0
    for result in body["results"]:
        assert result["level"] == "ERROR", result


def test_service_filter_returns_only_matching_service(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"services": ["payment-service"], "limit": 50},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    for result in body["results"]:
        assert result["service_name"] == "payment-service", result


def test_sort_by_timestamp_ascending_returns_ordered_results(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"sort_by": "timestamp", "sort_order": "asc", "limit": 25},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    timestamps = [r["timestamp"] for r in body["results"]]
    assert timestamps == sorted(timestamps), timestamps


def test_pagination_has_more_flag_changes_with_offset(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    first = http_client.post(
        "/api/v1/logs/search",
        json={"limit": 5, "offset": 0},
        headers=auth_headers,
    )
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["pagination"]["has_more"] is True
    assert first_body["pagination"]["offset"] == 0
    assert first_body["pagination"]["limit"] == 5

    last_offset = max(0, first_body["total_hits"] - 1)
    last = http_client.post(
        "/api/v1/logs/search",
        json={"limit": 5, "offset": max(45, last_offset)},
        headers=auth_headers,
    )
    assert last.status_code == 200, last.text
    last_body = last.json()
    expected_has_more = (
        max(45, last_offset) + 5
    ) < last_body["total_hits"]
    assert last_body["pagination"]["has_more"] == expected_has_more


def test_aggregations_levels_sum_matches_total_when_no_filter(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    response = http_client.post(
        "/api/v1/logs/search",
        json={"limit": 1},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    levels = body["aggregations"]["levels"]
    services = body["aggregations"]["services"]

    assert sum(b["doc_count"] for b in levels) == body["total_hits"]
    assert len(services) >= 1

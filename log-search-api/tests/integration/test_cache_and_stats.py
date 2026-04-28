from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

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


def _build_seed_entries(prefix: str, count: int = 5) -> list[dict[str, object]]:
    now = datetime.now(UTC)
    entries: list[dict[str, object]] = []
    messages = [
        "payment processed for order p-100",
        "payment failure on transaction p-101",
        "payment retry attempt for invoice p-102",
        "payment refund issued for p-103",
        "payment authorized for p-104",
    ]
    for i in range(count):
        ts = now - timedelta(minutes=i)
        entries.append(
            {
                "id": f"{prefix}-{int(now.timestamp())}-{i:04d}",
                "timestamp": ts.isoformat(),
                "level": "INFO",
                "service_name": "payment-service",
                "message": messages[i % len(messages)],
                "content": {"i": i, "trace": f"trace-{i}"},
            }
        )
    return entries


@pytest.fixture(scope="module", autouse=True)
def seed_data(http_client: httpx.Client, auth_headers: dict[str, str]) -> None:
    entries = _build_seed_entries(prefix="cache-stats", count=5)
    response = http_client.post(
        "/api/v1/logs/bulk",
        json={"entries": entries},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("created", 0) >= 1
    time.sleep(6)


def _unique_query(prefix: str) -> str:
    return f"payment {prefix}-{int(time.time() * 1000)}"


def test_repeat_search_returns_cache_hit(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    payload = {"q": "payment", "limit": 10, "offset": 0}
    first = http_client.post(
        "/api/v1/logs/search",
        json=payload,
        headers=auth_headers,
    )
    assert first.status_code == 200, first.text
    first_body = first.json()

    second = http_client.post(
        "/api/v1/logs/search",
        json=payload,
        headers=auth_headers,
    )
    assert second.status_code == 200, second.text
    second_body = second.json()

    assert second_body["cache_hit"] is True
    assert first_body["total_hits"] == second_body["total_hits"]


def test_get_post_share_cache_key(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    payload = {"q": "payment", "limit": 7, "offset": 0}
    primer = http_client.post(
        "/api/v1/logs/search",
        json=payload,
        headers=auth_headers,
    )
    assert primer.status_code == 200, primer.text

    get_resp = http_client.get(
        "/api/v1/logs/search",
        params={"q": "payment", "limit": 7, "offset": 0},
        headers=auth_headers,
    )
    assert get_resp.status_code == 200, get_resp.text
    get_body = get_resp.json()

    assert get_body["cache_hit"] is True


def test_stats_endpoint_returns_counters_and_index_info(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    primer_payload = {"q": "payment refund", "limit": 5, "offset": 0}
    first = http_client.post(
        "/api/v1/logs/search",
        json=primer_payload,
        headers=auth_headers,
    )
    assert first.status_code == 200, first.text
    second = http_client.post(
        "/api/v1/logs/search",
        json=primer_payload,
        headers=auth_headers,
    )
    assert second.status_code == 200, second.text

    response = http_client.get("/api/v1/stats", headers=auth_headers)
    assert response.status_code == 200, response.text
    body = response.json()

    assert "cache" in body
    assert "index" in body
    assert "timestamp" in body

    cache = body["cache"]
    assert cache["hits"] >= 1
    assert cache["misses"] >= 1
    assert cache["hit_rate"] > 0.0
    assert cache["errors"] >= 0

    index = body["index"]
    assert index["index"] == os.getenv("ELASTICSEARCH_INDEX", "logs")
    assert index["doc_count"] >= 5
    assert index["size_in_bytes"] >= 0


def test_stats_requires_authentication(http_client: httpx.Client) -> None:
    response = http_client.get("/api/v1/stats")
    assert response.status_code == 401, response.text

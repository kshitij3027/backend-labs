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


def _build_entries(count: int) -> list[dict[str, object]]:
    rng = random.Random(42)
    now = datetime.now(UTC)
    entries: list[dict[str, object]] = []
    for i in range(count):
        offset = timedelta(minutes=rng.randint(0, 24 * 60))
        ts = now - offset
        entries.append(
            {
                "id": f"bulk-{int(now.timestamp())}-{i:04d}",
                "timestamp": ts.isoformat(),
                "level": rng.choice(_LEVELS),
                "service_name": rng.choice(_SERVICES),
                "message": f"event {i} processed for service",
                "content": {"i": i, "trace": f"t-{i}"},
            }
        )
    return entries


def _poll_get(
    client: httpx.Client,
    headers: dict[str, str],
    doc_id: str,
    attempts: int = 8,
    delay: float = 0.5,
) -> httpx.Response:
    last: httpx.Response | None = None
    for _ in range(attempts):
        last = client.get(f"/api/v1/logs/{doc_id}", headers=headers)
        if last.status_code == 200:
            return last
        time.sleep(delay)
    assert last is not None
    return last


def test_bulk_ingest_100_docs_creates_all(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    entries = _build_entries(100)
    response = http_client.post(
        "/api/v1/logs/bulk",
        json={"entries": entries},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text

    body = response.json()
    assert body.get("total") == 100
    assert body.get("created") == 100
    assert body.get("errors") == 0
    assert body.get("error_items") == []

    sample_ids = random.Random(7).sample([e["id"] for e in entries], 3)
    for doc_id in sample_ids:
        get_response = _poll_get(http_client, auth_headers, str(doc_id))
        assert get_response.status_code == 200, get_response.text
        fetched = get_response.json()
        assert fetched["id"] == doc_id
        assert fetched["level"] in _LEVELS
        assert fetched["service_name"] in _SERVICES


def test_bulk_ingest_requires_authentication(http_client: httpx.Client) -> None:
    response = http_client.post(
        "/api/v1/logs/bulk", json={"entries": []}
    )
    assert response.status_code == 401, response.text


def test_index_mapping_has_expected_field_types() -> None:
    es_url = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200").rstrip("/")
    index = os.getenv("ELASTICSEARCH_INDEX", "logs")
    response = httpx.get(f"{es_url}/{index}/_mapping", timeout=10.0)
    assert response.status_code == 200, response.text

    body = response.json()
    index_block = body.get(index) or next(iter(body.values()))
    properties = index_block["mappings"]["properties"]

    assert properties["timestamp"]["type"] == "date"
    assert properties["level"]["type"] == "keyword"
    assert properties["service_name"]["type"] == "keyword"
    assert properties["message"]["type"] == "text"

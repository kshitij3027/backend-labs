from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

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
    with httpx.Client(base_url=base_url, timeout=15.0) as client:
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


def _sample_log_payload() -> dict[str, object]:
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": "ERROR",
        "service_name": "payment-service",
        "message": "transaction failed for user 42 with code TXN-500",
        "content": {"user_id": 42, "code": "TXN-500", "amount": 19.99},
    }


def test_ingest_single_log_returns_created_envelope(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    payload = _sample_log_payload()
    response = http_client.post("/api/v1/logs", json=payload, headers=auth_headers)
    assert response.status_code == 201, response.text

    body = response.json()
    assert isinstance(body.get("id"), str) and body["id"]
    assert body.get("result") == "created"
    assert body.get("index") == "logs"


def test_ingest_then_fetch_roundtrip(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    payload = _sample_log_payload()
    create_response = http_client.post(
        "/api/v1/logs", json=payload, headers=auth_headers
    )
    assert create_response.status_code == 201, create_response.text
    doc_id = create_response.json()["id"]

    last_status = None
    last_body: dict[str, object] | None = None
    for _ in range(10):
        get_response = http_client.get(
            f"/api/v1/logs/{doc_id}", headers=auth_headers
        )
        last_status = get_response.status_code
        if get_response.status_code == 200:
            last_body = get_response.json()
            break

    assert last_status == 200, f"final status {last_status}"
    assert last_body is not None
    assert last_body["id"] == doc_id
    assert last_body["level"] == payload["level"]
    assert last_body["service_name"] == payload["service_name"]
    assert last_body["message"] == payload["message"]
    assert last_body["content"] == payload["content"]


def test_get_missing_log_returns_404_envelope(
    http_client: httpx.Client, auth_headers: dict[str, str]
) -> None:
    bogus_id = f"missing-{uuid.uuid4().hex}"
    response = http_client.get(f"/api/v1/logs/{bogus_id}", headers=auth_headers)
    assert response.status_code == 404, response.text

    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "NOT_FOUND"
    assert isinstance(body["error"]["message"], str) and body["error"]["message"]


def test_get_log_requires_authentication(http_client: httpx.Client) -> None:
    response = http_client.get(f"/api/v1/logs/some-id-{uuid.uuid4().hex}")
    assert response.status_code == 401, response.text


def test_post_log_requires_authentication(http_client: httpx.Client) -> None:
    response = http_client.post("/api/v1/logs", json=_sample_log_payload())
    assert response.status_code == 401, response.text

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


def _token_url() -> str:
    return "/api/v1/auth/token"


def _me_url() -> str:
    return "/api/v1/auth/me"


def test_token_endpoint_rejects_bad_credentials(http_client: httpx.Client) -> None:
    response = http_client.post(
        _token_url(),
        data={"username": "nobody", "password": "definitely-wrong"},
    )
    assert response.status_code == 401
    body = response.json()
    assert "detail" in body or "error" in body


def test_token_endpoint_returns_bearer_on_good_credentials(
    http_client: httpx.Client, credentials: tuple[str, str]
) -> None:
    username, password = credentials
    response = http_client.post(
        _token_url(),
        data={"username": username, "password": password},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body.get("access_token"), str)
    assert body.get("token_type") == "bearer"
    assert isinstance(body.get("expires_at"), str)


def test_me_endpoint_rejects_missing_authorization(http_client: httpx.Client) -> None:
    response = http_client.get(_me_url())
    assert response.status_code == 401


def test_me_endpoint_rejects_bad_token(http_client: httpx.Client) -> None:
    response = http_client.get(
        _me_url(),
        headers={"Authorization": "Bearer this.is.not-a-valid-jwt"},
    )
    assert response.status_code == 401


def test_me_endpoint_returns_username_with_good_token(
    http_client: httpx.Client, credentials: tuple[str, str]
) -> None:
    username, password = credentials
    token_response = http_client.post(
        _token_url(),
        data={"username": username, "password": password},
    )
    assert token_response.status_code == 200, token_response.text
    access_token = token_response.json()["access_token"]

    me_response = http_client.get(
        _me_url(),
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert me_response.status_code == 200, me_response.text
    assert me_response.json() == {"username": username}

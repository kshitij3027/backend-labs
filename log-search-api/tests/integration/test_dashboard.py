"""Integration tests for the unauthenticated dashboard + OpenAPI surface."""

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
    with httpx.Client(base_url=base_url, timeout=15.0) as client:
        yield client


def test_dashboard_root_returns_html(http_client: httpx.Client) -> None:
    response = http_client.get("/")
    assert response.status_code == 200, response.text
    content_type = response.headers.get("content-type", "")
    assert content_type.startswith("text/html"), content_type

    html = response.text.lower()
    project_name = os.getenv("PROJECT_NAME", "Log Search API").lower()
    assert "<title>" in html
    assert project_name in html
    # Form fields the JS depends on.
    for needle in ('id="username"', 'id="password"', 'id="q"', 'id="levels"', 'id="services"'):
        assert needle in html, f"expected {needle!r} in dashboard html"
    # Embedded API prefix for the JS to read.
    assert "data-api-prefix" in html


def test_dashboard_root_is_unauthenticated(http_client: httpx.Client) -> None:
    # The dashboard itself should be reachable without a token; auth is
    # performed by the JS hitting /api/v1/auth/token.
    response = http_client.get("/", headers={})
    assert response.status_code == 200
    assert "html" in response.headers.get("content-type", "")


def test_swagger_docs_available(http_client: httpx.Client) -> None:
    response = http_client.get("/api/docs")
    assert response.status_code == 200, response.text
    assert "swagger" in response.text.lower() or "openapi" in response.text.lower()


def test_redoc_docs_available(http_client: httpx.Client) -> None:
    response = http_client.get("/api/redoc")
    assert response.status_code == 200, response.text
    assert "redoc" in response.text.lower() or "openapi" in response.text.lower()


def test_openapi_spec_lists_search_endpoint(http_client: httpx.Client) -> None:
    response = http_client.get("/openapi.json")
    assert response.status_code == 200, response.text
    spec = response.json()
    assert "info" in spec and "title" in spec["info"]
    assert isinstance(spec["info"]["title"], str) and spec["info"]["title"]
    paths = spec.get("paths") or {}
    assert "/api/v1/logs/search" in paths, sorted(paths.keys())[:20]


def test_static_app_js_served(http_client: httpx.Client) -> None:
    response = http_client.get("/static/app.js")
    assert response.status_code == 200, response.text
    assert "javascript" in response.headers.get("content-type", "").lower()
    assert "Log Search API" in response.text or "lsa_token" in response.text


def test_static_styles_css_served(http_client: httpx.Client) -> None:
    response = http_client.get("/static/styles.css")
    assert response.status_code == 200, response.text
    assert "css" in response.headers.get("content-type", "").lower()

"""Integration tests for the HTMX dashboard surface.

Commit 15 lands the dashboard shell and the stats card. These tests
exercise the live FastAPI stack inside the ``tester`` Docker profile —
they're skipped on the host because they rely on the ``BASE_URL`` env
var the compose file injects into the tester container.

Subsequent commits extend this file with assertions for the recent,
breakdown, in-flight, and FinHealth partials.
"""
from __future__ import annotations

import os

import httpx
import pytest

BASE_URL = os.environ.get("BASE_URL")

pytestmark = pytest.mark.skipif(
    not BASE_URL, reason="BASE_URL not set — integration tests only run in the tester container"
)


@pytest.mark.asyncio
async def test_dashboard_root_serves_htmx_html() -> None:
    """The root URL returns an HTML page that loads vendored htmx and the title."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "<title>Compliance Reporting Engine</title>" in body
    assert "htmx.min.js" in body


@pytest.mark.asyncio
async def test_stats_partial_renders_with_zero_state() -> None:
    """The stats partial renders even with an empty reports table."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        response = await client.get("/partials/stats")
    assert response.status_code == 200
    body = response.text
    assert "Total reports" in body
    assert "Success rate" in body
    assert "In flight" in body


@pytest.mark.asyncio
async def test_static_assets_served() -> None:
    """The vendored htmx and the dashboard CSS are served via /static."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        js = await client.get("/static/htmx.min.js")
        css = await client.get("/static/dashboard.css")
    assert js.status_code == 200
    assert len(js.content) > 1000  # non-trivial vendored asset
    assert css.status_code == 200
    assert len(css.content) > 100


@pytest.mark.asyncio
async def test_finhealth_partial_renders_empty() -> None:
    """The FinHealth partial renders an empty placeholder when no reports exist."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        response = await client.get("/partials/finhealth")
    assert response.status_code == 200
    body = response.text
    # Card title always renders; placeholder copy when no FinHealth reports.
    assert "FinHealth" in body


@pytest.mark.asyncio
async def test_dashboard_shell_includes_finhealth_card() -> None:
    """The dashboard shell wires the FinHealth partial via hx-get."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    # The shell renders an hx-get for the FinHealth partial endpoint.
    assert "/partials/finhealth" in body

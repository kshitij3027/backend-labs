"""Tests for the dashboard HTML + static asset routes.

Commit 10 replaces the placeholder dashboard with the full single-page
UI (live stats cards, search bar with filters, three "Generate N"
buttons). These tests assert the concrete markup the client script
depends on — the set of element IDs — rather than free-form shape,
since a broken ID would break the Chrome UI flow in Commit 13.

The static-asset tests prove the ``/static`` mount resolves to the
real ``static/`` directory on disk inside the running app for both
``app.css`` and ``app.js``.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


async def test_root_returns_html(async_client: AsyncClient) -> None:
    """GET / must render the dashboard template as HTML."""
    resp = await async_client.get("/")
    assert resp.status_code == 200

    # ``text/html; charset=utf-8`` is what Jinja2Templates emits by
    # default — ``startswith`` keeps us robust to header casing or
    # the exact charset suffix changing.
    assert resp.headers["content-type"].startswith("text/html")
    assert "<html" in resp.text.lower()
    assert "real-time log indexing" in resp.text.lower()


async def test_static_asset_served(async_client: AsyncClient) -> None:
    """The /static mount serves app.css with the expected content-type."""
    resp = await async_client.get("/static/app.css")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/css")


async def test_dashboard_has_required_ids(async_client: AsyncClient) -> None:
    """Every element ID the client script and Chrome UI tests expect
    must be present in the rendered template."""
    resp = await async_client.get("/")
    body = resp.text
    required_ids = [
        "stats-docs-indexed",
        "stats-current-segment-docs",
        "stats-flushed-memory-segments",
        "stats-disk-segments",
        "stats-memory-bytes",
        "stats-throughput",
        "stats-vocab-size",
        "stats-query-p95",
        "search-input",
        "search-service",
        "search-level",
        "search-limit",
        "search-button",
        "search-results",
        "search-meta",
        "generate-500",
        "generate-5000",
        "generate-50000",
        "status-dot",
    ]
    for ident in required_ids:
        assert f'id="{ident}"' in body, f"missing id={ident}"


async def test_dashboard_services_rendered(async_client: AsyncClient) -> None:
    """Services from sample_data.SERVICES should render as dropdown options."""
    resp = await async_client.get("/")
    body = resp.text
    # Services should render inside the select dropdown.
    assert "auth-service" in body
    assert "payment-service" in body


async def test_dashboard_levels_rendered(async_client: AsyncClient) -> None:
    """Log-level filter dropdown exposes the canonical levels."""
    resp = await async_client.get("/")
    body = resp.text
    for lvl in ["INFO", "WARN", "ERROR", "DEBUG"]:
        assert lvl in body


async def test_static_js_served(async_client: AsyncClient) -> None:
    """The /static mount serves app.js with a JS content-type."""
    resp = await async_client.get("/static/app.js")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith(
        "application/javascript"
    ) or resp.headers["content-type"].startswith("text/javascript")

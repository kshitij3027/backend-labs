"""Tests for the dashboard HTML + static asset routes.

Commit 7 ships a placeholder dashboard; Commit 10 replaces the
template with the full UI. These tests stay loose — they assert the
shape (HTML response, content-type, a project-identifying string)
rather than any specific markup — so the upcoming template rewrite
does not require a test rewrite.

The static-asset test proves the ``/static`` mount actually resolves
to the real ``static/`` directory on disk inside the running app.
Once ``app.js`` is added in Commit 10 we can extend this to check
the JS is served too.
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

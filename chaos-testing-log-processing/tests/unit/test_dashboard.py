"""Unit tests for C17 — vanilla dashboard mount + static asset serving.

C17 added ``src/dashboard/{index.html, app.js, styles.css}`` and wired it
into ``src.main.create_app`` by:

* Mounting ``src/dashboard/`` as a ``StaticFiles`` app under the
  ``/dashboard-static`` URL prefix.
* Registering a top-level ``GET /dashboard`` route that returns the
  ``index.html`` page (via ``FileResponse``).

These tests verify the static surface is wired correctly without entering
the lifespan or touching Docker / DB. We drive the ASGI app directly with
``httpx.AsyncClient`` + ``ASGITransport`` (the same pattern used by
``test_api_routes.py``). No lifespan means we never need a real
``DockerClient``, ``SystemMonitor``, or aiosqlite engine for these checks.
"""

from __future__ import annotations

import re

from httpx import ASGITransport, AsyncClient

from src.main import create_app


def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# --------------------------------------------------------------------------- #
# /dashboard — HTML page
# --------------------------------------------------------------------------- #


class TestDashboardHtml:
    async def test_dashboard_serves_html_with_title(self) -> None:
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard")
        assert resp.status_code == 200, resp.text
        ctype = resp.headers.get("content-type", "")
        # FastAPI's FileResponse emits ``text/html`` (sometimes with a
        # ``; charset=...`` suffix depending on the starlette version);
        # we accept either form.
        assert ctype.startswith("text/html"), (
            f"expected text/html content-type, got {ctype!r}"
        )
        body = resp.text
        assert "<title>Chaos Testing Framework — Dashboard</title>" in body, (
            "dashboard HTML missing expected <title> element"
        )

    async def test_dashboard_html_references_static_assets(self) -> None:
        """The HTML must point at the mounted static prefix for CSS + JS."""
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard")
        assert resp.status_code == 200
        body = resp.text
        # Both substring + regex give defence-in-depth against accidental
        # rewrites (e.g., switching to a relative path).
        assert "/dashboard-static/styles.css" in body
        assert "/dashboard-static/app.js" in body
        assert re.search(r'href=["\']/dashboard-static/styles\.css["\']', body), (
            "expected <link href> referencing /dashboard-static/styles.css"
        )
        assert re.search(r'src=["\']/dashboard-static/app\.js["\']', body), (
            "expected <script src> referencing /dashboard-static/app.js"
        )

    async def test_dashboard_html_loads_chart_js_from_cdn(self) -> None:
        """Chart.js comes from jsdelivr — no bundler, no vendoring."""
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard")
        assert resp.status_code == 200
        assert "cdn.jsdelivr.net/npm/chart.js" in resp.text, (
            "dashboard HTML must load Chart.js from cdn.jsdelivr.net"
        )


# --------------------------------------------------------------------------- #
# /dashboard-static/* — mounted StaticFiles
# --------------------------------------------------------------------------- #


class TestDashboardStaticAssets:
    async def test_styles_css_served_with_text_css_mime(self) -> None:
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard-static/styles.css")
        assert resp.status_code == 200, resp.text
        ctype = resp.headers.get("content-type", "")
        assert ctype.startswith("text/css"), (
            f"expected text/css content-type, got {ctype!r}"
        )
        # Case-sensitive substring check: the dashboard topbar selector
        # must actually be present in the served stylesheet.
        assert ".topbar" in resp.text, (
            "served styles.css missing expected .topbar selector"
        )

    async def test_app_js_served_with_javascript_mime(self) -> None:
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard-static/app.js")
        assert resp.status_code == 200, resp.text
        ctype = resp.headers.get("content-type", "")
        # Starlette / mimetypes may report ``application/javascript`` or
        # ``text/javascript`` depending on the platform's mimetype DB.
        assert ctype.startswith("application/javascript") or ctype.startswith(
            "text/javascript"
        ), f"expected JS content-type, got {ctype!r}"
        body = resp.text
        # The literal ``WebSocket`` class reference must be present — the
        # dashboard streams live run events over a WebSocket.
        assert "WebSocket" in body, "served app.js missing WebSocket reference"
        # Chart.js initialization — confirms the live chart wiring shipped.
        assert "new Chart(" in body, (
            "served app.js missing Chart.js initialization (new Chart(...))"
        )

    async def test_missing_static_asset_returns_404(self) -> None:
        app = create_app()
        async with _client(app) as client:
            resp = await client.get("/dashboard-static/missing.png")
        assert resp.status_code == 404

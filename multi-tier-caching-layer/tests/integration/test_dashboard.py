"""Integration tests for the live monitoring dashboard's serving layer.

These drive the FastAPI app **through its lifespan** (via
``app.router.lifespan_context``) so the StaticFiles mount and the ``GET /``
FileResponse are exercised exactly as in production. They assert only the
*serving* contract — that the HTML page and the static assets (vendored
Chart.js, CSS, JS) are reachable and reference the pieces the browser needs to
boot the dashboard (Chart.js, the dashboard script, the ``/ws/metrics`` socket,
and the stable element ids the JS patches). The live WebSocket behaviour itself
is covered by the Chrome MCP UI E2E and the ``/ws/metrics`` tests.

``pytest.ini`` sets ``asyncio_mode = auto`` so plain ``async def test_*``
functions run without an explicit ``@pytest.mark.asyncio`` decorator.
"""

from __future__ import annotations

import httpx
from httpx import ASGITransport, AsyncClient

from src.main import app


async def test_index_serves_dashboard_html() -> None:
    """GET / returns 200 HTML referencing Chart.js, the dashboard JS, and ids."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/")

    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]

    body = resp.text
    # Vendored asset references (no CDN) + the deferred dashboard script.
    assert "chart.min.js" in body
    assert "dashboard.js" in body
    assert "dashboard.css" in body
    # Connection badge + every metric-card id the JS patches.
    assert "conn-status" in body
    for card_id in (
        "m-hit-rate",
        "m-total-requests",
        "m-l1-mb",
        "m-cached-p90",
        "m-uncached-p90",
        "m-degraded",
    ):
        assert card_id in body, card_id
    # The three chart canvases by id.
    for canvas_id in ("hitRateChart", "tierChart", "latencyChart"):
        assert canvas_id in body, canvas_id
    # Recommendations panel + alert banner containers.
    assert "recommendations" in body
    assert "alert-banner" in body


async def test_static_dashboard_js_served() -> None:
    """GET /static/dashboard.js returns 200 and wires the WebSocket client."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/static/dashboard.js")

    assert resp.status_code == 200
    body = resp.text
    # The resilient client derives the WS URL and opens a WebSocket to /ws/metrics.
    assert "ws/metrics" in body
    assert "WebSocket" in body


async def test_static_chart_js_served_nonempty() -> None:
    """GET /static/chart.min.js returns 200 with a non-empty vendored bundle."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/static/chart.min.js")

    assert resp.status_code == 200
    assert len(resp.content) > 0


async def test_static_dashboard_css_served() -> None:
    """GET /static/dashboard.css returns 200."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/static/dashboard.css")

    assert resp.status_code == 200

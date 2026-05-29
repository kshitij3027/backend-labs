"""Dashboard *serving* integration tests for the C9 live dashboard (NON-UI).

These assert that the FastAPI app actually serves the dashboard shell and its
static assets correctly — the page HTML, the vendored Chart.js, and the client
JS/CSS — so a browser pointed at ``/`` would have everything it needs to render
and to open the ``/ws/metrics`` stream. The *visual* behaviour (charts painting,
badges flipping) is verified separately via the Chrome UI test in the main
thread; here we only check the HTTP/static serving contract.

The async-FastAPI pattern (lifespan context + ASGITransport AsyncClient) mirrors
``tests/integration/test_api.py``: we drive the *real* application
(``src.main.app``) through its ``lifespan`` so the ``/`` Jinja2 route and the
mounted ``/static`` StaticFiles app are exercised exactly as in production.

Determinism note
----------------
Every assertion here is over static / template-rendered content, so the live
background optimization loop is irrelevant to the result. We still pause it in
each test (mirroring ``test_api.py._pause_background_loop``) to keep the app
quiescent and avoid any incidental log noise interleaving with the request.
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


def _client() -> AsyncClient:
    """An in-process ASGI client bound to the real app."""
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://t")


async def _pause_background_loop() -> None:
    """Cancel the live optimization loop so nothing mutates during serving checks.

    Mirrors ``tests/integration/test_api.py``. The lifespan starts a background
    task that ticks every ``optimization_interval`` seconds; these serving tests
    don't depend on it, so we cancel it after startup to keep the app quiescent.
    The task is recreated fresh on the next lifespan entry, so this stays isolated
    to the current test.
    """
    task = app.state.optimization_task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --- GET / (the dashboard shell) --------------------------------------------


@pytest.mark.asyncio
async def test_index_serves_dashboard_html() -> None:
    """``GET /`` returns the HTML shell with both canvases and asset references."""
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/")
    assert r.status_code == 200
    # Content type is HTML (FastAPI HTMLResponse -> text/html; charset=utf-8).
    assert "text/html" in r.headers["content-type"].lower()

    body = r.text
    # Page title rendered from the template.
    assert "Adaptive Batching Engine" in body
    assert "<title>" in body
    # Both chart canvases the dashboard JS binds to must be present.
    assert 'id="throughputChart"' in body
    assert 'id="resourceChart"' in body
    # The shell must pull in the vendored Chart.js and the client JS.
    assert "/static/chart.min.js" in body
    assert "/static/dashboard.js" in body
    # And the stylesheet.
    assert "/static/dashboard.css" in body


@pytest.mark.asyncio
async def test_index_injects_dashboard_points() -> None:
    """The template injects ``window.DASHBOARD_POINTS`` so the JS knows the cap.

    This is the one server-rendered dynamic value in the shell; it must be a bare
    integer (not the un-rendered Jinja expression) so the client picks up a real
    point budget.
    """
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/")
    assert r.status_code == 200
    body = r.text
    assert "window.DASHBOARD_POINTS" in body
    # The Jinja expression must have been rendered away to a concrete number.
    assert "{{" not in body and "}}" not in body


# --- GET /static/dashboard.js (the client) ----------------------------------


@pytest.mark.asyncio
async def test_static_dashboard_js_served() -> None:
    """``/static/dashboard.js`` is served, non-empty, JS-ish, and opens the WS."""
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/static/dashboard.js")
    assert r.status_code == 200
    body = r.text
    assert len(body) > 0
    # StaticFiles guesses the media type from the extension; .js -> a JS-ish type.
    ctype = r.headers["content-type"].lower()
    assert "javascript" in ctype or "ecmascript" in ctype, f"unexpected type: {ctype}"
    # The client must construct a WebSocket against the dashboard's WS endpoint.
    assert "WebSocket" in body
    assert "/ws/metrics" in body


# --- GET /static/dashboard.css ----------------------------------------------


@pytest.mark.asyncio
async def test_static_dashboard_css_served() -> None:
    """``/static/dashboard.css`` is served and non-empty."""
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/static/dashboard.css")
    assert r.status_code == 200
    assert len(r.text) > 0
    assert "css" in r.headers["content-type"].lower()


# --- GET /static/chart.min.js (vendored Chart.js) ---------------------------


@pytest.mark.asyncio
async def test_static_chartjs_served_and_large() -> None:
    """``/static/chart.min.js`` is served, large, and is actually Chart.js."""
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/static/chart.min.js")
    assert r.status_code == 200
    body = r.text
    # Vendored Chart.js is a large bundle; assert it's clearly not a stub.
    assert len(body) > 100_000, f"chart.min.js unexpectedly small: {len(body)} bytes"
    # Sanity: the bundle defines/announces the Chart.js library.
    assert "Chart" in body


# --- Missing static asset ----------------------------------------------------


@pytest.mark.asyncio
async def test_missing_static_asset_is_404() -> None:
    """A request for a non-existent static file returns 404 (mount is wired)."""
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        async with _client() as ac:
            r = await ac.get("/static/does-not-exist.js")
    assert r.status_code == 404

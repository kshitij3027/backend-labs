"""HTTP-level tests for the dashboard UI (C5).

Exercises ``GET /`` (the Jinja2-rendered dashboard shell) and the
static-file mounts at ``/static/*``. We intentionally stay at the
HTTP layer here — DOM behavior, click flows, and console-error
checks are the main thread's job via Chrome MCP. The goal of these
tests is to guarantee:

* The shell is served as HTML with the expected title + asset links.
* All DOM IDs that ``app.js`` queries by ``getElementById`` actually
  exist in the rendered template (prevents a silent JS TypeError on
  first load).
* ``/static/app.js`` and ``/static/app.css`` are reachable with the
  correct MIME type and contain the hooks we rely on
  (``fetchSearch``, ``renderFacets``, ``AbortController``, ``debounce``,
  ``highlight`` + ``.level-INFO`` / ``.level-ERROR`` / styled ``mark``).
* Mounting ``/static`` + the ``/`` UI route didn't shadow ``/api/*``.
* XSS protection helpers (``escapeHtml`` + ``escapeRegex``) are
  present in the served JS — cheap evidence that user input passes
  through an escape layer before reaching the DOM.
"""

from __future__ import annotations

from httpx import AsyncClient


# ---------------------------------------------------------------------------
# GET / (dashboard shell)
# ---------------------------------------------------------------------------


async def test_dashboard_returns_html(async_client: AsyncClient):
    """GET / returns 200 text/html with the expected <title>."""
    resp = await async_client.get("/")
    assert resp.status_code == 200, resp.text

    ct = resp.headers.get("content-type", "")
    assert ct.startswith("text/html"), f"expected text/html content-type; got {ct!r}"

    body = resp.text
    assert "<title>" in body, "rendered dashboard must include a <title> tag"
    assert "Faceted Log Search" in body, (
        "dashboard title text 'Faceted Log Search' missing from body"
    )


async def test_dashboard_links_css_and_js(async_client: AsyncClient):
    """Rendered dashboard references the static app.css and app.js bundles."""
    resp = await async_client.get("/")
    assert resp.status_code == 200, resp.text
    body = resp.text

    assert "/static/app.css" in body, (
        "dashboard should link to /static/app.css"
    )
    assert "/static/app.js" in body, (
        "dashboard should include a <script> tag pointing at /static/app.js"
    )


async def test_dashboard_has_required_dom_ids(async_client: AsyncClient):
    """Rendered shell contains every DOM id that app.js references.

    If any of these go missing, ``getElementById`` returns ``null``
    and the dashboard throws a TypeError on first load. This test
    is a cheap guard against that class of regression.
    """
    resp = await async_client.get("/")
    assert resp.status_code == 200, resp.text
    body = resp.text

    required_ids = [
        'id="facet-panel"',
        'id="results"',
        'id="query-input"',
        'id="generate-btn"',
        'id="clear-btn"',
        'id="load-more-btn"',
        'id="stat-total"',
        'id="stat-time"',
        'id="stat-filters"',
        'id="stat-cache"',
    ]
    missing = [ident for ident in required_ids if ident not in body]
    assert not missing, f"rendered shell missing required DOM ids: {missing}"


# ---------------------------------------------------------------------------
# /static/app.js
# ---------------------------------------------------------------------------


async def test_static_js_served(async_client: AsyncClient):
    """GET /static/app.js returns 200 JS with the expected symbols."""
    resp = await async_client.get("/static/app.js")
    assert resp.status_code == 200, resp.text

    ct = resp.headers.get("content-type", "").lower()
    assert "javascript" in ct, f"expected javascript content-type; got {ct!r}"

    body = resp.text
    for needle in ("fetchSearch", "renderFacets", "AbortController", "debounce", "highlight"):
        assert needle in body, f"app.js missing expected symbol {needle!r}"


async def test_js_escapes_user_input(async_client: AsyncClient):
    """Served JS has both ``escapeHtml`` and ``escapeRegex`` helpers.

    Cheap evidence that user-supplied query text and facet values go
    through an escape layer before being injected into the DOM, so
    an attacker can't break out via ``<script>`` or a crafted regex.
    """
    resp = await async_client.get("/static/app.js")
    assert resp.status_code == 200, resp.text
    body = resp.text

    assert "escapeHtml" in body, (
        "app.js should define escapeHtml to prevent XSS in rendered output"
    )
    assert "escapeRegex" in body, (
        "app.js should define escapeRegex so highlight() can't be broken by "
        "a user query containing regex metacharacters"
    )


# ---------------------------------------------------------------------------
# /static/app.css
# ---------------------------------------------------------------------------


async def test_static_css_served(async_client: AsyncClient):
    """GET /static/app.css returns 200 text/css with level-badge classes."""
    resp = await async_client.get("/static/app.css")
    assert resp.status_code == 200, resp.text

    ct = resp.headers.get("content-type", "").lower()
    assert "text/css" in ct, f"expected text/css content-type; got {ct!r}"

    body = resp.text
    for needle in (".level-INFO", ".level-ERROR", "mark"):
        assert needle in body, f"app.css missing expected selector {needle!r}"


# ---------------------------------------------------------------------------
# Sanity — UI router did not shadow /api/*
# ---------------------------------------------------------------------------


async def test_dashboard_does_not_break_api_routes(async_client: AsyncClient):
    """GET /api/stats still returns 200 after UI mounts are in place."""
    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200, resp.text

"""Integration tests for the C9 Jinja2 + HTMX dashboard.

Coverage:

* ``GET /`` returns 200 + an HTML page with the three expected
  sections (header title, paste-a-log demo heading, pattern-hits
  block).
* ``GET /api/stats/html`` returns 200 + the partial including the
  ``stats-grid`` container and a "Logs Processed" label so HTMX can
  swap it directly into ``#live-stats``.
* ``GET /api/pattern_hits/html`` returns 200; with no traffic the
  partial falls back to the "No redactions yet." copy.
* ``GET /static/htmx.min.js`` returns 200 — confirms the vendored
  asset is reachable.
* ``GET /static/dashboard.css`` returns 200 — same coverage for CSS.

Why drive via ``ASGITransport`` + ``LifespanManager``
----------------------------------------------------
The dashboard routes resolve ``request.app.state.config_manager`` and
``request.app.state.stats`` off the lifespan-built singletons; without
``LifespanManager`` running the startup the routes would AttributeError
on ``app.state``. Matches the pattern used by every other integration
test file in this directory.
"""
from __future__ import annotations

from typing import AsyncIterator

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.main import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Yield an :class:`httpx.AsyncClient` with the FastAPI lifespan running."""
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dashboard_root_returns_html_with_three_sections(
    client: AsyncClient,
) -> None:
    """GET / returns 200, HTML, and contains all three section markers.

    We assert on stable strings rather than DOM structure so a minor
    styling tweak doesn't break the test. The three strings checked
    correspond 1:1 to the three sections specified in plan.md C9.
    """
    resp = await client.get("/")
    assert resp.status_code == 200, resp.text
    # HTMLResponse defaults Content-Type to text/html
    assert "text/html" in resp.headers.get("content-type", "")

    body = resp.text
    # Header / page title is rendered into the H1.
    assert "Log Redaction Engine" in body
    # Paste-a-log section heading.
    assert "Try a redaction" in body
    # Pattern-hits section: the polling target is inside ``#pattern-hits``;
    # the partial renders the "Pattern Hits" h3 once it loads. The shell
    # page itself contains the section id, which is the deterministic
    # marker we assert on.
    assert "pattern-hits" in body
    # Vendored HTMX script is wired in.
    assert "/static/htmx.min.js" in body
    # CSS stylesheet is wired in.
    assert "/static/dashboard.css" in body


@pytest.mark.asyncio
async def test_stats_html_partial_renders_grid(client: AsyncClient) -> None:
    """GET /api/stats/html returns 200 + the stats-grid partial.

    Asserts on the ``stats-grid`` class + ``Logs Processed`` label —
    both come from ``_stats_card.html`` and together prove the partial
    rendered the full template (not just a placeholder).
    """
    resp = await client.get("/api/stats/html")
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "stats-grid" in body
    assert "Logs Processed" in body
    # Numeric data attribute is present (used by the Chrome MCP test
    # to scrape ``logs_processed`` without parsing the formatted cell).
    assert "data-logs-processed=" in body


@pytest.mark.asyncio
async def test_pattern_hits_html_partial_empty_state(client: AsyncClient) -> None:
    """GET /api/pattern_hits/html with no traffic returns the empty-state copy.

    The PatternCounters singleton is rebuilt by the lifespan on every
    fixture invocation, so the per-test snapshot starts empty. The
    partial's ``{% else %}`` branch renders the "No redactions yet."
    paragraph in that case.
    """
    resp = await client.get("/api/pattern_hits/html")
    assert resp.status_code == 200, resp.text
    body = resp.text
    # Heading is rendered from the partial regardless of state.
    assert "Pattern Hits" in body
    # No redactions were performed by this test, so the empty-state
    # paragraph is what we should see.
    assert "No redactions yet" in body


@pytest.mark.asyncio
async def test_static_htmx_is_served(client: AsyncClient) -> None:
    """GET /static/htmx.min.js returns 200 — the vendored asset is reachable.

    StaticFiles serves the bytes verbatim; we only assert the
    response status and a content-length sanity check (htmx 1.9.12
    minified is ~48 kB; we use a generous floor to detect a stub).
    """
    resp = await client.get("/static/htmx.min.js")
    assert resp.status_code == 200, resp.text
    # Generous floor: anything below 10 kB is almost certainly a stub.
    assert len(resp.content) > 10_000, (
        f"htmx.min.js seems too small ({len(resp.content)} bytes); "
        "is it a stub?"
    )
    # The version comment we prepended at vendoring time is the first
    # token in the file; assert its presence to confirm provenance.
    assert b"htmx 1.9.12" in resp.content[:200]


@pytest.mark.asyncio
async def test_static_dashboard_css_is_served(client: AsyncClient) -> None:
    """GET /static/dashboard.css returns 200 + serves the stylesheet.

    Sanity check on the static mount; we assert on a stable selector
    name (``stats-grid``) so future cosmetic changes can re-flow the
    rest of the file without breaking the test.
    """
    resp = await client.get("/static/dashboard.css")
    assert resp.status_code == 200, resp.text
    assert "stats-grid" in resp.text

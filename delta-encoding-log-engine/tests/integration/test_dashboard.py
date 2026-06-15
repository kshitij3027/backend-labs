"""TestClient integration tests for the live dashboard backend (Commit 11).

Drives the real wired app (``app.main:app``) through ``fastapi.testclient.TestClient``
under a ``with`` block so :func:`app.main.lifespan` runs — which both builds the shared
``app.state`` graph (settings + metrics + store + recon cache) AND starts the single
``broadcast_loop`` background task that fans a stats tick out to every WebSocket client.

Coverage:

* ``GET /`` serves the single-page dashboard (HTML, the right ``<title>``, the
  ``/static/dashboard.js`` reference, and the stat-card IDs the JS writes into).
* ``GET /static/dashboard.js`` / ``dashboard.css`` are served, non-empty, and the JS
  speaks WebSocket against ``/ws``.
* ``WS /ws`` delivers an **immediate** first ``tick`` on connect (so the page paints
  without waiting a full cadence), carrying ``refresh_ms`` plus either a three-section
  ``stats`` dict or a non-null ``error``; and after a real generate+compress the tick's
  ``stats.storage.count`` reflects the compressed batch.
* With ``DASHBOARD_REFRESH_MS`` cranked down, **periodic** ticks arrive (first immediate
  + at least one loop-driven), proving the background loop is actually ticking.
* ``GET /api/stats`` still returns the three-section shape — a regression guard for the
  ``compose_stats`` factoring this commit extracted from the handler.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.settings import get_settings

# Stat-card element IDs the dashboard JS writes into (from dashboard/templates/index.html).
# We assert a representative spread across all three sections.
STAT_CARD_IDS = (
    "stat-delta-reduction",
    "stat-count",
    "stat-reconstruct-p99",
    "stat-errors",
    "stat-cache-hitrate",
)


@pytest.fixture
def client():
    """Yield a TestClient with lifespan active; reset engine state before each test.

    The ``with TestClient(app)`` block triggers startup/shutdown so ``app.state`` is
    fully built and the broadcast loop is running. ``POST /api/reset`` zeroes the store +
    metrics + cache so a tick's counts/uptime don't leak between tests despite the
    process-wide singleton ``app`` state.
    """
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


# --------------------------------------------------------------------------- #
# GET / — the single-page dashboard.
# --------------------------------------------------------------------------- #
def test_index_serves_dashboard_html(client):
    """GET / -> 200 HTML carrying the title, the JS reference, and the stat-card IDs."""
    resp = client.get("/")
    assert resp.status_code == 200, resp.text
    assert "text/html" in resp.headers["content-type"].lower()

    body = resp.text
    # Title (full element so we know the page, not just a stray substring).
    assert "<title>Delta Encoding Log Engine</title>" in body
    # The page loads the client script that opens the WebSocket.
    assert "/static/dashboard.js" in body
    # And it must contain the stat-card mount points the JS targets.
    for stat_id in STAT_CARD_IDS:
        assert stat_id in body, f"missing stat-card id {stat_id!r} in index.html"


# --------------------------------------------------------------------------- #
# GET /static/* — the dashboard assets shipped in the image.
# --------------------------------------------------------------------------- #
def test_static_dashboard_js_served(client):
    """GET /static/dashboard.js -> 200, non-empty, and speaks WebSocket against /ws."""
    resp = client.get("/static/dashboard.js")
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert body.strip(), "dashboard.js is empty"
    assert "WebSocket" in body
    assert "/ws" in body


def test_static_dashboard_css_served(client):
    """GET /static/dashboard.css -> 200 and non-empty."""
    resp = client.get("/static/dashboard.css")
    assert resp.status_code == 200, resp.text
    assert resp.text.strip(), "dashboard.css is empty"


# --------------------------------------------------------------------------- #
# WS /ws — the immediate first tick.
# --------------------------------------------------------------------------- #
def test_ws_first_tick_envelope(client):
    """Connecting to /ws yields an immediate well-formed tick (stats dict or error)."""
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()

    assert msg["type"] == "tick"
    assert "refresh_ms" in msg
    assert isinstance(msg["refresh_ms"], int)

    # Either a healthy three-section stats document, or a surfaced (non-null) error.
    if msg.get("error") is not None:
        assert msg["stats"] is None
    else:
        stats = msg["stats"]
        assert isinstance(stats, dict)
        for section in ("storage", "performance", "system"):
            assert section in stats, f"tick stats missing {section!r}: {stats}"


def test_ws_first_tick_reflects_compressed_batch(client):
    """After generate+compress over REST, the next /ws tick's storage.count matches."""
    count = 300
    gresp = client.post("/api/generate", json={"count": count, "seed": 7})
    assert gresp.status_code == 200, gresp.text
    cresp = client.post("/api/compress", json={"use_generated": True})
    assert cresp.status_code == 200, cresp.text

    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()

    assert msg["type"] == "tick"
    assert msg.get("error") is None, f"unexpected error tick: {msg}"
    storage = msg["stats"]["storage"]
    assert storage["count"] == count, f"tick storage.count != {count}: {storage}"
    # Sanity: the batch really was compressed into keyframes (default interval 100).
    assert storage["keyframe_count"] >= 1
    # And the delta-reduction surfaced in the tick is a real percentage, not the
    # zeroed-store default.
    assert storage["delta_reduction"] > 0.0


# --------------------------------------------------------------------------- #
# WS /ws — periodic ticks (background loop is actually ticking).
# --------------------------------------------------------------------------- #
def test_ws_periodic_ticks_arrive():
    """With a fast refresh, /ws delivers >=2 ticks (immediate + >=1 periodic) quickly.

    Builds a *fresh* TestClient AFTER cranking ``DASHBOARD_REFRESH_MS`` down so the
    lifespan's broadcast loop runs on the short cadence rather than the 2000ms default.
    We never block on a sleep: ``receive_json`` returns as the loop pushes each tick, so
    the immediate tick plus one loop-driven tick land well inside the read timeout.
    """
    prev = os.environ.get("DASHBOARD_REFRESH_MS")
    os.environ["DASHBOARD_REFRESH_MS"] = "200"
    get_settings.cache_clear()  # force the lifespan to read the overridden cadence
    try:
        with TestClient(app) as c:
            c.post("/api/reset")
            # Confirm the override actually took on app.state (guards the test itself).
            assert app.state.settings.dashboard_refresh_ms == 200
            with c.websocket_connect("/ws") as ws:
                first = ws.receive_json()
                second = ws.receive_json()
        assert first["type"] == "tick"
        assert second["type"] == "tick"
        assert first["refresh_ms"] == 200
    finally:
        # Restore env + cache so other tests see the default 2000ms cadence again.
        if prev is None:
            os.environ.pop("DASHBOARD_REFRESH_MS", None)
        else:
            os.environ["DASHBOARD_REFRESH_MS"] = prev
        get_settings.cache_clear()


# --------------------------------------------------------------------------- #
# GET /api/stats — unchanged shape after the compose_stats refactor.
# --------------------------------------------------------------------------- #
def test_api_stats_shape_unchanged(client):
    """GET /api/stats still returns the three-section document (compose_stats guard)."""
    # Exercise a real flow so performance/cache sub-blocks are populated.
    client.post("/api/generate", json={"count": 200, "seed": 5})
    client.post("/api/compress", json={"use_generated": True})
    client.get("/api/logs/100")

    resp = client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    for section in ("storage", "performance", "system"):
        assert section in body, f"/api/stats missing {section!r}: {body}"

    # storage exposes the byte accounting the dashboard reads.
    assert "count" in body["storage"]
    assert "delta_reduction" in body["storage"]
    # performance folds the recon cache stats in (compose_stats behaviour).
    assert "cache" in body["performance"]
    assert "reconstruct_p99_ms" in body["performance"]
    # system carries the error-gate triple.
    assert "errors" in body["system"]
    assert body["system"]["errors"] == 0

"""Integration tests for the live dashboard FRONTEND (:mod:`src.dashboard` + assets).

These prove the *presentation layer* is wired and served end-to-end by the real
Flask app: the ``/`` route renders ``templates/index.html`` (not the old stub), and
the vendored JS/CSS assets are served by Flask's static handler from ``static/``.

A genuine :class:`~src.orchestrator.Orchestrator` sits behind a real
``create_app(..., async_mode="threading")`` so no eventlet server is required.

The assertions are deliberately *contract* level:

* the rendered ``/`` body references the three vendored/app scripts and carries the
  stable element ids the JS (and the later Chrome UI test) depend on;
* the vendored bundles are non-trivial in size — proof they are the real libraries
  (not an HTML error page or an empty placeholder), since the Docker E2E has no
  internet to fall back to a CDN.
"""

import pytest

from src.config import Settings
from src.dashboard import create_app
from src.orchestrator import Orchestrator


def _config(**overrides) -> Settings:
    """A fast, deterministic Settings for the frontend wiring tests."""
    params = dict(
        cooldown_period_seconds=0.0,
        scale_down_cooldown_seconds=0.0,
        monitoring_interval_seconds=5.0,
        orchestration_interval_seconds=5.0,
        min_workers=2,
        max_workers=20,
    )
    params.update(overrides)
    return Settings(**params)


@pytest.fixture
def client():
    """A Flask test client over a real orchestrator in threading mode."""
    config = _config()
    app, _socketio = create_app(config, Orchestrator(config), async_mode="threading")
    app.config["TESTING"] = True
    return app.test_client()


# --------------------------------------------------------------------------- #
# The rendered dashboard page
# --------------------------------------------------------------------------- #
def test_index_renders_real_template(client):
    """GET / returns 200 and the REAL dashboard page (title + not the API stub)."""
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # The real template title is present...
    assert "Adaptive Resource Allocation" in body
    # ...and it is NOT the minimal "UI not yet built" stub from src/dashboard.py.
    assert "Dashboard UI not yet built" not in body


def test_index_references_vendored_scripts(client):
    """The page loads the two vendored libs + the app JS by absolute static path."""
    body = client.get("/").get_data(as_text=True)
    assert "/static/js/chart.umd.min.js" in body
    assert "/static/js/socket.io.min.js" in body
    assert "/static/js/dashboard.js" in body
    assert "/static/css/style.css" in body


def test_index_has_stable_element_ids(client):
    """The page exposes the stable ids the JS + the Chrome UI test bind to."""
    body = client.get("/").get_data(as_text=True)
    expected_ids = [
        # connection pill
        "conn-status",
        # representative stat cards
        "stat-workers",
        "stat-utilization",
        "stat-cpu",
        "stat-memory",
        "stat-queue",
        "stat-latency",
        "stat-arrival",
        "stat-forecast",
        "stat-decision",
        "stat-cooldown",
        "stat-anomaly",
        "stat-cost",
        # charts
        "chart-utilization",
        "chart-cpu-mem",
        "chart-workers",
        "chart-queue-latency",
        # controls
        "btn-scale-up",
        "btn-scale-down",
        "load-rate",
        "load-seconds",
        "btn-inject-load",
        # history list
        "scaling-history",
    ]
    for el_id in expected_ids:
        assert f'id="{el_id}"' in body, f"missing element id={el_id!r} in index.html"


# --------------------------------------------------------------------------- #
# Vendored static assets served by Flask
# --------------------------------------------------------------------------- #
def test_vendored_chartjs_served(client):
    """GET /static/js/chart.umd.min.js returns 200 and a non-trivial real bundle."""
    resp = client.get("/static/js/chart.umd.min.js")
    assert resp.status_code == 200
    body = resp.get_data()
    # Chart.js 4.4.1 UMD is ~200KB; assert well above any stub/error-page size.
    assert len(body) > 50000
    # Sanity: a JS bundle, not an HTML error page.
    assert b"<!doctype" not in body[:200].lower()
    assert b"<html" not in body[:200].lower()


def test_vendored_socketio_served(client):
    """GET /static/js/socket.io.min.js returns 200 and a non-trivial real bundle."""
    resp = client.get("/static/js/socket.io.min.js")
    assert resp.status_code == 200
    body = resp.get_data()
    # Socket.IO 4.7.5 client is ~40-50KB.
    assert len(body) > 30000
    assert b"<!doctype" not in body[:200].lower()
    assert b"<html" not in body[:200].lower()


def test_dashboard_js_served(client):
    """GET /static/js/dashboard.js returns 200 (the app's own client script)."""
    resp = client.get("/static/js/dashboard.js")
    assert resp.status_code == 200
    body = resp.get_data()
    assert len(body) > 1000
    # It is our IIFE client, not an error page.
    assert b"use strict" in body


def test_stylesheet_served(client):
    """GET /static/css/style.css returns 200 with real CSS."""
    resp = client.get("/static/css/style.css")
    assert resp.status_code == 200
    body = resp.get_data()
    assert len(body) > 500
    assert b"{" in body and b"}" in body

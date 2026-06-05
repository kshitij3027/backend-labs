"""Integration tests for the live monitoring dashboard (Commit 22).

Exercises the static single-page dashboard the app serves alongside the
``/ws`` live-metrics stream, all against the wired FastAPI app via the shared
``client`` fixture:

* ``GET /`` serves the dashboard HTML (right content-type, page title, and the
  three local asset references the page bootstraps from);
* every stable element id the client patches (``dashboard.js`` looks them up by
  id) is present in the served markup, so the wiring can't silently drift;
* the static assets themselves (vendored Chart.js, the dashboard JS + CSS) serve
  ``200`` — and Chart.js is genuinely the large vendored bundle, not a stub;
* ``dashboard.js`` actually consumes the tick contract — it wires the ``/ws``
  endpoint and references the card + chart canvas ids it renders into;
* and the page pulls Chart.js **locally** (no CDN ``<script>``/``<link>``), so
  the dashboard renders with no outbound network dependency.

These are pure HTTP/asset-surface checks (no WebSocket frames — that contract is
covered in ``test_websocket.py``), so the default ``client`` fixture is enough.
"""
from __future__ import annotations

# Every stable id the dashboard markup must expose so dashboard.js can patch it:
# the six metric cards, the connection badge, the seven chart canvases, and the
# migration activity list. Kept in one list so a single missing id is reported
# precisely rather than failing an opaque substring check.
_CARD_IDS = [
    "m-total-storage",
    "m-compression-ratio",
    "m-active-partitions",
    "m-ingest-eps",
    "m-analytical-speedup",
    "m-migrations-completed",
]
_CHART_IDS = [
    "formatChart",
    "tenantChart",
    "migrationChart",
    "latencyByFormatChart",
    "storageChart",
    "indexChart",
    "tierChart",
]
_STABLE_IDS = _CARD_IDS + ["conn-status"] + _CHART_IDS + ["migration-log"]

# Local asset references the served HTML bootstraps from (no CDN equivalents).
_LOCAL_ASSETS = ["/static/chart.min.js", "/static/dashboard.js", "/static/dashboard.css"]

# CDN hosts that must never appear in the markup — Chart.js is vendored locally.
_CDN_HOSTS = ["cdn.jsdelivr", "unpkg.com", "cdnjs"]


# --------------------------------------------------------------------------- #
# 1. GET / serves the dashboard HTML.
# --------------------------------------------------------------------------- #
def test_root_serves_dashboard_html(client):
    """``GET /`` returns the dashboard page: 200, HTML, title + local assets."""
    r = client.get("/")
    assert r.status_code == 200, r.text
    assert "text/html" in r.headers["content-type"], r.headers["content-type"]
    # The page title text identifies the dashboard.
    assert "Adaptive Storage Format Optimizer" in r.text
    # It bootstraps from the three vendored local assets (not a CDN).
    for asset in _LOCAL_ASSETS:
        assert asset in r.text, f"served HTML missing reference to {asset}"


# --------------------------------------------------------------------------- #
# 2. Every stable element id is present in the served markup.
# --------------------------------------------------------------------------- #
def test_dashboard_html_has_all_stable_ids(client):
    """All card / badge / chart / log ids dashboard.js patches are in the HTML."""
    html = client.get("/").text
    missing = [eid for eid in _STABLE_IDS if eid not in html]
    assert not missing, f"dashboard HTML is missing stable element ids: {missing}"


# --------------------------------------------------------------------------- #
# 3. The static assets serve 200 (and Chart.js is the real vendored bundle).
# --------------------------------------------------------------------------- #
def test_static_assets_serve(client):
    """Vendored Chart.js, dashboard.js and dashboard.css all serve ``200``.

    Chart.js must also be the genuine ~205KB vendored bundle, not an empty or
    stub file, so the page can actually render charts offline.
    """
    chart = client.get("/static/chart.min.js")
    assert chart.status_code == 200, chart.text
    assert len(chart.content) > 100_000, (
        f"vendored chart.min.js is only {len(chart.content)} bytes — looks like a "
        "stub, not the real bundle"
    )

    js = client.get("/static/dashboard.js")
    assert js.status_code == 200, js.text

    css = client.get("/static/dashboard.css")
    assert css.status_code == 200, css.text


# --------------------------------------------------------------------------- #
# 4. dashboard.js consumes the tick contract: wires /ws and the rendered ids.
# --------------------------------------------------------------------------- #
def test_dashboard_js_wires_ws_and_ids(client):
    """``dashboard.js`` references ``/ws`` plus the card + chart ids it renders.

    Proves the client actually consumes the tick payload — it derives the WS URL
    from ``/ws`` and looks the metric cards / chart canvases up by the same
    stable ids the markup exposes, so the served page and its script agree.
    """
    js = client.get("/static/dashboard.js").text
    assert "/ws" in js, "dashboard.js does not reference the /ws WebSocket endpoint"
    # It patches the metric cards by id...
    for cid in _CARD_IDS:
        assert cid in js, f"dashboard.js never references card id {cid}"
    # ...and renders into the chart canvases by id.
    for cid in _CHART_IDS:
        assert cid in js, f"dashboard.js never references chart id {cid}"


# --------------------------------------------------------------------------- #
# 5. No CDN: Chart.js (and friends) must be served from the vendored local file.
# --------------------------------------------------------------------------- #
def test_dashboard_has_no_cdn_references(client):
    """The served HTML pulls all libraries locally — no CDN host appears."""
    html = client.get("/").text
    leaked = [host for host in _CDN_HOSTS if host in html]
    assert not leaked, f"dashboard HTML references CDN host(s) instead of vendoring: {leaked}"

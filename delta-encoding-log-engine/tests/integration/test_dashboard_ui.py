"""TestClient integration tests for the dashboard UI assets (Commit 12).

Commit 12 turns the cards-only dashboard into a full UI: two live Chart.js line charts
(``#chart-reduction`` / ``#chart-latency``) plus interactive control forms (generate /
compress / reconstruct / random-access / reset). Chart.js is **vendored** locally
(``dashboard/static/chart.umd.min.js``, ~200KB) — there is intentionally NO CDN.

We can't render JS here, so these tests assert the SERVED content instead: that the page
ships the local Chart.js tag (before ``dashboard.js``), every canvas + form-control mount
point the JS targets, the stat cards, and crucially that no remote ``http(s)://`` CDN
pulls Chart.js. Then we prove the assets themselves are real (the vendored bundle is a
true ~200KB file, not a stub/redirect), the JS references the globals + endpoints it
drives, and the CSS is non-empty. Finally a live-API sanity pass exercises the same REST
targets the forms ``fetch`` (generate → compress → reconstruct), so the form wiring is
known to point at valid, working endpoints.

Driven through ``fastapi.testclient.TestClient`` over the real wired ``app.main:app``
under a ``with`` block so :func:`app.main.lifespan` builds ``app.state`` and starts the
broadcast loop. Run **inside Docker only** (``Dockerfile.test``) — no host pytest.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app

# Canvas mount points the dashboard JS draws the two live charts into.
CANVAS_IDS = ("chart-reduction", "chart-latency")

# Every interactive control mount point wired in dashboard.js (buttons, inputs, result
# spans, and the random-access <pre>). If any of these is missing the form is dead.
FORM_CONTROL_IDS = (
    "gen-count",
    "btn-generate",
    "generate-result",
    "btn-compress",
    "compress-result",
    "btn-reconstruct",
    "reconstruct-result",
    "logs-index",
    "btn-logs",
    "logs-result",
    "btn-reset",
    "reset-result",
)

# A representative spread of the stat-card IDs the tick handler writes into.
STAT_CARD_IDS = ("stat-delta-reduction", "stat-reconstruct-p99")

# The local (vendored, no-CDN) Chart.js bundle and the dashboard client script.
CHART_SRC = "/static/chart.umd.min.js"
DASHBOARD_JS_SRC = "/static/dashboard.js"


@pytest.fixture
def client():
    """Yield a TestClient with lifespan active; reset engine state before each test.

    The ``with TestClient(app)`` block runs startup/shutdown so ``app.state`` is fully
    built (settings + metrics + store + recon cache) and the broadcast loop is ticking.
    ``POST /api/reset`` zeroes the store/metrics/cache so the live-API sanity test starts
    from a clean slate despite the process-wide singleton ``app`` state.
    """
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


# --------------------------------------------------------------------------- #
# GET / — the page ships the local Chart.js, both canvases, and every control.
# --------------------------------------------------------------------------- #
def test_index_loads_local_chartjs_before_dashboard_js(client):
    """GET / references the vendored Chart.js, and it loads BEFORE dashboard.js.

    Ordering matters: dashboard.js reads the global ``Chart`` on boot, so the bundle's
    script tag must appear earlier in the document than the client script's tag.
    """
    resp = client.get("/")
    assert resp.status_code == 200, resp.text
    assert "text/html" in resp.headers["content-type"].lower()
    body = resp.text

    assert CHART_SRC in body, f"missing local Chart.js script ref {CHART_SRC!r}"
    assert DASHBOARD_JS_SRC in body, f"missing client script ref {DASHBOARD_JS_SRC!r}"

    chart_at = body.index(CHART_SRC)
    dash_at = body.index(DASHBOARD_JS_SRC)
    assert chart_at < dash_at, (
        f"Chart.js ({CHART_SRC} @ {chart_at}) must load before "
        f"dashboard.js ({DASHBOARD_JS_SRC} @ {dash_at})"
    )


def test_index_has_both_chart_canvases(client):
    """GET / contains both live-chart <canvas> mount points by id."""
    body = client.get("/").text
    for canvas_id in CANVAS_IDS:
        assert f'id="{canvas_id}"' in body, f"missing canvas id {canvas_id!r} in index.html"


def test_index_has_all_form_controls(client):
    """GET / contains every interactive control mount point the JS wires."""
    body = client.get("/").text
    for control_id in FORM_CONTROL_IDS:
        assert f'id="{control_id}"' in body, (
            f"missing form-control id {control_id!r} in index.html"
        )


def test_index_has_stat_cards(client):
    """GET / still carries the representative stat-card mount points."""
    body = client.get("/").text
    for stat_id in STAT_CARD_IDS:
        assert f'id="{stat_id}"' in body, f"missing stat-card id {stat_id!r} in index.html"


def test_index_has_no_cdn_chartjs_reference(client):
    """GET / must NOT pull Chart.js from a remote CDN — the only ref is the local bundle.

    Asserts there is no ``http://`` / ``https://`` URL in the page at all (the page is
    fully same-origin: it links only ``/static/*`` assets), which proves Chart.js comes
    from the vendored ``/static/chart.umd.min.js`` and never from a CDN. Also guards the
    common ``cdn``/``jsdelivr``/``unpkg``/``cdnjs`` script hosts explicitly.
    """
    body = client.get("/").text

    # No absolute remote URLs anywhere in the served page.
    assert "http://" not in body, "page contains an http:// URL (should be same-origin only)"
    assert "https://" not in body, (
        "page contains an https:// URL (Chart.js must be vendored, not from a CDN)"
    )

    # Belt-and-suspenders: none of the usual CDN hosts appear.
    lowered = body.lower()
    for cdn_token in ("cdn.jsdelivr", "unpkg.com", "cdnjs.cloudflare", "cdn.skypack"):
        assert cdn_token not in lowered, f"page references CDN host {cdn_token!r}"

    # And the only Chart.js script source is the local vendored bundle.
    assert CHART_SRC in body


# --------------------------------------------------------------------------- #
# GET /static/chart.umd.min.js — the REAL vendored bundle, not a stub.
# --------------------------------------------------------------------------- #
def test_static_chartjs_is_real_vendored_bundle(client):
    """GET /static/chart.umd.min.js -> 200 and is a real ~200KB file (not a stub/redirect).

    A length over 100KB proves it is the genuine vendored Chart.js UMD build rather than
    an empty placeholder, a redirect stub, or a CDN-shim shim. ``follow_redirects`` stays
    default-off intent: a 200 (not 3xx) is required since we assert on the body bytes.
    """
    resp = client.get(CHART_SRC)
    assert resp.status_code == 200, resp.text
    size = len(resp.content)
    assert size > 100_000, (
        f"{CHART_SRC} is only {size} bytes — expected the real ~200KB vendored "
        f"Chart.js bundle, not a stub/redirect"
    )


# --------------------------------------------------------------------------- #
# GET /static/dashboard.js — references the globals + endpoints it drives.
# --------------------------------------------------------------------------- #
def test_static_dashboard_js_wires_chart_and_endpoints(client):
    """GET /static/dashboard.js -> 200 and references Chart, every API path, DOMContentLoaded."""
    resp = client.get(DASHBOARD_JS_SRC)
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert body.strip(), "dashboard.js is empty"

    # Uses the (vendored) global chart constructor.
    assert "Chart" in body, "dashboard.js does not reference the Chart global"
    # Wires every same-origin endpoint the controls fetch.
    for endpoint in (
        "/api/generate",
        "/api/compress",
        "/api/reconstruct",
        "/api/logs/",
        "/api/reset",
    ):
        assert endpoint in body, f"dashboard.js missing endpoint reference {endpoint!r}"
    # Boots off DOMContentLoaded (charts + controls + socket wired after parse).
    assert "DOMContentLoaded" in body, "dashboard.js does not boot on DOMContentLoaded"


# --------------------------------------------------------------------------- #
# GET /static/dashboard.css — served and non-empty.
# --------------------------------------------------------------------------- #
def test_static_dashboard_css_served(client):
    """GET /static/dashboard.css -> 200 and non-empty."""
    resp = client.get("/static/dashboard.css")
    assert resp.status_code == 200, resp.text
    assert resp.text.strip(), "dashboard.css is empty"


# --------------------------------------------------------------------------- #
# Live API sanity — the REST targets the forms fetch actually work.
# --------------------------------------------------------------------------- #
def test_form_target_endpoints_work_end_to_end(client):
    """generate -> compress -> reconstruct over REST all 200 with the fields the forms read.

    Proves the form ``fetch`` targets are valid and return the exact keys the dashboard
    JS pulls out: ``/api/compress`` yields ``delta_reduction`` and ``/api/reconstruct``
    with ``verify`` yields ``fidelity_ok == True``. This is the server-side half of the
    interactive controls (the browser half is covered in the dedicated Chrome UI phase).
    """
    # Generate (btn-generate -> POST /api/generate {count}).
    gresp = client.post("/api/generate", json={"count": 50})
    assert gresp.status_code == 200, gresp.text
    assert gresp.json()["count"] == 50

    # Compress (btn-compress -> POST /api/compress {use_generated:true}).
    cresp = client.post("/api/compress", json={"use_generated": True})
    assert cresp.status_code == 200, cresp.text
    cbody = cresp.json()
    assert "delta_reduction" in cbody, f"compress response missing delta_reduction: {cbody}"

    # Reconstruct & verify (btn-reconstruct -> POST /api/reconstruct {verify:true}).
    rresp = client.post("/api/reconstruct", json={"verify": True})
    assert rresp.status_code == 200, rresp.text
    rbody = rresp.json()
    assert rbody["fidelity_ok"] is True, f"reconstruct fidelity not OK: {rbody}"

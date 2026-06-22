"""Integration tests for live metrics: ``GET /metrics`` + the ``/ws/metrics`` feed (Commit 10).

Exercises the real Commit-10 HTTP/WebSocket surface of
:func:`src.api.create_app` through Starlette's
:class:`~fastapi.testclient.TestClient`:

* ``GET /metrics`` — the REST mirror of the aggregator snapshot (shape, and that it
  reflects classifies done through ``POST /classify``).
* ``WS /ws/metrics`` — the dashboard feed: a snapshot is painted on connect, and the
  background broadcaster pushes fresh snapshots that reflect newly-classified logs.
* ``/stats`` and ``/metrics`` agree on ``total_classified`` (single source of truth).

As in the other integration modules, a module-scoped ``client`` injects a tiny
config (``rf_n_estimators=5``, ``gb_n_estimators=5``) and an isolated tmp
``model_dir`` so first boot trains a small model exactly once; the ``with
TestClient(app)`` block drives the FastAPI lifespan so the model is loaded and the
broadcaster task is running before any request. The module also shortens
``src.api.BROADCAST_INTERVAL_SEC`` to ~0.1s so the WS broadcaster-tick assertions
do not wait a full second per read.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import src.api as api
from src.api import create_app
from src.config import Settings

# The spec's canonical input: must classify to ERROR / SYSTEM (used to assert the
# metrics distributions reflect real classifications).
CANONICAL_LOG = "Database connection failed with timeout error"
EXPECTED_SEVERITY = "ERROR"
EXPECTED_CATEGORY = "SYSTEM"

# The exact public snapshot contract (matches MetricsAggregator.snapshot()).
SNAPSHOT_KEYS = {
    "total_classified",
    "severity_distribution",
    "category_distribution",
    "service_distribution",
    "avg_confidence",
    "throughput_per_sec",
    "recent_predictions",
    "model_status",
    "current_version",
    "uptime_sec",
}

SAMPLE_LOGS = [
    "Database connection failed with timeout error",
    "GET /api/users 200 OK in 12ms",
    "User login successful for account 42",
    "Disk usage at 95% on /var partition",
]


@pytest.fixture(scope="module", autouse=True)
def fast_broadcast():
    """Shorten the broadcaster interval module-wide so WS-tick reads are fast.

    The default 1.0s would make every "read the next broadcaster tick" assertion
    wait a full second; 0.1s keeps the tests snappy while still going through the
    real periodic-broadcast path. Restored after the module runs.
    """
    original = api.BROADCAST_INTERVAL_SEC
    api.BROADCAST_INTERVAL_SEC = 0.1
    yield
    api.BROADCAST_INTERVAL_SEC = original


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny model once at startup.

    Tiny estimators + an isolated tmp ``model_dir`` keep first-boot training fast;
    the ``with`` block drives the lifespan so a ready model is loaded and the
    metrics broadcaster task is running before the first request.
    """
    model_dir = tmp_path_factory.mktemp("models")
    app = create_app(
        Settings(rf_n_estimators=5, gb_n_estimators=5, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


# --------------------------------------------------------------------------- #
# GET /metrics — REST mirror
# --------------------------------------------------------------------------- #


def test_metrics_endpoint_shape_and_ready(client):
    """``GET /metrics`` returns 200 with the full snapshot shape and ready status."""
    resp = client.get("/metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == SNAPSHOT_KEYS, f"unexpected metrics keys: {sorted(body)}"
    # Startup loaded/trained a model, so the aggregator was seeded "ready".
    assert body["model_status"] == "ready"
    assert isinstance(body["total_classified"], int)
    assert isinstance(body["throughput_per_sec"], (int, float))


def test_metrics_reflects_classify(client):
    """After a classify, ``/metrics`` total rises and recent_predictions is populated."""
    before = client.get("/metrics").json()["total_classified"]

    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text

    snap = client.get("/metrics").json()
    assert snap["total_classified"] == before + 1
    # The recent feed now holds at least the canonical prediction we just made.
    recent = snap["recent_predictions"]
    assert len(recent) >= 1
    assert any(
        e["severity"] == EXPECTED_SEVERITY and e["category"] == EXPECTED_CATEGORY
        for e in recent
    ), f"canonical prediction not found in recent feed: {recent}"


def test_metrics_distributions_reflect_classifies(client):
    """Severity/category distributions in ``/metrics`` grow with classifies."""
    before = client.get("/metrics").json()
    before_sev = before["severity_distribution"].get(EXPECTED_SEVERITY, 0)
    before_cat = before["category_distribution"].get(EXPECTED_CATEGORY, 0)

    n = 3
    for _ in range(n):
        resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
        assert resp.status_code == 200, resp.text

    after = client.get("/metrics").json()
    # Both distributions are plain {label: count} dicts that grew by n.
    assert after["severity_distribution"].get(EXPECTED_SEVERITY, 0) == before_sev + n
    assert after["category_distribution"].get(EXPECTED_CATEGORY, 0) == before_cat + n


def test_stats_total_matches_metrics_total(client):
    """``/stats`` and ``/metrics`` agree on total_classified (single source of truth)."""
    # Do a few classifies so the count is non-trivial.
    for log in SAMPLE_LOGS:
        resp = client.post("/classify", json={"raw_log": log})
        assert resp.status_code == 200, resp.text

    stats_total = client.get("/stats").json()["total_classified"]
    metrics_total = client.get("/metrics").json()["total_classified"]
    assert stats_total == metrics_total, (
        f"/stats={stats_total} but /metrics={metrics_total} (must be one source)"
    )


# --------------------------------------------------------------------------- #
# WS /ws/metrics — paint-on-connect + live broadcaster
# --------------------------------------------------------------------------- #


def test_ws_paint_on_connect(client):
    """Connecting to ``/ws/metrics`` immediately yields one snapshot with full shape."""
    with client.websocket_connect("/ws/metrics") as ws:
        snap = ws.receive_json()
    assert set(snap) == SNAPSHOT_KEYS, f"unexpected WS snapshot keys: {sorted(snap)}"
    assert snap["model_status"] == "ready"


def test_ws_reflects_updates(client):
    """A WS broadcaster tick reflects classifies done after the initial snapshot."""
    with client.websocket_connect("/ws/metrics") as ws:
        # Paint-on-connect snapshot first.
        initial = ws.receive_json()
        initial_total = initial["total_classified"]

        # Classify a few logs *after* connecting.
        for log in SAMPLE_LOGS:
            resp = client.post("/classify", json={"raw_log": log})
            assert resp.status_code == 200, resp.text

        # Read broadcaster ticks until the total reflects the new classifies. The
        # broadcaster interval was shortened to 0.1s, so a couple of reads suffice;
        # we bound the loop to avoid hanging if something is wrong.
        latest = initial
        for _ in range(50):
            latest = ws.receive_json()
            if latest["total_classified"] >= initial_total + len(SAMPLE_LOGS):
                break

    assert latest["total_classified"] >= initial_total + len(SAMPLE_LOGS), (
        f"WS total did not rise: started {initial_total}, last {latest['total_classified']}"
    )


def test_ws_snapshot_matches_metrics_total(client):
    """The WS paint-on-connect snapshot agrees with the ``/metrics`` REST total."""
    # Establish a known count first.
    for log in SAMPLE_LOGS:
        resp = client.post("/classify", json={"raw_log": log})
        assert resp.status_code == 200, resp.text

    rest_total = client.get("/metrics").json()["total_classified"]
    with client.websocket_connect("/ws/metrics") as ws:
        ws_total = ws.receive_json()["total_classified"]

    # Same aggregator backs both surfaces; the WS paint is taken at/after the REST
    # read, so it can only be >= (never behind).
    assert ws_total >= rest_total

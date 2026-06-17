"""Integration tests for the Flask + SocketIO dashboard (:mod:`src.dashboard`).

These exercise the real wiring: a genuine :class:`~src.orchestrator.Orchestrator`
(driven by deterministic simulated time) sits behind the app, and we hit it through
Flask's :meth:`~flask.Flask.test_client` for HTTP and SocketIO's ``test_client`` for
WebSocket. ``async_mode="threading"`` is used throughout so no eventlet server is
required to run the suite.

The HTTP-layer assertions focus on *contract*: status codes, the 503/400 error
idioms, and the shape of the JSON payloads — the deeper scaling behaviour itself is
covered by ``tests/integration/test_orchestrator.py``.
"""

from unittest.mock import MagicMock

import pytest

from src.config import Settings
from src.dashboard import create_app, start_background_tasks
from src.orchestrator import Orchestrator


# A deterministic, far-from-midnight start time so the time-of-day factor is stable
# across the short simulated windows used below.
T0 = 1_000_000.0


def fast_config(**overrides) -> Settings:
    """Build a :class:`Settings` tuned for fast, deterministic dashboard tests.

    Cooldowns are zeroed so manual scales never collide with a damping hold, and the
    workload numbers keep baseline utilization negligible.
    """
    params = dict(
        cooldown_period_seconds=0.0,
        scale_down_cooldown_seconds=0.0,
        monitoring_interval_seconds=5.0,
        orchestration_interval_seconds=5.0,
        min_workers=2,
        max_workers=20,
        capacity_per_worker=400.0,
        base_arrival_rate=100.0,
    )
    params.update(overrides)
    return Settings(**params)


@pytest.fixture
def orchestrator() -> Orchestrator:
    """A real, freshly-wired orchestrator (no ticks run yet)."""
    return Orchestrator(fast_config())


@pytest.fixture
def app_and_socketio(orchestrator):
    """Create the app + SocketIO in threading mode over the real orchestrator."""
    config = orchestrator.config
    app, socketio = create_app(config, orchestrator, async_mode="threading")
    app.config["TESTING"] = True
    return app, socketio


@pytest.fixture
def client(app_and_socketio):
    """A Flask test client for HTTP assertions."""
    app, _ = app_and_socketio
    return app.test_client()


# --------------------------------------------------------------------------- #
# Health + index
# --------------------------------------------------------------------------- #
def test_health_ok(client):
    """GET /health returns 200 with the documented service identity."""
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "healthy"
    assert body["service"] == "adaptive-resource-allocation"


def test_index_returns_html(client):
    """GET / returns 200 HTML (the inline stub when the template is absent)."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"<html" in resp.data.lower()


# --------------------------------------------------------------------------- #
# /api/status
# --------------------------------------------------------------------------- #
def test_api_status_shape(client, orchestrator):
    """GET /api/status returns the full snapshot schema after one paired tick."""
    # Populate metrics + a forecast/decision so the payload is non-trivial.
    orchestrator.collector_tick(now=T0)
    orchestrator.orchestration_tick(now=T0)

    resp = client.get("/api/status")
    assert resp.status_code == 200
    body = resp.get_json()
    for key in (
        "current_metrics",
        "forecast",
        "workers",
        "last_decision",
        "scaling_history",
        "anomaly",
        "cost",
    ):
        assert key in body
    assert body["current_metrics"]  # non-empty after a collector tick
    assert body["workers"]["backend"] == "simulated"


# --------------------------------------------------------------------------- #
# /api/metrics
# --------------------------------------------------------------------------- #
def test_api_metrics_shape(client, orchestrator):
    """GET /api/metrics returns current_metrics plus a per-field series map."""
    orchestrator.collector_tick(now=T0)

    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    body = resp.get_json()
    assert "current_metrics" in body
    assert "series" in body
    assert "workers_series" in body
    # The documented plotted fields are all present as lists.
    for field in (
        "cpu_percent",
        "memory_percent",
        "effective_utilization",
        "queue_depth",
        "latency_ms",
        "arrival_rate",
    ):
        assert field in body["series"]
        assert isinstance(body["series"][field], list)


def test_api_metrics_empty_history_is_defensive(client):
    """GET /api/metrics before any tick still returns empty series, not a 500."""
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["current_metrics"] == {}
    assert all(body["series"][f] == [] for f in body["series"])


# --------------------------------------------------------------------------- #
# /api/scaling
# --------------------------------------------------------------------------- #
def test_api_scaling_direction_up(client, orchestrator):
    """POST /api/scaling {"direction":"up"} adds one worker, reason 'manual'."""
    before = orchestrator.pool.current()

    resp = client.post("/api/scaling", json={"direction": "up"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["reason"] == "manual"
    assert body["action"] == "scale_up"
    assert orchestrator.pool.current() == before + 1


def test_api_scaling_target(client, orchestrator):
    """POST /api/scaling {"target":7} sets the pool to exactly 7 workers."""
    resp = client.post("/api/scaling", json={"target": 7})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["to_workers"] == 7
    assert orchestrator.pool.current() == 7


def test_api_scaling_missing_args_400(client):
    """POST /api/scaling with neither direction nor target is a 400."""
    resp = client.post("/api/scaling", json={})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_api_scaling_bad_direction_400(client):
    """POST /api/scaling with an invalid direction is a 400."""
    resp = client.post("/api/scaling", json={"direction": "sideways"})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# --------------------------------------------------------------------------- #
# /api/load
# --------------------------------------------------------------------------- #
def test_api_load_ramps(client, orchestrator):
    """POST /api/load injects a ramp and confirms the target/seconds back."""
    resp = client.post("/api/load", json={"arrival_rate": 9000, "ramp_seconds": 5})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ramping"
    assert body["target_arrival_rate"] == 9000
    assert body["ramp_seconds"] == 5

    # Verify the ramp actually took effect. The endpoint calls ramp() with no
    # now=, so it anchors to the REAL wall clock — NOT the simulated T0 (~1970)
    # clock used elsewhere in this suite. Evaluating against T0 here is the flaky
    # bug: T0 is ~780M seconds before the ramp start, so frac clamps to 0 and the
    # model returns base*time_of_day(real_now), which dips below base overnight.
    # Instead, evaluate just AFTER the ramp completes on the same real clock the
    # endpoint used: once frac == 1 the ramp override returns the target exactly,
    # which is deterministic regardless of time-of-day.
    import time

    future = time.time() + 5 + 1  # ramp_seconds (5) + 1s margin past completion
    assert abs(orchestrator.load_model.arrival_rate(now=future) - 9000) < 1e-6


def test_api_load_default_ramp_seconds(client):
    """ramp_seconds is optional and defaults to 10."""
    resp = client.post("/api/load", json={"arrival_rate": 1000})
    assert resp.status_code == 200
    assert resp.get_json()["ramp_seconds"] == 10


def test_api_load_negative_rate_400(client):
    """A negative arrival_rate is rejected with 400."""
    resp = client.post("/api/load", json={"arrival_rate": -5})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_api_load_missing_rate_400(client):
    """A missing arrival_rate is rejected with 400."""
    resp = client.post("/api/load", json={})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# --------------------------------------------------------------------------- #
# 503-when-missing idiom
# --------------------------------------------------------------------------- #
def test_api_status_503_when_orchestrator_missing():
    """With no orchestrator wired, /api/status degrades to a clean 503."""
    app, _ = create_app(fast_config(), None, async_mode="threading")
    resp = app.test_client().get("/api/status")
    assert resp.status_code == 503
    assert resp.get_json()["error"] == "orchestrator unavailable"


def test_api_metrics_503_when_orchestrator_popped():
    """Popping the orchestrator after construction also yields a 503."""
    config = fast_config()
    app, _ = create_app(config, Orchestrator(config), async_mode="threading")
    app.config["ORCHESTRATOR"] = None
    resp = app.test_client().get("/api/metrics")
    assert resp.status_code == 503


# --------------------------------------------------------------------------- #
# SocketIO connect → immediate snapshot
# --------------------------------------------------------------------------- #
def test_socketio_emits_snapshot_on_connect(app_and_socketio, orchestrator):
    """On connect the client receives both a status_update and a metrics_update."""
    app, socketio = app_and_socketio
    # Give the orchestrator some state so the emitted snapshot is meaningful.
    orchestrator.collector_tick(now=T0)
    orchestrator.orchestration_tick(now=T0)

    socketio_client = socketio.test_client(app)
    assert socketio_client.is_connected()

    received = socketio_client.get_received()
    event_names = {msg["name"] for msg in received}
    assert "status_update" in event_names
    assert "metrics_update" in event_names

    socketio_client.disconnect()


# --------------------------------------------------------------------------- #
# Background tasks wiring (no loops actually run)
# --------------------------------------------------------------------------- #
def test_start_background_tasks_schedules_two_loops(app_and_socketio, orchestrator):
    """start_background_tasks schedules both loops without running them.

    A mock SocketIO captures the scheduling: ``start_background_task`` must be called
    for each of the two loops (metrics + orchestration). The loop callables are never
    invoked here, so no real cadence executes.
    """
    app, _ = app_and_socketio
    fake_socketio = MagicMock()

    start_background_tasks(fake_socketio, app, orchestrator, orchestrator.config)

    assert fake_socketio.start_background_task.call_count >= 2
    # Each scheduled target is a callable (the loop closure), not yet executed.
    for call in fake_socketio.start_background_task.call_args_list:
        target = call.args[0]
        assert callable(target)

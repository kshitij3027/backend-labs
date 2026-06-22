"""Integration tests for the adaptive learning loop's HTTP surface (Commit 12).

Drives the real wired app (:func:`src.api.create_app`) through Starlette's
:class:`~fastapi.testclient.TestClient`, so the FastAPI lifespan trains a tiny
``v1`` (base + multi-service) before any request, then exercises the full
feedback → drift → graceful-retrain → re-arm flow over the live HTTP contract:

* ``GET /adaptive/status`` — the drift-monitor snapshot + ``is_training``.
* ``POST /feedback`` — record ground truth, report correctness + recent accuracy,
  and (once recent accuracy slips below the threshold with a full window) launch a
  graceful background retrain that hot-swaps a new version with **no downtime**.

To force a *wrong* feedback deterministically we always classify the log first to
learn what the live model predicts, then submit a *different* valid severity as
the ground truth (so ``correct`` is guaranteed False and the window fills below
threshold). A small ``drift_window`` (10) and tiny estimators keep the
fill-the-window-then-retrain cycle fast and hermetic.

The module-scoped ``client`` shares one trained app across the drift/retrain
tests (they run in file order: status → correct → wrong → drift-trigger →
post-retrain → graceful → regression); a separate function-scoped untrained app
covers the 503 path.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings
from src.log_generator import SEVERITIES

CANONICAL_LOG = "Database connection failed with timeout error"

# The five keys the base ``POST /classify`` (ClassifyResponse) is contracted to emit.
CLASSIFY_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

# Keys ``GET /adaptive/status`` (AdaptiveStatusResponse) must always carry.
ADAPTIVE_STATUS_KEYS = {
    "recent_accuracy",
    "window_size",
    "window_capacity",
    "threshold",
    "total_feedback",
    "retrains_triggered",
    "is_window_full",
    "is_training",
}

# A small, varied corpus to drive feedback against (each is classified first so we
# never depend on what the model actually predicts).
SAMPLE_LOGS = [
    "Database connection failed with timeout error",
    "GET /api/users 200 OK in 12ms",
    "User login successful for account 42",
    "Disk usage at 95% on /var partition",
    "NullPointerException in payment handler",
    "Cache miss for key session:abc",
    "Connection pool exhausted on database primary",
    "Slow query detected took 4200ms rows=512",
    "High cache eviction rate 900 keys/s",
    "Unhandled exception in request handler stacktrace",
    "Service health check returned 503",
    "Memory pressure detected, GC pause 800ms",
    "Failed to acquire lock after 30s",
    "Replication lag exceeded 60s on replica",
]

# Drift window for the test app: small so 10-ish wrong feedbacks fill it and
# cross the full-and-below-threshold boundary quickly.
DRIFT_WINDOW = 10


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny ``v1`` at startup.

    Tiny estimators + a small ``drift_window`` + an isolated tmp ``model_dir`` keep
    both first-boot training and the feedback-driven retrain fast; the ``with``
    block drives the lifespan so a ready model is loaded before the first request.
    """
    model_dir = tmp_path_factory.mktemp("adaptive_models")
    app = create_app(
        Settings(
            drift_window=DRIFT_WINDOW,
            accuracy_retrain_threshold=0.9,
            rf_n_estimators=4,
            gb_n_estimators=4,
            # Small corpus so the feedback-driven retrain (which regenerates
            # cfg.sample_size logs) stays fast and hermetic.
            sample_size=200,
            model_dir=str(model_dir),
        ),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def unready_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``.

    The empty registry plus disabled auto-train leaves no loaded classifier, so
    ``POST /feedback`` (which must classify the log to score it) returns ``503``.
    """
    empty_dir = tmp_path / "empty_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


def _predicted_severity(client, raw_log: str) -> str:
    """Return what the live model predicts for ``raw_log`` (via ``POST /classify``)."""
    resp = client.post("/classify", json={"raw_log": raw_log})
    assert resp.status_code == 200, resp.text
    return resp.json()["severity"]


def _a_wrong_severity(predicted: str) -> str:
    """Pick any valid severity that differs from ``predicted`` (forces a wrong label)."""
    for sev in SEVERITIES:
        if sev != predicted:
            return sev
    raise AssertionError("SEVERITIES must contain more than one label")


def _wait_until_ready(client, *, timeout: float = 30.0, interval: float = 0.25):
    """Poll ``GET /train/status`` until a retrain finishes (or time out).

    Returns the final status body once ``is_training`` is ``False`` and
    ``model_status == "ready"``; fails the test if the deadline passes first.
    """
    deadline = time.monotonic() + timeout
    body = client.get("/train/status").json()
    while time.monotonic() < deadline:
        body = client.get("/train/status").json()
        if not body["is_training"] and body["model_status"] == "ready":
            return body
        time.sleep(interval)
    pytest.fail(f"retrain did not finish within {timeout}s; last status: {body}")


# --------------------------------------------------------------------------- #
# GET /adaptive/status
# --------------------------------------------------------------------------- #


def test_adaptive_status_shape_and_initial_state(client):
    """``GET /adaptive/status`` returns 200 with all snapshot keys + is_training."""
    resp = client.get("/adaptive/status")
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert set(body) == ADAPTIVE_STATUS_KEYS, f"unexpected keys: {sorted(body)}"
    # Fresh monitor before any feedback: empty window, not training.
    assert body["window_size"] == 0
    assert body["window_capacity"] == DRIFT_WINDOW
    assert body["threshold"] == 0.9
    assert body["recent_accuracy"] == 1.0
    assert body["is_window_full"] is False
    assert body["is_training"] is False


# --------------------------------------------------------------------------- #
# POST /feedback — single correct / single wrong
# --------------------------------------------------------------------------- #


def test_single_correct_feedback(client):
    """Feeding the model's own prediction back as truth reports correct, no retrain."""
    log = "User login successful for account 42"
    predicted = _predicted_severity(client, log)

    resp = client.post(
        "/feedback", json={"raw_log": log, "true_severity": predicted}
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["recorded"] is True
    assert body["predicted_severity"] == predicted
    assert body["true_severity"] == predicted
    assert body["correct"] is True
    assert body["retrain_triggered"] is False  # one correct bit can't trigger
    assert 0.0 <= body["recent_accuracy"] <= 1.0


def test_single_wrong_feedback(client):
    """Submitting a severity that differs from the prediction reports correct False."""
    log = "GET /api/users 200 OK in 12ms"
    predicted = _predicted_severity(client, log)
    wrong = _a_wrong_severity(predicted)

    resp = client.post("/feedback", json={"raw_log": log, "true_severity": wrong})
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["recorded"] is True
    assert body["predicted_severity"] == predicted
    assert body["true_severity"] == wrong
    assert body["correct"] is False
    # Window not yet full (only a couple of feedbacks so far) -> no retrain.
    assert body["retrain_triggered"] is False


# --------------------------------------------------------------------------- #
# Drift -> graceful retrain -> re-arm  (the headline flow)
# --------------------------------------------------------------------------- #


def test_drift_triggers_graceful_retrain_and_rearms(client):
    """Filling the window with wrong feedback triggers a graceful retrain + re-arm.

    Submits a stream of deliberately-wrong feedbacks (classify each log first, then
    send a different valid severity). Once the rolling window is full and recent
    accuracy is below the 0.9 threshold, one ``/feedback`` must return
    ``retrain_triggered True``. We then:

    * fire a ``/classify`` immediately after the trigger and assert it still 200s
      (the old model keeps serving — graceful, no-downtime hot-swap), then
    * poll until the retrain finishes and assert the registry version advanced and
      the monitor re-armed (``retrains_triggered`` >= 1, window back to size 0).
    """
    # Make sure no retrain is mid-flight from an earlier test before we begin.
    _wait_until_ready(client)
    before_version = client.get("/train/status").json()["current_version"]
    before_retrains = client.get("/adaptive/status").json()["retrains_triggered"]

    triggered_body = None
    # Submit up to ~window + a few extra; the trigger lands on/after the window
    # fills (full + below threshold). Cycle through varied logs.
    for i in range(DRIFT_WINDOW + 4):
        log = SAMPLE_LOGS[i % len(SAMPLE_LOGS)]
        predicted = _predicted_severity(client, log)
        wrong = _a_wrong_severity(predicted)
        resp = client.post(
            "/feedback", json={"raw_log": log, "true_severity": wrong}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["correct"] is False  # we deliberately mislabel every one
        if body["retrain_triggered"]:
            triggered_body = body
            break

    assert triggered_body is not None, (
        "retrain_triggered never fired after filling the window below threshold"
    )
    # NOTE: the triggering response's ``recent_accuracy`` is read AFTER the handler
    # has already re-armed the monitor (mark_retrained clears the window before the
    # response is built), so it reads back as the empty-window 1.0 — we assert the
    # re-arm/version advance below rather than the (now-cleared) accuracy here.

    # --- Graceful, no-downtime: /classify serves the OLD model during retrain. ---
    # Fire it right after the trigger; whether the retrain is still running or has
    # already swapped, classification must never 503 / error.
    graceful = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert graceful.status_code == 200, graceful.text
    assert set(graceful.json()) == CLASSIFY_KEYS

    # --- Poll until the background retrain finishes and the new model is live. ---
    final = _wait_until_ready(client)
    assert final["is_training"] is False
    assert final["model_status"] == "ready"

    # Version advanced past the pre-retrain version (v1 -> v2, ...).
    after_version = final["current_version"]
    assert before_version is not None and after_version is not None
    assert after_version != before_version
    assert int(after_version.lstrip("v")) > int(before_version.lstrip("v"))

    # --- Monitor re-armed: a retrain was counted and the window was cleared. ---
    status = client.get("/adaptive/status").json()
    assert status["retrains_triggered"] >= before_retrains + 1
    assert status["is_training"] is False
    # Re-arm clears the window before the thread runs; allow for a few feedbacks
    # that may have landed after the trigger but well under a full window.
    assert status["window_size"] < DRIFT_WINDOW
    assert status["is_window_full"] is False


# --------------------------------------------------------------------------- #
# 503 path — no model loaded
# --------------------------------------------------------------------------- #


def test_feedback_503_when_unready(unready_client):
    """With no model + ``auto_train=False``, ``POST /feedback`` returns 503."""
    resp = unready_client.post(
        "/feedback", json={"raw_log": CANONICAL_LOG, "true_severity": "ERROR"}
    )
    assert resp.status_code == 503, resp.text


# --------------------------------------------------------------------------- #
# Regression smoke — adaptive loop didn't break base inference / stats
# --------------------------------------------------------------------------- #


def test_regression_classify_and_stats_still_work(client):
    """After the adaptive flow, ``/classify`` (5 keys) and ``/stats`` still behave."""
    # Drain any retrain still in flight from the drift test so model_status has
    # settled back to "ready" before we assert on it.
    _wait_until_ready(client)

    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert set(result) == CLASSIFY_KEYS, f"unexpected keys: {sorted(result)}"
    assert 0.0 <= result["confidence"] <= 1.0

    stats = client.get("/stats")
    assert stats.status_code == 200, stats.text
    body = stats.json()
    assert isinstance(body["total_classified"], int)
    assert body["total_classified"] >= 0
    assert body["model_status"] == "ready"

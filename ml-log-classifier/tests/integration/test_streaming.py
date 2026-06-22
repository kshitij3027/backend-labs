"""Integration tests for on-demand training + bulk/streaming inference (Commit 9).

Exercises the real HTTP contract of the Commit-9 routes added to
:func:`src.api.create_app` through Starlette's
:class:`~fastapi.testclient.TestClient`:

* ``POST /train`` — kick off a background retrain (202), with a 409 concurrency
  guard, and ``GET /train/status`` polling until the hot-swap lands.
* ``POST /classify/batch`` — vectorized list-in / list-out, counter += N.
* ``POST /classify/stream`` — NDJSON streaming, one result line per input log,
  counter += N.
* The ``503`` path for batch + stream when no model is loaded.

As in :mod:`tests.integration.test_api`, a module-scoped ``client`` injects a
tiny config (``rf_n_estimators=5``, ``gb_n_estimators=5``) and an isolated tmp
``model_dir`` so first boot trains a small ``v1`` model exactly once for the whole
module; the ``with TestClient(app)`` block drives the FastAPI lifespan so the
model is loaded before any request is served. Small corpus sizes keep the
on-demand ``/train`` retrains fast and hermetic.
"""

from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings

# The spec's headline sample (project requirements §5, §8): this exact input must
# classify to ERROR / SYSTEM, even when routed through the batch endpoint.
CANONICAL_LOG = "Database connection failed with timeout error"
EXPECTED_SEVERITY = "ERROR"
EXPECTED_CATEGORY = "SYSTEM"

# Every key ``LogClassifier.classify`` / ``ClassifyResponse`` is contracted to emit.
CLASSIFY_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

# The accuracy keys ``trainer.train`` puts into its metrics dict (a subset check).
METRIC_ACCURACY_KEYS = {"severity_test_accuracy", "category_test_accuracy"}

# A small, varied set of logs to feed the batch / stream endpoints.
SAMPLE_LOGS = [
    "Database connection failed with timeout error",
    "GET /api/users 200 OK in 12ms",
    "User login successful for account 42",
    "Disk usage at 95% on /var partition",
    "NullPointerException in payment handler",
    "Cache miss for key session:abc",
]


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny ``v1`` model at startup.

    Tiny estimators + an isolated tmp ``model_dir`` keep both first-boot training
    and the on-demand ``/train`` retrains fast; the ``with`` block drives the
    lifespan so a ready model is loaded before the first request.
    """
    model_dir = tmp_path_factory.mktemp("models")
    app = create_app(
        Settings(rf_n_estimators=5, gb_n_estimators=5, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def unready_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``.

    The empty registry plus disabled auto-train leaves the app with no loaded
    classifier, so the batch + stream endpoints must return ``503``.
    """
    empty_dir = tmp_path / "empty_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


def _wait_until_ready(client, *, timeout: float = 30.0, interval: float = 0.25):
    """Poll ``GET /train/status`` until training finishes (or time out).

    Returns the final status body once ``is_training`` is ``False`` and
    ``model_status == "ready"``. Fails the test if the deadline passes first.
    """
    deadline = time.monotonic() + timeout
    body = client.get("/train/status").json()
    while time.monotonic() < deadline:
        body = client.get("/train/status").json()
        if not body["is_training"] and body["model_status"] == "ready":
            return body
        time.sleep(interval)
    pytest.fail(f"training did not finish within {timeout}s; last status: {body}")


# --------------------------------------------------------------------------- #
# POST /train  +  GET /train/status
# --------------------------------------------------------------------------- #


def test_train_returns_202_and_training_status(client):
    """``POST /train`` launches a background retrain and returns 202 + training state."""
    resp = client.post("/train", json={"count": 80})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["is_training"] is True
    assert body["model_status"] == "training"
    # Drain the background retrain so it doesn't bleed into later tests.
    _wait_until_ready(client)


def test_train_completes_and_advances_version(client):
    """Polling status after a retrain shows ready, a new version, and metrics."""
    # The startup trained v1; this retrain should produce v2 (or later).
    before = client.get("/train/status").json()["current_version"]

    resp = client.post("/train", json={"count": 80})
    assert resp.status_code == 202, resp.text

    final = _wait_until_ready(client)
    assert final["is_training"] is False
    assert final["model_status"] == "ready"

    # current_version advanced past the pre-retrain version (v1 -> v2, ...).
    assert before is not None and final["current_version"] is not None
    assert final["current_version"] != before
    assert int(final["current_version"].lstrip("v")) > int(before.lstrip("v"))

    # last_metrics is a dict carrying the held-out accuracy keys.
    metrics = final["last_metrics"]
    assert isinstance(metrics, dict)
    assert METRIC_ACCURACY_KEYS.issubset(metrics), (
        f"missing accuracy keys; got: {sorted(metrics)}"
    )


def test_train_concurrent_guard_returns_409(client):
    """A second ``/train`` while one is already running returns 409.

    Fire two requests back-to-back: the first should win (202) and the second,
    arriving while the first is still training, should be rejected with 409. To
    avoid flakiness if the first retrain finishes too fast, we only assert the
    409 when status confirms a retrain is genuinely in progress.
    """
    first = client.post("/train", json={"count": 120})
    assert first.status_code == 202, first.text

    second = client.post("/train", json={"count": 120})
    if client.get("/train/status").json()["is_training"]:
        # A retrain is in progress -> the concurrent submit must be rejected.
        assert second.status_code == 409, second.text
        assert "in progress" in second.json()["detail"].lower()
    else:
        # The first retrain already finished before the second landed; the second
        # then legitimately started its own retrain (202). Either way no error.
        assert second.status_code in (202, 409), second.text

    # Drain whatever retrain is still running before the next test.
    _wait_until_ready(client)


# --------------------------------------------------------------------------- #
# POST /classify/batch
# --------------------------------------------------------------------------- #


def test_classify_batch_shape_and_count(client):
    """A batch of 5 logs returns 5 results, ``count == 5``, each with 5 keys."""
    logs = [{"raw_log": m} for m in SAMPLE_LOGS[:5]]
    resp = client.post("/classify/batch", json={"logs": logs})
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["count"] == 5
    assert len(body["results"]) == 5
    for result in body["results"]:
        assert set(result) == CLASSIFY_KEYS, f"unexpected keys: {sorted(result)}"
        assert 0.0 <= result["confidence"] <= 1.0


def test_classify_batch_increments_counter_by_n(client):
    """A batch of 4 logs bumps ``total_classified`` by exactly 4."""
    before = client.get("/stats").json()["total_classified"]

    logs = [{"raw_log": m} for m in SAMPLE_LOGS[:4]]
    resp = client.post("/classify/batch", json={"logs": logs})
    assert resp.status_code == 200, resp.text
    assert resp.json()["count"] == 4

    after = client.get("/stats").json()["total_classified"]
    assert after == before + 4, f"counter went {before} -> {after} (expected +4)"


def test_classify_batch_canonical_error_system(client):
    """The canonical log, routed through the batch endpoint, classifies ERROR/SYSTEM."""
    logs = [
        {"raw_log": "User login successful for account 42"},
        {"raw_log": CANONICAL_LOG},
        {"raw_log": "GET /api/health 200 OK"},
    ]
    resp = client.post("/classify/batch", json={"logs": logs})
    assert resp.status_code == 200, resp.text

    results = resp.json()["results"]
    canonical = results[1]  # the CANONICAL_LOG entry, in input order
    assert canonical["severity"] == EXPECTED_SEVERITY, results
    assert canonical["category"] == EXPECTED_CATEGORY, results


# --------------------------------------------------------------------------- #
# POST /classify/stream  (NDJSON)
# --------------------------------------------------------------------------- #


def test_classify_stream_ndjson_shape(client):
    """Streaming 6 logs yields NDJSON: 6 lines, each a result with 5 keys."""
    logs = [{"raw_log": m} for m in SAMPLE_LOGS]
    with client.stream("POST", "/classify/stream", json={"logs": logs}) as resp:
        assert resp.status_code == 200, resp.text
        assert resp.headers["content-type"].startswith("application/x-ndjson")
        lines = [json.loads(line) for line in resp.iter_lines() if line]

    assert len(lines) == 6
    for result in lines:
        assert set(result) == CLASSIFY_KEYS, f"unexpected keys: {sorted(result)}"
        assert 0.0 <= result["confidence"] <= 1.0


def test_classify_stream_increments_counter_by_n(client):
    """Streaming N logs bumps ``total_classified`` by exactly N."""
    before = client.get("/stats").json()["total_classified"]

    logs = [{"raw_log": m} for m in SAMPLE_LOGS]  # 6 logs
    with client.stream("POST", "/classify/stream", json={"logs": logs}) as resp:
        assert resp.status_code == 200, resp.text
        lines = [line for line in resp.iter_lines() if line]
    assert len(lines) == len(logs)

    after = client.get("/stats").json()["total_classified"]
    assert after == before + len(logs), (
        f"counter went {before} -> {after} (expected +{len(logs)})"
    )


# --------------------------------------------------------------------------- #
# 503 path — no model loaded
# --------------------------------------------------------------------------- #


def test_batch_and_stream_503_when_unready(unready_client):
    """With no model + ``auto_train=False``, batch and stream both return 503."""
    logs = [{"raw_log": CANONICAL_LOG}]

    batch = unready_client.post("/classify/batch", json={"logs": logs})
    assert batch.status_code == 503, batch.text

    stream = unready_client.post("/classify/stream", json={"logs": logs})
    assert stream.status_code == 503, stream.text

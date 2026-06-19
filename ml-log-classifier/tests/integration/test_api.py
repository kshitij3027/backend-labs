"""Integration tests for the FastAPI base surface (Commit 8).

Exercises the real HTTP contract of :func:`src.api.create_app` through Starlette's
:class:`~fastapi.testclient.TestClient`. Using ``with TestClient(app) as client``
runs the FastAPI **lifespan**, so the load-or-train startup actually executes and
``app.state`` is populated with a ready model before any request is served — these
are end-to-end tests of the wired app, not isolated handler unit tests.

To keep startup fast the ``client`` fixture injects a tiny config
(``rf_n_estimators=5``, ``gb_n_estimators=5``) and an isolated ``model_dir`` under a
per-module tmp dir, so first boot trains a small model exactly once for the whole
module. A separate, function-scoped ``untrained_client`` covers the
``auto_train=False`` + empty-registry path that must surface ``"untrained"`` /
``503``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings

# The spec's headline sample (project requirements §5, §8): this exact input must
# classify to ERROR / SYSTEM.
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


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny model once at startup.

    Tiny estimators + an isolated tmp ``model_dir`` keep the first-boot training
    fast; the ``with`` block drives the lifespan so the model is loaded before the
    first request.
    """
    model_dir = tmp_path_factory.mktemp("models")
    app = create_app(
        Settings(rf_n_estimators=5, gb_n_estimators=5, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def untrained_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``.

    The empty registry plus disabled auto-train means startup leaves the app
    ``"untrained"`` so ``POST /classify`` must return ``503``.
    """
    empty_dir = tmp_path / "empty_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


# --------------------------------------------------------------------------- #
# /health and /stats
# --------------------------------------------------------------------------- #


def test_health_ok(client):
    """``GET /health`` returns 200 with ``status == "healthy"``."""
    resp = client.get("/health")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "healthy"
    # Startup completed before serving, so the model is ready.
    assert body["model_status"] == "ready"


def test_stats_ready(client):
    """``GET /stats`` reports a ready model and an integer classified count."""
    resp = client.get("/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["model_status"] == "ready"
    assert isinstance(body["total_classified"], int)
    assert body["total_classified"] >= 0


# --------------------------------------------------------------------------- #
# POST /classify — happy path / shape / canonical
# --------------------------------------------------------------------------- #


def test_classify_shape(client):
    """A successful classify returns all five keys with a valid confidence."""
    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == CLASSIFY_KEYS, f"unexpected keys: {sorted(body)}"

    confidence = body["confidence"]
    assert isinstance(confidence, float)
    assert 0.0 <= confidence <= 1.0
    # The two per-axis confidences are also probabilities in [0, 1].
    for key in ("severity_confidence", "category_confidence"):
        assert isinstance(body[key], float)
        assert 0.0 <= body[key] <= 1.0


def test_classify_canonical_error_system(client):
    """The spec's canonical input classifies to ERROR / SYSTEM."""
    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["severity"] == EXPECTED_SEVERITY and body["category"] == EXPECTED_CATEGORY, (
        f"canonical classify mismatch: expected "
        f"{EXPECTED_SEVERITY}/{EXPECTED_CATEGORY}, got "
        f"{body.get('severity')}/{body.get('category')} — full body: {body}"
    )


def test_classify_with_timestamp(client):
    """An optional ISO-8601 ``timestamp`` is accepted and classify still succeeds."""
    resp = client.post(
        "/classify",
        json={"raw_log": CANONICAL_LOG, "timestamp": "2026-06-18T12:34:56"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == CLASSIFY_KEYS
    assert body["severity"] == EXPECTED_SEVERITY
    assert body["category"] == EXPECTED_CATEGORY


def test_classify_web_request_log_valid_severity(client):
    """A web-request-style log classifies to one of the known severities."""
    resp = client.post(
        "/classify",
        json={"raw_log": "GET /api/users 200 OK in 12ms"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # We don't pin the exact label (web logs are mostly INFO/WARNING in the corpus),
    # only that the service returns a non-empty severity from its trained classes.
    assert isinstance(body["severity"], str) and body["severity"]
    assert isinstance(body["category"], str) and body["category"]


# --------------------------------------------------------------------------- #
# Counter
# --------------------------------------------------------------------------- #


def test_classify_increments_counter(client):
    """Each successful classify bumps ``total_classified`` by exactly one."""
    before = client.get("/stats").json()["total_classified"]

    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text

    after = client.get("/stats").json()["total_classified"]
    assert after == before + 1, f"counter went {before} -> {after} (expected +1)"


# --------------------------------------------------------------------------- #
# POST /classify — validation (422)
# --------------------------------------------------------------------------- #


def test_classify_empty_raw_log_422(client):
    """An empty ``raw_log`` violates ``min_length=1`` and yields 422."""
    resp = client.post("/classify", json={"raw_log": ""})
    assert resp.status_code == 422, resp.text


def test_classify_missing_raw_log_422(client):
    """A body without ``raw_log`` is a validation error (422)."""
    resp = client.post("/classify", json={"timestamp": "2026-06-18T12:34:56"})
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------- #
# 503 path — untrained app
# --------------------------------------------------------------------------- #


def test_untrained_stats_and_classify_503(untrained_client):
    """With no model + ``auto_train=False``: stats are ``untrained`` and classify 503s."""
    stats = untrained_client.get("/stats")
    assert stats.status_code == 200, stats.text
    assert stats.json()["model_status"] == "untrained"

    # /health still reports the process is up (liveness), just not model-ready.
    health = untrained_client.get("/health")
    assert health.status_code == 200, health.text
    assert health.json()["status"] == "healthy"
    assert health.json()["model_status"] == "untrained"

    classify = untrained_client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert classify.status_code == 503, classify.text

"""Integration tests for the feature-importance viz feed (Commit 15).

Exercises the real HTTP contract of ``GET /feature-importance`` through Starlette's
:class:`~fastapi.testclient.TestClient`, driving the FastAPI **lifespan** so the
load-or-train startup runs and ``app.state.classifier`` holds a ready model before
any request is served (these are end-to-end tests of the wired app, not isolated
handler unit tests).

This route (Commit 15) reads the severity ensemble's RandomForest
``feature_importances_`` off the *currently-served* classifier and returns the top-N
``{name, importance}`` pairs sorted descending, plus the registry's current version.
It is additive and read-only — so a small regression block re-asserts the base
``/classify`` (5 keys), ``/classify/service`` (8 keys) and ``/stats`` contracts are
unchanged.

Tiny estimators (``rf_n_estimators=4``, ``gb_n_estimators=4``) + an isolated tmp
``model_dir`` keep first-boot training fast; the module-scoped ``client`` fixture
trains exactly one small model for the whole module. A function-scoped
``untrained_client`` (``auto_train=False`` + empty registry) covers the ``503`` path.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings

# The spec's canonical input — reused for the regression assertions so this file is
# self-contained.
CANONICAL_LOG = "Database connection failed with timeout error"

# Base /classify response keys (unchanged by this additive commit).
CLASSIFY_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

# Hierarchical /classify/service response keys (Commit 11): the five base keys plus
# service / service_confidence / anomaly_score (8 total).
SERVICE_KEYS = {
    "service",
    "service_confidence",
    "severity",
    "severity_confidence",
    "category",
    "category_confidence",
    "confidence",
    "anomaly_score",
}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny model once at startup.

    Tiny estimators + an isolated tmp ``model_dir`` keep first-boot training fast;
    the ``with`` block drives the lifespan so a ready model is loaded before the
    first request.
    """
    model_dir = tmp_path_factory.mktemp("fi_models")
    app = create_app(
        Settings(rf_n_estimators=4, gb_n_estimators=4, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def untrained_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``.

    The empty registry plus disabled auto-train means startup leaves the app
    ``"untrained"`` so ``GET /feature-importance`` must return ``503``.
    """
    empty_dir = tmp_path / "fi_empty_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


# --------------------------------------------------------------------------- #
# GET /feature-importance — shape / contract
# --------------------------------------------------------------------------- #


def test_feature_importance_shape(client):
    """``GET /feature-importance`` → 200 with a ``features`` list + ``model_version``.

    Every item is a ``{name: str, importance: float >= 0}`` pair, and the version
    is the registry's current id (``"v1"`` after the first auto-train).
    """
    resp = client.get("/feature-importance")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Envelope shape.
    assert set(body) == {"features", "model_version"}, f"unexpected keys: {sorted(body)}"
    assert isinstance(body["features"], list)
    # After auto-train a v1 is current; it must be a non-empty string.
    assert isinstance(body["model_version"], str) and body["model_version"]

    features = body["features"]
    # A fitted RF over the corpus exposes importances, so the list is non-empty.
    assert len(features) > 0, "expected a fitted model to expose feature importances"

    for item in features:
        assert set(item) == {"name", "importance"}, f"unexpected item keys: {sorted(item)}"
        assert isinstance(item["name"], str) and item["name"], "feature name must be a non-empty string"
        # Pydantic coerces ints to float; assert it is a real number >= 0.
        assert isinstance(item["importance"], float)
        assert item["importance"] >= 0.0, f"importance must be >= 0, got {item['importance']}"


def test_feature_importance_sorted_descending_default(client):
    """The default response is sorted by importance descending."""
    resp = client.get("/feature-importance")
    assert resp.status_code == 200, resp.text
    importances = [f["importance"] for f in resp.json()["features"]]
    assert importances == sorted(importances, reverse=True), (
        f"importances not sorted descending: {importances}"
    )


def test_feature_importance_top_caps_and_sorted(client):
    """``?top=10`` returns at most 10 items, still sorted descending."""
    resp = client.get("/feature-importance", params={"top": 10})
    assert resp.status_code == 200, resp.text
    features = resp.json()["features"]

    assert len(features) <= 10, f"expected <= 10 features, got {len(features)}"
    importances = [f["importance"] for f in features]
    assert importances == sorted(importances, reverse=True), (
        f"top=10 importances not sorted descending: {importances}"
    )


def test_feature_importance_names_are_real_features(client):
    """Returned names intersect the model's real feature space.

    The feature pipeline emits ``tfidf__*`` names for the TF-IDF block and dense
    column names (e.g. ``msg_len``, ``level_ERROR``, ``token_count``,
    ``punct_count``) for the metadata/temporal block. We require *some* name to
    match that known space (in addition to all being non-empty strings) so this
    is a real feature feed, not arbitrary strings.
    """
    resp = client.get("/feature-importance", params={"top": 50})
    assert resp.status_code == 200, resp.text
    names = [f["name"] for f in resp.json()["features"]]
    assert names, "expected at least one feature name"

    # Known dense column names from src.features.DENSE_COLUMNS (+ level_* one-hots).
    known_dense = {
        "msg_len",
        "token_count",
        "punct_count",
        "digit_count",
        "upper_ratio",
        "hour_sin",
        "hour_cos",
    }
    has_tfidf = any(n.startswith("tfidf__") for n in names)
    has_dense = any(n in known_dense or n.startswith("level_") for n in names)
    assert has_tfidf or has_dense, (
        f"no recognizable feature names (expected some tfidf__* or dense columns): {names}"
    )


def test_feature_importance_top_zero_empty(client):
    """``?top=0`` clamps to an empty feature list (still 200 with a version)."""
    resp = client.get("/feature-importance", params={"top": 0})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["features"] == []
    # The model is still loaded, so the version is reported.
    assert isinstance(body["model_version"], str) and body["model_version"]


# --------------------------------------------------------------------------- #
# 503 path — untrained app
# --------------------------------------------------------------------------- #


def test_feature_importance_untrained_503(untrained_client):
    """With no model + ``auto_train=False``, ``/feature-importance`` returns 503."""
    resp = untrained_client.get("/feature-importance")
    assert resp.status_code == 503, resp.text


# --------------------------------------------------------------------------- #
# Regression — the additive commit leaves base routes unchanged
# --------------------------------------------------------------------------- #


def test_base_routes_unchanged_regression(client):
    """``/classify`` (5 keys), ``/classify/service`` (8 keys) and ``/stats`` still hold.

    Feature-importance is additive/read-only, so the pre-existing contracts must be
    byte-for-byte intact.
    """
    # Base classify — exactly the five canonical keys.
    base = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert base.status_code == 200, base.text
    assert set(base.json()) == CLASSIFY_KEYS, f"/classify keys changed: {sorted(base.json())}"

    # Hierarchical multi-service classify — exactly the eight keys.
    svc = client.post("/classify/service", json={"raw_log": CANONICAL_LOG})
    assert svc.status_code == 200, svc.text
    assert set(svc.json()) == SERVICE_KEYS, f"/classify/service keys changed: {sorted(svc.json())}"

    # Stats — ready model + integer count.
    stats = client.get("/stats")
    assert stats.status_code == 200, stats.text
    sbody = stats.json()
    assert sbody["model_status"] == "ready"
    assert isinstance(sbody["total_classified"], int) and sbody["total_classified"] >= 0

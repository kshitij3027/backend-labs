"""Integration tests for A/B serving + graceful fallback (Commit 13, Feature Area C).

Drives the real HTTP contract of the Commit-13 serving routes added to
:func:`src.api.create_app` through Starlette's
:class:`~fastapi.testclient.TestClient`:

* ``GET /models`` — the A/B + registry view (:class:`ModelsResponse`): annotated
  version list, champion, A/B version ids, split.
* ``POST /classify/ab`` — A/B-routed classification (:class:`ABClassifyResponse`):
  the five classify keys + ``model_version`` / ``ab_group`` / ``fallback_used``.
* ``POST /train`` makes the freshly-trained version the **challenger** (group B)
  while the champion (group A) keeps serving — verified end-to-end by training a
  second version and re-reading ``/models``.
* ``POST /models/promote`` — champion swap (200; 404 on an unknown version).
* ``POST /models/ab`` — (re)configure: 422 on an out-of-bounds split (schema
  bound), 400 on an unknown version id.
* Per-version ``serving_metrics`` accumulate as ``/classify/ab`` traffic flows.
* Regression: the base ``POST /classify`` (5-key) and ``GET /stats`` still work.

As in :mod:`tests.integration.test_streaming`, a module-scoped ``client`` injects a
tiny config (``rf_n_estimators=4``, ``gb_n_estimators=4``) and an isolated tmp
``model_dir`` so first boot trains a small ``v1`` model exactly once for the whole
module; the ``with TestClient(app)`` block drives the FastAPI lifespan so the model
is loaded (and the A/B router defaulted) before any request is served. Small corpus
sizes keep the on-demand ``/train`` retrain fast and hermetic.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings

#: The spec's headline sample (project requirements §5, §8): ERROR / SYSTEM.
CANONICAL_LOG = "Database connection failed with timeout error"
EXPECTED_SEVERITY = "ERROR"
EXPECTED_CATEGORY = "SYSTEM"

#: The five keys every base classification result carries.
CLASSIFY_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

#: The A/B response = the five classify keys + three serving keys.
AB_KEYS = CLASSIFY_KEYS | {"model_version", "ab_group", "fallback_used"}

#: The top-level keys of a ModelsResponse.
MODELS_RESPONSE_KEYS = {"models", "champion", "a_version", "b_version", "split_b"}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny ``v1`` model at startup.

    Tiny estimators + an isolated tmp ``model_dir`` keep both first-boot training
    and the single on-demand ``/train`` retrain fast; the ``with`` block drives the
    lifespan so a ready model (and a defaulted A/B router) exists before the first
    request.
    """
    model_dir = tmp_path_factory.mktemp("ab_models")
    app = create_app(
        Settings(rf_n_estimators=4, gb_n_estimators=4, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


def _wait_until_ready(client, *, timeout: float = 30.0, interval: float = 0.25):
    """Poll ``GET /train/status`` until a retrain finishes (or time out).

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
# GET /models
# --------------------------------------------------------------------------- #


def test_models_lists_v1_with_champion(client):
    """``GET /models`` returns the ModelsResponse shape with v1 listed + a champion."""
    resp = client.get("/models")
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert MODELS_RESPONSE_KEYS.issubset(body), f"unexpected keys: {sorted(body)}"
    assert isinstance(body["models"], list) and body["models"], "no models listed"

    ids = {m["version"] for m in body["models"]}
    assert "v1" in ids, f"v1 not listed; got {sorted(ids)}"
    assert body["champion"] is not None
    # Each entry is annotated by ABRouter.models().
    for m in body["models"]:
        assert {"is_champion", "ab_group", "serving_metrics"}.issubset(m)
    # On first boot A and B both bind to v1 (current == latest).
    assert body["a_version"] == "v1"
    assert body["b_version"] == "v1"


# --------------------------------------------------------------------------- #
# POST /classify/ab
# --------------------------------------------------------------------------- #


def test_classify_ab_shape_and_plausible_labels(client):
    """``POST /classify/ab`` returns the 8-key A/B response; canonical -> ERROR/SYSTEM."""
    resp = client.post("/classify/ab", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert set(body) == AB_KEYS, f"unexpected keys: {sorted(body)}"
    assert body["ab_group"] in {"A", "B"}
    assert isinstance(body["model_version"], str) and body["model_version"]
    assert isinstance(body["fallback_used"], bool)
    assert 0.0 <= body["confidence"] <= 1.0
    # The spec's headline sample classifies ERROR / SYSTEM through the A/B path too.
    assert body["severity"] == EXPECTED_SEVERITY, body
    assert body["category"] == EXPECTED_CATEGORY, body


# --------------------------------------------------------------------------- #
# POST /train makes the new version the challenger (group B)
# --------------------------------------------------------------------------- #


def test_train_makes_new_version_the_ab_challenger(client):
    """Training a 2nd version advances current and installs it as group B (challenger)."""
    before = client.get("/models").json()
    before_champion = before["champion"]

    resp = client.post("/train", json={"count": 60})
    assert resp.status_code == 202, resp.text
    final = _wait_until_ready(client)

    # The registry's current_version advanced to v2 (or later).
    assert final["current_version"] is not None
    assert final["current_version"] != before_champion
    assert int(final["current_version"].lstrip("v")) > int(
        str(before_champion).lstrip("v")
    )

    after = client.get("/models").json()
    ids = {m["version"] for m in after["models"]}
    assert len(after["models"]) >= 2, f"expected >=2 versions, got {sorted(ids)}"

    # Faithful A/B flow: training advances the registry's ``current`` (champion) to
    # the new version, but the new version becomes the *challenger* (group B) — it
    # does NOT immediately take group A's traffic. Group A stays the prior champion
    # (v1) until an explicit promote; B is the freshly-trained latest version.
    new_version = final["current_version"]
    assert after["champion"] == new_version  # registry current advanced (v1 -> v2)
    assert after["b_version"] == new_version  # newest version is the challenger (B)
    assert after["a_version"] == before_champion  # group A unchanged (still v1)
    assert after["a_version"] != after["b_version"]  # A and B now distinct versions


# --------------------------------------------------------------------------- #
# POST /models/promote
# --------------------------------------------------------------------------- #


def test_promote_v2_changes_champion(client):
    """``POST /models/promote`` to an existing version makes it the new champion."""
    # Ensure there are >=2 versions (a second one was trained above, but stay robust
    # if test ordering changes).
    models = client.get("/models").json()["models"]
    if len(models) < 2:
        client.post("/train", json={"count": 60})
        _wait_until_ready(client)

    resp = client.post("/models/promote", json={"version": "v2"})
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["champion"] == "v2", body
    assert body["a_version"] == "v2", body  # promoted version serves group A
    # The promoted version is flagged is_champion in the annotated list.
    v2 = next(m for m in body["models"] if m["version"] == "v2")
    assert v2["is_champion"] is True


def test_promote_unknown_version_returns_404(client):
    """Promoting a version id the registry does not know returns 404."""
    resp = client.post("/models/promote", json={"version": "v999"})
    assert resp.status_code == 404, resp.text
    assert "v999" in resp.json()["detail"]


# --------------------------------------------------------------------------- #
# POST /models/ab — reconfigure validation
# --------------------------------------------------------------------------- #


def test_configure_ab_split_out_of_bounds_returns_422(client):
    """A split_b outside [0, 1] is rejected by the schema bound (422)."""
    resp = client.post("/models/ab", json={"split_b": 1.5})
    assert resp.status_code == 422, resp.text


def test_configure_ab_unknown_version_returns_400(client):
    """An unknown version id supplied to /models/ab is a 400 (registry validation)."""
    resp = client.post("/models/ab", json={"b_version": "v999"})
    assert resp.status_code == 400, resp.text


# --------------------------------------------------------------------------- #
# Per-version serving metrics accumulate with /classify/ab traffic
# --------------------------------------------------------------------------- #


def test_ab_traffic_accumulates_per_version_serving_metrics(client):
    """After several ``/classify/ab`` calls, per-version serving_metrics show requests > 0."""
    # Spread traffic across both arms so at least one version records requests.
    client.post("/models/ab", json={"split_b": 0.5})

    for i in range(12):
        r = client.post("/classify/ab", json={"raw_log": f"{CANONICAL_LOG} #{i}"})
        assert r.status_code == 200, r.text

    models = client.get("/models").json()["models"]
    total_served = sum(m["serving_metrics"]["requests"] for m in models)
    assert total_served > 0, f"no version recorded any served requests: {models}"


# --------------------------------------------------------------------------- #
# Regression — base /classify and /stats are unchanged by the A/B additions
# --------------------------------------------------------------------------- #


def test_base_classify_still_five_keys(client):
    """The base ``POST /classify`` still returns exactly the five classify keys."""
    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    assert set(resp.json()) == CLASSIFY_KEYS, f"unexpected keys: {sorted(resp.json())}"


def test_stats_still_works(client):
    """``GET /stats`` still reports a total + a ready model status."""
    resp = client.get("/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "total_classified" in body and "model_status" in body
    assert body["model_status"] == "ready"

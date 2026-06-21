"""Integration tests for ``GET /cache/stats`` (Commit 16).

Exercises the real HTTP contract of the prediction-cache stats endpoint through
Starlette's :class:`~fastapi.testclient.TestClient`, driving the FastAPI **lifespan**
so the load-or-train startup runs and ``app.state.classifier`` holds a ready model
before any request is served.

Three behaviours are pinned:

* ``GET /cache/stats`` → 200 with exactly the five
  :meth:`~src.cache.PredictionCache.stats` keys; after a couple of ``/classify``
  calls the cache holds at least one entry (``size > 0``).
* The cache does **not** bypass metrics: classifying the *same* log twice (the
  second is a cache hit) still increments ``total_classified`` by 2 — the
  authoritative count comes from the metrics aggregator, not the model call.
* ``503`` when no model is loaded (``auto_train=False`` + an empty ``model_dir``).

Tiny estimators (``rf_n_estimators=4``, ``gb_n_estimators=4``) + an isolated tmp
``model_dir`` keep first-boot training fast.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings

CACHE_STATS_KEYS = {"hits", "misses", "hit_rate", "size", "capacity"}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained a tiny model once at startup."""
    model_dir = tmp_path_factory.mktemp("cache_models")
    app = create_app(
        Settings(rf_n_estimators=4, gb_n_estimators=4, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def untrained_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``."""
    empty_dir = tmp_path / "cache_empty_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


# --------------------------------------------------------------------------- #
# GET /cache/stats — shape + populates after classify
# --------------------------------------------------------------------------- #


def test_cache_stats_shape_and_populates(client):
    """``/cache/stats`` returns the five keys; ``size`` grows after classifying."""
    resp = client.get("/cache/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == CACHE_STATS_KEYS, f"unexpected keys: {sorted(body)}"
    assert isinstance(body["hits"], int)
    assert isinstance(body["misses"], int)
    assert isinstance(body["hit_rate"], float)
    assert isinstance(body["size"], int)
    assert body["capacity"] == 1024  # default cache_size

    # Drive a couple of classifications, then the cache must hold >= 1 entry.
    client.post("/classify", json={"raw_log": "Disk usage at 91% on /var partition"})
    client.post("/classify", json={"raw_log": "TLS handshake failed with upstream peer"})

    after = client.get("/cache/stats")
    assert after.status_code == 200, after.text
    assert after.json()["size"] > 0, "cache should hold entries after /classify calls"


def test_cache_hit_still_counts_in_total_classified(client):
    """A cache-hit ``/classify`` still bumps ``total_classified`` (metrics not bypassed).

    Classifying the *same* log twice: the second call is served from the prediction
    cache, but the metrics aggregator is the authoritative counter, so the reported
    ``total_classified`` must rise by exactly 2.
    """
    before = client.get("/stats").json()["total_classified"]

    log = {"raw_log": "Repeated identical log line for cache-hit accounting"}
    r1 = client.post("/classify", json=log)
    r2 = client.post("/classify", json=log)
    assert r1.status_code == 200 and r2.status_code == 200
    # Same input -> identical classification output (deterministic + cached).
    assert r1.json() == r2.json()

    after = client.get("/stats").json()["total_classified"]
    assert after - before == 2, (
        f"total_classified should rise by 2 across a repeat (cache hit) classify; "
        f"before={before} after={after}"
    )

    # And the repeat registered as a cache hit.
    assert client.get("/cache/stats").json()["hits"] >= 1


# --------------------------------------------------------------------------- #
# 503 path — untrained app
# --------------------------------------------------------------------------- #


def test_cache_stats_untrained_503(untrained_client):
    """With no model + ``auto_train=False``, ``/cache/stats`` returns 503."""
    resp = untrained_client.get("/cache/stats")
    assert resp.status_code == 503, resp.text

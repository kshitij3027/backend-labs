"""Integration tests for the FastAPI REST surface (C11).

These drive the *real* application via :class:`fastapi.testclient.TestClient` **as a context
manager**, so the startup lifespan runs and the :class:`~src.engine.ClusteringEngine` is
warmed up before any request is served. Warm-up is intentionally tiny (a few hundred
generated logs) so the whole module stays fast while still exercising the genuine
fit/transform/assign path through every endpoint — no mocks.

The engine warm-up does real sklearn work, so each test builds its own client via
:func:`make_client`; the ``with`` block is what triggers (and tears down) the lifespan.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import create_app
from src.engine import ClusteringEngine
from src.log_generator import generate_logs


def make_client() -> TestClient:
    """Build a TestClient over an app warmed on a small, fast generated batch.

    Use as ``with make_client() as c:`` so the startup lifespan (which warms the engine)
    actually runs — a bare ``TestClient(app)`` without the context manager would not.
    """
    app = create_app(warmup_logs=generate_logs(250, seed=1))
    return TestClient(app)


def _one_log() -> dict:
    """Return a single generated log as a JSON-able dict (a valid ``/cluster`` body)."""
    return generate_logs(1, seed=7)[0].model_dump(mode="json")


def _logs(n: int, seed: int = 11) -> list[dict]:
    """Return ``n`` generated logs as JSON-able dicts."""
    return [log.model_dump(mode="json") for log in generate_logs(n, seed=seed)]


# --------------------------------------------------------------------------- health


def test_health_ok_after_warmup() -> None:
    with make_client() as c:
        resp = c.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["version"] == "0.1.0"
        assert len(body["algorithms"]) == 3
        assert set(body["algorithms"]) == set(ClusteringEngine.ALGORITHMS)


def test_root_banner() -> None:
    with make_client() as c:
        resp = c.get("/")
        assert resp.status_code == 200
        assert resp.json()["service"] == "real-time-log-pattern-clustering"


# -------------------------------------------------------------------------- cluster


def test_cluster_single_log() -> None:
    with make_client() as c:
        resp = c.post("/cluster", json=_one_log())
        assert resp.status_code == 200
        body = resp.json()
        # The combined ClusterAssignment surface.
        assert len(body["results"]) == 3
        for r in body["results"]:
            assert r["algorithm"] in ClusteringEngine.ALGORITHMS
            assert "cluster_id" in r
            assert 0.0 <= r["confidence"] <= 1.0
        assert "is_anomaly" in body
        assert "is_new_pattern" in body
        assert "pattern_type" in body
        assert "masked_message" in body


def test_cluster_rejects_bad_body() -> None:
    with make_client() as c:
        # Missing required fields (service/level/message/timestamp) -> 422.
        resp = c.post("/cluster", json={"message": "no other fields"})
        assert resp.status_code == 422


def test_cluster_batch() -> None:
    with make_client() as c:
        resp = c.post("/cluster/batch", json={"logs": _logs(20, seed=11)})
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 20
        assert all(len(item["results"]) == 3 for item in body)


def test_cluster_batch_empty() -> None:
    with make_client() as c:
        resp = c.post("/cluster/batch", json={"logs": []})
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------- stats


def test_stats_after_processing() -> None:
    with make_client() as c:
        # Push at least 21 logs through so the counters are non-trivial.
        assert c.post("/cluster", json=_one_log()).status_code == 200
        assert c.post("/cluster/batch", json={"logs": _logs(20, seed=11)}).status_code == 200

        resp = c.get("/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_processed"] >= 21
        assert "throughput_per_sec" in body
        assert "total_clusters" in body
        assert "silhouette" in body  # float or null
        assert body["silhouette"] is None or isinstance(body["silhouette"], float)


# ------------------------------------------------------------------------- clusters


def test_clusters_all_algorithms() -> None:
    with make_client() as c:
        resp = c.get("/clusters")
        assert resp.status_code == 200
        body = resp.json()
        for algorithm in ClusteringEngine.ALGORITHMS:
            assert algorithm in body
            assert isinstance(body[algorithm], list)


def test_clusters_for_algorithm() -> None:
    with make_client() as c:
        resp = c.get("/clusters/kmeans")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


def test_clusters_unknown_algorithm_404() -> None:
    with make_client() as c:
        resp = c.get("/clusters/bogus")
        assert resp.status_code == 404


def test_cluster_detail() -> None:
    with make_client() as c:
        resp = c.get("/clusters/kmeans/0")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        assert body["algorithm"] == "kmeans"
        assert body["cluster_id"] == 0


def test_cluster_detail_unknown_algorithm_404() -> None:
    with make_client() as c:
        resp = c.get("/clusters/bogus/0")
        assert resp.status_code == 404


# ------------------------------------------------------------------------- patterns


def test_patterns_non_empty() -> None:
    with make_client() as c:
        resp = c.get("/patterns")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) > 0
        first = body[0]
        assert "pattern_id" in first
        assert "pattern_type" in first
        assert "count" in first


# ------------------------------------------------------------------------ anomalies


def test_anomalies_list() -> None:
    with make_client() as c:
        resp = c.get("/anomalies", params={"limit": 10})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# -------------------------------------------------------------------------- scatter


def test_scatter_points_shape() -> None:
    with make_client() as c:
        resp = c.get("/scatter/kmeans", params={"limit": 100})
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        for point in body:
            assert set(point.keys()) == {"x", "y", "cluster_id"}


def test_scatter_unknown_algorithm_404() -> None:
    with make_client() as c:
        resp = c.get("/scatter/bogus")
        assert resp.status_code == 404


# --------------------------------------------------------------------------- config


def test_config_endpoint() -> None:
    with make_client() as c:
        resp = c.get("/config")
        assert resp.status_code == 200
        body = resp.json()
        for section in ("kmeans", "dbscan", "hdbscan", "realtime"):
            assert section in body

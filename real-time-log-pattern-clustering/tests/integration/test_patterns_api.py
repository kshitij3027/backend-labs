"""Integration tests for the batch pattern-mining REST endpoints (C18).

These drive the real app via :class:`fastapi.testclient.TestClient` **as a context manager** so
the startup lifespan runs — which warms the engine *and* stashes the warm-up corpus on
``app.state.corpus`` for the miners to consume. The ``GET /patterns/temporal`` and
``GET /patterns/performance`` endpoints then mine that exact corpus, so the responses reflect
genuine end-to-end mining (no mocks).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import create_app
from src.log_generator import generate_logs


def make_client() -> TestClient:
    """Build a TestClient over an app warmed (and corpus-loaded) on a small generated batch.

    Use as ``with make_client() as c:`` so the lifespan runs — that is what warms the engine
    and populates ``app.state.corpus`` that the pattern endpoints mine.
    """
    app = create_app(warmup_logs=generate_logs(600, seed=3))
    return TestClient(app)


def test_temporal_endpoint_returns_patterns() -> None:
    """GET /patterns/temporal -> 200 with a non-empty list of well-shaped patterns."""
    with make_client() as c:
        resp = c.get("/patterns/temporal")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert body, "expected at least one temporal pattern"
        for item in body:
            assert "kind" in item
            assert "description" in item
            assert "window" in item


def test_performance_endpoint_returns_bands_and_signatures() -> None:
    """GET /patterns/performance -> 200 dict with bands, signatures, total_with_latency."""
    with make_client() as c:
        resp = c.get("/patterns/performance")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        assert {"bands", "signatures", "total_with_latency"} <= body.keys()

        assert body["total_with_latency"] > 0
        assert body["bands"], "expected non-empty latency bands"
        assert isinstance(body["signatures"], list)

        # Each band carries the documented summary fields.
        for band in body["bands"]:
            assert {"band", "count", "min_ms", "mean_ms", "p95_ms", "max_ms"} <= band.keys()


def test_performance_bands_ascend_by_mean_ms() -> None:
    """The latency bands are ordered fastest -> slowest by mean_ms."""
    with make_client() as c:
        body = c.get("/patterns/performance").json()
        means = [band["mean_ms"] for band in body["bands"]]
        assert means == sorted(means)


def test_temporal_endpoint_503_before_warmup() -> None:
    """Without entering the lifespan the engine is unwarmed, so the endpoint guards with 503."""
    # A bare client (no ``with``) never runs startup, so engine/corpus are absent.
    client = TestClient(create_app(warmup_logs=generate_logs(600, seed=3)))
    resp = client.get("/patterns/temporal")
    assert resp.status_code == 503

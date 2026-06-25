"""Integration tests for the C19 behavioral + sequence pattern-mining endpoints.

These drive the real app via :class:`fastapi.testclient.TestClient` **as a context manager**
so the startup lifespan runs — which warms the engine *and* stashes the warm-up corpus on
``app.state.corpus`` for the miners to consume. ``GET /patterns/behavioral`` and
``GET /patterns/sequence`` then mine that exact corpus end-to-end (no mocks), exercising the
same code paths the dashboard's PatternPanels hits.
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
    app = create_app(warmup_logs=generate_logs(800, seed=4))
    return TestClient(app)


def test_behavioral_endpoint_returns_groups() -> None:
    """GET /patterns/behavioral -> 200 dict with a non-empty groups list + entities > 0."""
    with make_client() as c:
        resp = c.get("/patterns/behavioral")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        assert {"groups", "entities"} <= body.keys()

        assert body["entities"] > 0
        assert isinstance(body["groups"], list)
        assert body["groups"], "expected at least one behavior cohort"

        # Each group carries the documented summary fields.
        for g in body["groups"]:
            assert {
                "group",
                "label",
                "count",
                "mean_requests",
                "mean_error_rate",
                "mean_response_ms",
                "example_entities",
            } <= g.keys()
            assert isinstance(g["example_entities"], list)

        # The partition is complete: group counts sum to the entity total.
        assert sum(g["count"] for g in body["groups"]) == body["entities"]


def test_sequence_endpoint_returns_anomaly_summary() -> None:
    """GET /patterns/sequence -> 200 dict with analyzed > 0, an anomalies list, and window."""
    with make_client() as c:
        resp = c.get("/patterns/sequence")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        assert {"analyzed", "window", "anomalies"} <= body.keys()

        assert body["analyzed"] > 0
        assert isinstance(body["anomalies"], list)
        assert isinstance(body["window"], int)

        # Any flagged anomaly carries the documented shape.
        for a in body["anomalies"]:
            assert {"entity", "score", "length", "sample_events"} <= a.keys()
            assert 0.0 <= a["score"] <= 1.0
            assert isinstance(a["sample_events"], list)


def test_behavioral_endpoint_503_before_warmup() -> None:
    """Without entering the lifespan the engine is unwarmed, so the endpoint guards with 503."""
    # A bare client (no ``with``) never runs startup, so engine/corpus are absent.
    client = TestClient(create_app(warmup_logs=generate_logs(800, seed=4)))
    resp = client.get("/patterns/behavioral")
    assert resp.status_code == 503


def test_sequence_endpoint_503_before_warmup() -> None:
    """The sequence endpoint likewise guards with 503 before warm-up completes."""
    client = TestClient(create_app(warmup_logs=generate_logs(800, seed=4)))
    resp = client.get("/patterns/sequence")
    assert resp.status_code == 503

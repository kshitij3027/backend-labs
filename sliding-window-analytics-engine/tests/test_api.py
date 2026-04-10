"""HTTP layer tests for the sliding-window analytics engine.

These tests exercise the FastAPI app defined in :mod:`src.main` via
``fastapi.testclient.TestClient``. The background
:class:`LogEventGenerator` is suppressed by setting the
``DISABLE_GENERATOR`` env var *before* the app is imported — this keeps
unit tests deterministic and prevents the 600 evt/s producer from
racing with assertions on window counts.

TestClient drives the FastAPI lifespan via its context manager, so all
tests create a fresh client-per-test inside a ``with`` block to ensure
clean setup and teardown.
"""

from __future__ import annotations

import importlib
import os

import pytest

# Disable the background generator *before* importing the app so the
# lifespan skips the asyncio.create_task call entirely.
os.environ["DISABLE_GENERATOR"] = "1"

from fastapi.testclient import TestClient  # noqa: E402

import src.main as main_module  # noqa: E402

# Re-import to make sure module-level ``app`` picks up the env var state.
main_module = importlib.reload(main_module)


@pytest.fixture
def client() -> TestClient:
    """A TestClient bound to the reloaded ``src.main`` app.

    We yield from within the context manager so the lifespan runs its
    startup + shutdown phases around the test body.
    """
    with TestClient(main_module.app) as c:
        yield c


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["active_windows"] >= 7


def test_metric_ingestion(client: TestClient) -> None:
    response = client.post(
        "/api/metric",
        json={"metric": "response_time", "value": 100.0},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] is True
    assert isinstance(body["event_id"], str) and body["event_id"]


def test_metric_validation_empty_metric(client: TestClient) -> None:
    response = client.post(
        "/api/metric",
        json={"metric": "", "value": 1.0},
    )
    assert response.status_code == 422


def test_metric_validation_non_numeric_value(client: TestClient) -> None:
    response = client.post(
        "/api/metric",
        json={"metric": "response_time", "value": "not-a-number"},
    )
    assert response.status_code == 422


def test_stats_endpoint_shape(client: TestClient) -> None:
    response = client.get("/api/stats")
    assert response.status_code == 200
    body = response.json()
    assert "metrics" in body
    assert "active_windows" in body
    assert body["active_windows"] >= 7

    metrics = body["metrics"]
    assert "response_time" in metrics
    rt = metrics["response_time"]
    for resolution in ("1m", "15m", "4h"):
        assert resolution in rt, f"response_time missing resolution {resolution!r}"
        # Each WindowResult should round-trip via asdict() with the
        # canonical field set.
        result = rt[resolution]
        for field in (
            "window_name",
            "resolution",
            "window_start",
            "window_end",
            "count",
            "sum",
            "average",
            "min",
            "max",
            "std_dev",
        ):
            assert field in result, f"field {field!r} missing from WindowResult JSON"


def test_stats_reflects_ingested_metric(client: TestClient) -> None:
    # Capture baseline count for response_time.1m.
    before = client.get("/api/stats").json()
    before_count = before["metrics"]["response_time"]["1m"]["count"]

    # Ingest a single event.
    ingest = client.post(
        "/api/metric",
        json={"metric": "response_time", "value": 42.0},
    )
    assert ingest.status_code == 200

    after = client.get("/api/stats").json()
    after_count = after["metrics"]["response_time"]["1m"]["count"]
    assert after_count == before_count + 1, (
        f"expected count to increment from {before_count} to {before_count + 1}, "
        f"got {after_count}"
    )

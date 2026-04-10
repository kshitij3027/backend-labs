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

Commit 6 note: because POST /api/metric now routes through an async
queue (drained by a dedicated consumer task) rather than calling
``window_manager.dispatch`` synchronously, tests that assert on stats
*immediately* after a POST need a tiny settle window — we poll
``/api/stats`` for up to a second instead of reading it once.
"""

from __future__ import annotations

import importlib
import os
import time

import pytest

# Disable the background generator *before* importing the app so the
# lifespan skips the asyncio.create_task call entirely.
os.environ["DISABLE_GENERATOR"] = "1"

from fastapi.testclient import TestClient  # noqa: E402

import src.main as main_module  # noqa: E402

# Re-import to make sure module-level ``app`` picks up the env var state.
main_module = importlib.reload(main_module)


def _poll_count(
    client: TestClient,
    metric: str,
    resolution: str,
    expected: int,
    timeout: float = 2.0,
) -> int:
    """Poll ``/api/stats`` until the target count appears (or the timeout elapses).

    The ingest pipeline drains into the window manager on the event
    loop, so a just-posted event may not be reflected in the very next
    stats read. This helper retries for a short window to keep the
    Commit 6 async path green without introducing flakiness.
    """
    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        body = client.get("/api/stats").json()
        last = body["metrics"][metric][resolution]["count"]
        if last >= expected:
            return last
        time.sleep(0.05)
    return last


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

    # Ingest a single event. Because the event now flows through the
    # async ingest queue, we poll /api/stats instead of reading once.
    ingest = client.post(
        "/api/metric",
        json={"metric": "response_time", "value": 42.0},
    )
    assert ingest.status_code == 200

    expected = before_count + 1
    after_count = _poll_count(client, "response_time", "1m", expected)
    assert after_count == expected, (
        f"expected count to increment from {before_count} to {expected}, "
        f"got {after_count}"
    )


def test_stats_includes_ingest_counters(client: TestClient) -> None:
    """Commit 6: /api/stats should expose the ingest-pipeline snapshot."""
    body = client.get("/api/stats").json()
    assert "ingest" in body, "stats response missing 'ingest' key"
    ingest = body["ingest"]
    for key in ("queue_depth", "queue_maxsize", "enqueued", "dropped", "sampled", "processed"):
        assert key in ingest, f"ingest snapshot missing {key!r}"
    assert ingest["queue_maxsize"] >= 1
    assert ingest["queue_depth"] >= 0

    # A fresh POST should bump enqueued and (after the consumer drains)
    # processed — poll briefly to allow the consumer task to run.
    before_enqueued = ingest["enqueued"]
    client.post("/api/metric", json={"metric": "response_time", "value": 1.0})
    deadline = time.monotonic() + 2.0
    after_enqueued = before_enqueued
    while time.monotonic() < deadline:
        after_enqueued = client.get("/api/stats").json()["ingest"]["enqueued"]
        if after_enqueued > before_enqueued:
            break
        time.sleep(0.05)
    assert after_enqueued > before_enqueued

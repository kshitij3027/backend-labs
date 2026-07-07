"""Unit tests for GET /api/v1/correlations — ordering, filtering, and clamping.

The app gets a hand-wired Runtime whose engine was pre-filled through a real
detect() call with a stubbed detector (so the deque/counters go through the
production recording path), and the endpoint reads pure in-memory state.
"""

import time

import pytest
from fastapi.testclient import TestClient

from src.aggregation import MetricAggregator
from src.api import create_app
from src.config import Settings
from src.engine import CorrelationEngine
from src.main import Runtime
from src.models import Correlation, CorrelationType, EventRef, SourceType

EPOCH = 1000.0


def fab_corr(i: int, strength: float) -> Correlation:
    """A fabricated temporal correlation; higher ``i`` = newer detection."""
    ts = EPOCH + i
    return Correlation(
        id=f"corr-{i}",
        detected_at=ts + 2.0,
        correlation_type=CorrelationType.TEMPORAL,
        event_a=EventRef(
            id=f"ev-{i}a", source=SourceType.WEB, service="nginx",
            message="request", timestamp=ts,
        ),
        event_b=EventRef(
            id=f"ev-{i}b", source=SourceType.DATABASE, service="postgresql",
            message="query", timestamp=ts + 1.0,
        ),
        strength=strength,
        confidence=0.5,
        details={"dt_seconds": 1.0, "support": i + 1},
    )


class StubDetector:
    """A detector that returns a canned batch (drives the real recording path)."""

    name = "stub"

    def __init__(self, out: list[Correlation]) -> None:
        self._out = out

    def detect(self, ctx) -> list[Correlation]:
        return self._out


@pytest.fixture()
def client() -> TestClient:
    """A TestClient over a Runtime whose engine holds 3 fabricated correlations."""
    settings = Settings(_env_file=None)
    engine = CorrelationEngine(settings, MetricAggregator(), store=None)
    corrs = [fab_corr(0, 0.3), fab_corr(1, 0.6), fab_corr(2, 0.9)]
    engine.detectors = [StubDetector(corrs)]
    assert engine.detect([], [], now=EPOCH + 4.0) == corrs  # sanity: all recorded
    runtime = Runtime(settings=settings, started_at=time.monotonic(), engine=engine)
    return TestClient(create_app(runtime=runtime))


def test_default_returns_all_newest_first(client):
    resp = client.get("/api/v1/correlations")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"count", "correlations"}
    assert body["count"] == 3
    assert body["count"] == len(body["correlations"])
    assert [c["id"] for c in body["correlations"]] == ["corr-2", "corr-1", "corr-0"]
    assert body["correlations"][0]["correlation_type"] == "temporal"


def test_min_strength_filters(client):
    body = client.get("/api/v1/correlations", params={"min_strength": 0.5}).json()
    assert body["count"] == 2
    assert [c["id"] for c in body["correlations"]] == ["corr-2", "corr-1"]
    assert all(c["strength"] >= 0.5 for c in body["correlations"])


def test_limit_returns_newest(client):
    body = client.get("/api/v1/correlations", params={"limit": 1}).json()
    assert body["count"] == 1
    assert body["correlations"][0]["id"] == "corr-2"


def test_limit_zero_clamps_to_one(client):
    resp = client.get("/api/v1/correlations", params={"limit": 0})
    assert resp.status_code == 200  # silent clamp — never a 422
    assert resp.json()["count"] == 1


def test_limit_overshoot_clamps_silently(client):
    resp = client.get("/api/v1/correlations", params={"limit": 999999})
    assert resp.status_code == 200
    assert resp.json()["count"] == 3  # everything available, no error


def test_missing_engine_degrades_to_empty_feed():
    bare = Runtime(settings=Settings(_env_file=None), started_at=time.monotonic())
    client = TestClient(create_app(runtime=bare))
    resp = client.get("/api/v1/correlations")
    assert resp.status_code == 200
    assert resp.json() == {"count": 0, "correlations": []}

"""Unit tests for GET /api/v1/correlations/types/{type} — purity, 422, clamping.

The path parameter is typed as the CorrelationType enum, so FastAPI itself
rejects an unknown type with a 422: unlike the numeric params (which clamp
silently), a bogus type is a wrong API call, not a tuning mistake. The filter
must be pure — only correlations of the requested type, newest first.
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


def fab_corr(i: int, ctype: CorrelationType) -> Correlation:
    """Fabricated correlation #i (higher ``i`` = newer detection)."""
    ts = EPOCH + i
    return Correlation(
        id=f"corr-{i}",
        detected_at=ts,
        correlation_type=ctype,
        event_a=EventRef(
            id=f"ev-{i}a", source=SourceType.WEB, service="nginx",
            message="request", timestamp=ts - 2.0,
        ),
        event_b=EventRef(
            id=f"ev-{i}b", source=SourceType.DATABASE, service="postgresql",
            message="query", timestamp=ts - 1.0,
        ),
        strength=0.7,
        confidence=0.5,
        details={},
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
    """A TestClient over an engine holding 3 session_based + 2 temporal corrs."""
    settings = Settings(_env_file=None)
    engine = CorrelationEngine(settings, MetricAggregator(), store=None)
    corrs = [
        fab_corr(0, CorrelationType.SESSION),
        fab_corr(1, CorrelationType.TEMPORAL),
        fab_corr(2, CorrelationType.SESSION),
        fab_corr(3, CorrelationType.TEMPORAL),
        fab_corr(4, CorrelationType.SESSION),
    ]
    engine.detectors = [StubDetector(corrs)]
    assert engine.detect([], [], now=EPOCH + 10.0) == corrs  # sanity: all recorded
    runtime = Runtime(settings=settings, started_at=time.monotonic(), engine=engine)
    return TestClient(create_app(runtime=runtime))


def test_type_filter_returns_only_that_type_newest_first(client):
    resp = client.get("/api/v1/correlations/types/session_based")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"correlation_type", "count", "correlations"}
    assert body["correlation_type"] == "session_based"
    assert body["count"] == 3 == len(body["correlations"])
    assert [c["id"] for c in body["correlations"]] == ["corr-4", "corr-2", "corr-0"]
    assert all(c["correlation_type"] == "session_based" for c in body["correlations"])


def test_other_type_counts_independently(client):
    body = client.get("/api/v1/correlations/types/temporal").json()
    assert body["count"] == 2
    assert all(c["correlation_type"] == "temporal" for c in body["correlations"])


def test_bogus_type_is_a_422(client):
    assert client.get("/api/v1/correlations/types/bogus").status_code == 422


def test_limit_zero_clamps_to_one(client):
    resp = client.get(
        "/api/v1/correlations/types/session_based", params={"limit": 0}
    )
    assert resp.status_code == 200  # silent clamp — never a 422
    body = resp.json()
    assert body["count"] == 1
    assert body["correlations"][0]["id"] == "corr-4"  # the newest survives


def test_limit_overshoot_clamps_silently(client):
    resp = client.get(
        "/api/v1/correlations/types/session_based", params={"limit": 999999}
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 3  # everything available, no error


def test_missing_engine_degrades_to_empty_feed():
    bare = Runtime(settings=Settings(_env_file=None), started_at=time.monotonic())
    body = (
        TestClient(create_app(runtime=bare))
        .get("/api/v1/correlations/types/temporal")
        .json()
    )
    assert body == {"correlation_type": "temporal", "count": 0, "correlations": []}

"""Unit tests for GET /api/v1/correlations/stats (spec area: correlation stats).

The response is the SPEC-VERBATIM 4-key payload — exactly total / types /
avg_strength / recent_count and NOTHING else (operational extras live in
/api/v1/dashboard on purpose). Correlations are fabricated at controlled
detected_at offsets from the wall clock, because the endpoint windows
recent_count over the 60 seconds before "now" (wall time inside the handler).
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

SPEC_KEYS = {"total", "types", "avg_strength", "recent_count"}


def fab_corr(i: int, strength: float, detected_at: float) -> Correlation:
    return Correlation(
        id=f"corr-{i}",
        detected_at=detected_at,
        correlation_type=CorrelationType.TEMPORAL,
        event_a=EventRef(
            id=f"ev-{i}a", source=SourceType.WEB, service="nginx",
            message="request", timestamp=detected_at - 2.0,
        ),
        event_b=EventRef(
            id=f"ev-{i}b", source=SourceType.DATABASE, service="postgresql",
            message="query", timestamp=detected_at - 1.0,
        ),
        strength=strength,
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


def make_client(corrs: list[Correlation]) -> TestClient:
    """A TestClient over a Runtime whose engine recorded ``corrs`` for real."""
    settings = Settings(_env_file=None)
    engine = CorrelationEngine(settings, MetricAggregator(), store=None)
    if corrs:
        engine.detectors = [StubDetector(corrs)]
        assert engine.detect([], [], now=time.time()) == corrs  # sanity: recorded
    runtime = Runtime(settings=settings, started_at=time.monotonic(), engine=engine)
    return TestClient(create_app(runtime=runtime))


def test_stats_payload_has_exactly_the_four_spec_keys():
    now = time.time()
    client = make_client(
        [fab_corr(0, 0.3, now - 5.0), fab_corr(1, 0.6, now - 30.0)]
    )
    resp = client.get("/api/v1/correlations/stats")
    assert resp.status_code == 200
    assert set(resp.json().keys()) == SPEC_KEYS  # spec-verbatim: no extras, ever


def test_avg_strength_is_strength_sum_over_total():
    now = time.time()
    strengths = [0.3, 0.6, 0.9]
    client = make_client(
        [fab_corr(i, s, now - 5.0) for i, s in enumerate(strengths)]
    )
    body = client.get("/api/v1/correlations/stats").json()
    assert body["total"] == 3
    assert body["types"] == {"temporal": 3}
    assert body["avg_strength"] == pytest.approx(sum(strengths) / 3, abs=1e-4)


def test_recent_count_windows_the_last_60_seconds():
    now = time.time()
    client = make_client(
        [
            fab_corr(0, 0.5, now - 5.0),  # inside the window
            fab_corr(1, 0.5, now - 30.0),  # inside the window
            fab_corr(2, 0.5, now - 120.0),  # outside: older than 60 s
        ]
    )
    body = client.get("/api/v1/correlations/stats").json()
    assert body["total"] == 3
    assert body["recent_count"] == 2


def test_empty_engine_returns_zeros():
    body = make_client([]).get("/api/v1/correlations/stats").json()
    assert body == {"total": 0, "types": {}, "avg_strength": 0.0, "recent_count": 0}


def test_missing_engine_degrades_to_zeros():
    bare = Runtime(settings=Settings(_env_file=None), started_at=time.monotonic())
    resp = TestClient(create_app(runtime=bare)).get("/api/v1/correlations/stats")
    assert resp.status_code == 200
    assert resp.json() == {"total": 0, "types": {}, "avg_strength": 0.0, "recent_count": 0}

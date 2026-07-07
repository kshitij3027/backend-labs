"""Unit tests for GET /api/v1/dashboard — the single-poll payload's exact shape.

A hand-wired Runtime (real generator + collector ticked on a simulated clock,
engine populated through a stubbed detect() pass, real AlertManager) exercises
every section; assertions pin the exact top-level key set, the 5x5 symmetric
matrix, the <= 60 ascending timeline buckets, the <= 200-point scatter with its
exact per-point fields, and the 20-newest feeds. A bare Runtime proves every
section degrades to an empty shape instead of a 500.
"""

import random
import time

import pytest
from fastapi.testclient import TestClient

from src.aggregation import MetricAggregator
from src.alerts import AlertManager
from src.api import create_app
from src.collector import LogCollector
from src.config import Settings
from src.engine import CorrelationEngine
from src.generators import LogGenerator
from src.main import Runtime
from src.models import Correlation, CorrelationType, EventRef, SourceType

EPOCH = 1000.0
N_CORRS = 250  # > the 200-point scatter cap, well under the 2000 deque cap

TOP_LEVEL_KEYS = {
    "generated_at",
    "status",
    "stats",
    "timeline",
    "scatter",
    "matrix",
    "recent_correlations",
    "recent_logs",
    "alerts",
}

_SOURCES = list(SourceType)


def fab_corr(i: int) -> Correlation:
    """Fabricated corr #i cycling adjacent source pairs; some are alert-strong."""
    ts = EPOCH + i * 0.7  # spread across many 10 s timeline buckets
    src_a = _SOURCES[i % 5]
    src_b = _SOURCES[(i + 1) % 5]
    return Correlation(
        id=f"corr-{i}",
        detected_at=ts,
        correlation_type=CorrelationType.TEMPORAL,
        event_a=EventRef(
            id=f"ev-{i}a", source=src_a, service=src_a.value,
            message="a", timestamp=ts - 2.0,
        ),
        event_b=EventRef(
            id=f"ev-{i}b", source=src_b, service=src_b.value,
            message="b", timestamp=ts - 1.0,
        ),
        strength=0.3 + 0.1 * (i % 7),  # peaks at 0.9 -> trips the warning rule
        confidence=0.95,
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
    """A TestClient over a fully hand-wired Runtime (no Redis anywhere)."""
    settings = Settings(_env_file=None)
    generator = LogGenerator(settings, rng=random.Random(7))
    aggregator = MetricAggregator()
    collector = LogCollector(settings, generator, aggregator, store=None)
    for i in range(5):  # populate the recent-logs buffer deterministically
        collector.tick(EPOCH + i)

    alerts = AlertManager(settings)
    engine = CorrelationEngine(settings, aggregator, store=None, alerts=alerts)
    corrs = [fab_corr(i) for i in range(N_CORRS)]
    engine.detectors = [StubDetector(corrs)]
    assert len(engine.detect([], [], now=EPOCH + 200.0)) == N_CORRS

    runtime = Runtime(
        settings=settings,
        started_at=time.monotonic(),
        generator=generator,
        aggregator=aggregator,
        collector=collector,
        engine=engine,
        alerts=alerts,
    )
    return TestClient(create_app(runtime=runtime))


def test_top_level_keys_are_exactly_the_dashboard_contract(client):
    resp = client.get("/api/v1/dashboard")
    assert resp.status_code == 200
    assert set(resp.json().keys()) == TOP_LEVEL_KEYS


def test_status_section_shape(client):
    status = client.get("/api/v1/dashboard").json()["status"]
    assert set(status.keys()) == {"healthy", "redis", "pipeline_running", "active_scenario"}
    assert status["healthy"] is True
    assert status["redis"] is False  # no store wired into this runtime
    assert status["pipeline_running"] is False  # no background task under tests
    assert status["active_scenario"] is None or isinstance(status["active_scenario"], str)


def test_stats_section_is_spec_keys_plus_operational_extras(client):
    stats = client.get("/api/v1/dashboard").json()["stats"]
    spec_keys = {"total", "types", "avg_strength", "recent_count"}
    operational_keys = {
        "events_processed",
        "events_per_sec",
        "parse_errors",
        "uptime_seconds",
        "memory_mb",
        "alerts_total",
    }
    assert spec_keys | operational_keys <= set(stats.keys())
    assert stats["total"] == N_CORRS
    assert stats["events_processed"] > 0  # the collector really ticked
    assert stats["alerts_total"] >= 1  # strong fabricated corrs fired warnings
    assert stats["uptime_seconds"] >= 0.0


def test_matrix_is_five_by_five_and_symmetric(client):
    matrix = client.get("/api/v1/dashboard").json()["matrix"]
    assert matrix["sources"] == [source.value for source in SourceType]
    assert len(matrix["cells"]) == 5
    assert all(len(row) == 5 for row in matrix["cells"])
    for i in range(5):
        for j in range(5):
            assert matrix["cells"][i][j] == matrix["cells"][j][i]
    assert any(cell > 0.0 for row in matrix["cells"] for cell in row)


def test_timeline_bounded_and_ascending(client):
    timeline = client.get("/api/v1/dashboard").json()["timeline"]
    assert 0 < len(timeline) <= 60
    bucket_starts = [bucket["t"] for bucket in timeline]
    assert bucket_starts == sorted(bucket_starts)
    assert len(set(bucket_starts)) == len(bucket_starts)  # strictly ascending


def test_scatter_capped_at_200_with_exact_point_fields(client):
    scatter = client.get("/api/v1/dashboard").json()["scatter"]
    assert len(scatter) == 200  # 250 retained -> the newest 200
    assert all(
        set(point.keys()) == {"strength", "confidence", "type", "detected_at"}
        for point in scatter
    )


def test_feeds_are_capped_at_20_newest(client):
    body = client.get("/api/v1/dashboard").json()
    assert len(body["recent_correlations"]) == 20
    assert body["recent_correlations"][0]["id"] == f"corr-{N_CORRS - 1}"  # newest first
    assert 0 < len(body["recent_logs"]) <= 20
    assert 1 <= len(body["alerts"]) <= 20
    assert body["alerts"][0]["severity"] in {"warning", "critical"}


def test_bare_runtime_degrades_to_empty_sections():
    bare = Runtime(settings=Settings(_env_file=None), started_at=time.monotonic())
    resp = TestClient(create_app(runtime=bare)).get("/api/v1/dashboard")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == TOP_LEVEL_KEYS  # shape survives missing pieces
    assert body["status"]["redis"] is False
    assert body["stats"]["total"] == 0
    assert body["timeline"] == []
    assert body["scatter"] == []
    assert body["recent_correlations"] == []
    assert body["recent_logs"] == []
    assert body["alerts"] == []

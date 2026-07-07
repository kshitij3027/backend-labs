"""Integration: the complete C7 v1 API surface over a live, hand-wired runtime.

Drives ~20 simulated 1-second ticks through the REAL pipeline stages — seeded
generator -> parsers -> collector -> aggregator -> engine wired with the
PatternLearner and AlertManager, everything against a real Redis — then
exercises the HTTP surface the way the dashboard and the C8 E2E verifier will:
stats keeps the exact 4-key contract and grows between polls, type filtering
stays pure, the dashboard payload is complete, pattern baselines land in Redis
(and re-detected patterns carry ``details.pattern_count`` >= 2), and the
per-minute durability counters agree with the engine.

Every pipeline call passes an explicit simulated ``now``; ``recent_count``
(wall-clock windowed inside the stats endpoint) is deliberately left
unasserted here for that reason.
"""

import math
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
from src.models import Correlation, CorrelationType, EventRef, LogEvent, SourceType
from src.patterns import PatternLearner
from src.store import KEY_PATTERN_INDEX, RedisStore

EPOCH = 1000.0

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


def build_runtime(redis_url: str) -> Runtime:
    """Hand-wire every production Runtime piece (as Runtime.build would)."""
    settings = Settings(_env_file=None, events_per_second=60)
    store = RedisStore(redis_url)
    generator = LogGenerator(settings, rng=random.Random(5))
    aggregator = MetricAggregator()
    collector = LogCollector(settings, generator, aggregator, store=store)
    alerts = AlertManager(settings)
    patterns = PatternLearner(settings, store)
    engine = CorrelationEngine(
        settings, aggregator, store=store, patterns=patterns, alerts=alerts
    )
    return Runtime(
        settings=settings,
        started_at=time.monotonic(),
        store=store,
        generator=generator,
        aggregator=aggregator,
        collector=collector,
        engine=engine,
        patterns=patterns,
        alerts=alerts,
    )


def drive(runtime: Runtime, first_tick: int, ticks: int, pending: list[LogEvent]) -> None:
    """Simulate the pipeline loop: 1 s collector ticks, detection every 2nd tick."""
    settings = runtime.settings
    for i in range(first_tick, first_tick + ticks):
        now = EPOCH + i
        pending.extend(runtime.collector.tick(now))
        if i % 2 == 1:  # the production 2 s detection cadence
            cutoff = now - settings.window_seconds
            window = [ev for ev in runtime.collector.buffer if ev.timestamp >= cutoff]
            runtime.engine.detect(list(pending), window, now)
            pending.clear()


def test_full_api_surface_over_live_pipeline(redis_client, redis_url):
    runtime = build_runtime(redis_url)
    client = TestClient(create_app(runtime=runtime))
    pending: list[LogEvent] = []

    drive(runtime, 0, 14, pending)

    # --- stats: exact spec shape, non-empty, then grows between polls -----------
    first = client.get("/api/v1/correlations/stats")
    assert first.status_code == 200
    stats_1 = first.json()
    assert set(stats_1.keys()) == {"total", "types", "avg_strength", "recent_count"}
    assert stats_1["total"] > 0
    assert 0.0 < stats_1["avg_strength"] <= 1.0

    drive(runtime, 14, 6, pending)  # fresh journeys -> fresh detections
    stats_2 = client.get("/api/v1/correlations/stats").json()
    assert stats_2["total"] > stats_1["total"]

    # --- type filter purity ------------------------------------------------------
    engine = runtime.engine
    type_value, type_count = next(iter(engine.by_type.items()))
    body = client.get(
        f"/api/v1/correlations/types/{type_value}", params={"limit": 1000}
    ).json()
    assert body["correlation_type"] == type_value
    assert body["count"] == type_count  # everything retained, nothing foreign
    assert all(c["correlation_type"] == type_value for c in body["correlations"])

    # --- dashboard: complete payload, straight from in-memory state --------------
    dash = client.get("/api/v1/dashboard").json()
    assert set(dash.keys()) == TOP_LEVEL_KEYS
    assert dash["status"]["redis"] is True  # the store really answered
    assert dash["stats"]["total"] == engine.total
    assert dash["matrix"]["sources"] == [source.value for source in SourceType]
    assert len(dash["matrix"]["cells"]) == 5
    assert dash["timeline"]
    assert dash["recent_logs"] and len(dash["recent_logs"]) <= 20
    assert isinstance(dash["alerts"], list)
    settings = runtime.settings
    alert_worthy = any(
        corr.strength >= settings.alert_strength_threshold
        and corr.confidence >= settings.alert_confidence_threshold
        for corr in engine.correlations
    )
    if alert_worthy:  # full-coverage session journeys score 1.0 -> warnings
        assert dash["alerts"], "alert-worthy correlations exist but the feed is empty"
        assert dash["stats"]["alerts_total"] >= 1

    # --- pattern learning: baselines persisted + re-detections counted -----------
    pattern_keys = [
        key
        for key in redis_client.scan_iter(match="corr:pattern:*")
        if key != KEY_PATTERN_INDEX
    ]
    assert pattern_keys, "no pattern baselines were persisted to Redis"
    assert redis_client.zcard(KEY_PATTERN_INDEX) == len(pattern_keys)
    assert any(
        corr.details.get("pattern_count", 0) >= 2 for corr in engine.correlations
    ), "no pattern was re-detected across 20 ticks"

    # --- per-minute durability counters agree with the engine ---------------------
    minute_keys = list(redis_client.scan_iter(match="corr:stats:minute:*"))
    assert minute_keys
    minute_total = sum(int(redis_client.hget(key, "total") or 0) for key in minute_keys)
    assert minute_total == engine.total


def test_pattern_baselines_survive_restart(redis_client, redis_url):
    settings = Settings(_env_file=None)
    corr = Correlation(
        id="corr-restart",
        detected_at=EPOCH,
        correlation_type=CorrelationType.SESSION,
        event_a=EventRef(
            id="ev-a", source=SourceType.WEB, service="nginx",
            message="request", timestamp=EPOCH - 3.0,
        ),
        event_b=EventRef(
            id="ev-b", source=SourceType.DATABASE, service="postgresql",
            message="query", timestamp=EPOCH,
        ),
        strength=0.9,
        confidence=0.9,
        details={},
    )

    first = PatternLearner(settings, RedisStore(redis_url))
    for offset in (0.0, 10.0, 20.0):
        first.record([corr], now=EPOCH + offset)

    # The concrete Redis schema: one baseline hash per pattern + the zset index.
    key = "corr:pattern:session_based:database:web"  # endpoints sorted
    row = redis_client.hgetall(key)
    assert int(row["count"]) == 3
    assert float(row["strength_sum"]) == pytest.approx(3 * 0.9)
    assert float(row["strength_sqsum"]) == pytest.approx(3 * 0.81)
    assert float(row["first_seen"]) == EPOCH  # HSETNX kept the first stamp
    assert float(row["last_seen"]) == EPOCH + 20.0
    assert redis_client.zscore(KEY_PATTERN_INDEX, key) == 3.0

    # A brand-new learner (a "restarted process") hydrates the baseline lazily.
    second = PatternLearner(settings, RedisStore(redis_url))
    assessment = second.assess(corr, now=EPOCH + 60.0)
    assert assessment.count == 3
    assert assessment.avg_strength == pytest.approx(0.9)
    assert assessment.boost == pytest.approx(min(0.15, 0.03 * math.log(4)))
    assert not assessment.is_new  # a known pattern now, however strong

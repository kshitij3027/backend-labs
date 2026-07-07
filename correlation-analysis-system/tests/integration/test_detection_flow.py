"""Integration: collector -> engine -> RedisStore persistence over a real Redis.

Drives 12 simulated 1-second ticks through the real pipeline stages (seeded
generator, real parsers/aggregator/collector) with a detection pass every 2nd
tick — the production cadence — then asserts correlations were detected,
mirrored to Redis, and counted consistently between the in-memory engine and
the ``corr:stats`` hash.

Every call passes an explicit simulated ``now``; engine.stats() defaults its
``recent_count`` window to wall-clock time, which is why tests on a simulated
clock must pass ``stats(now=...)`` explicitly instead.
"""

import random

import pytest

from src.aggregation import MetricAggregator
from src.collector import LogCollector
from src.config import Settings
from src.engine import CorrelationEngine
from src.generators import LogGenerator
from src.models import CorrelationType, LogEvent
from src.store import (
    KEY_CORRELATIONS_BY_TYPE,
    KEY_CORRELATIONS_RECENT,
    KEY_STATS,
    RedisStore,
)

EPOCH = 1000.0
TICKS = 12


def test_detection_flow_persists_and_counts_consistently(redis_client, redis_url):
    settings = Settings(_env_file=None, events_per_second=60)
    generator = LogGenerator(settings, rng=random.Random(11))
    aggregator = MetricAggregator()
    store = RedisStore(redis_url)
    collector = LogCollector(settings, generator, aggregator, store=store)
    engine = CorrelationEngine(settings, aggregator, store=store)

    last_now = EPOCH
    pending_new: list[LogEvent] = []
    for i in range(TICKS):
        now = EPOCH + i
        last_now = now
        pending_new.extend(collector.tick(now))
        if i % 2 == 1:  # every 2nd tick — the production 2 s detection cadence
            cutoff = now - settings.window_seconds
            window_events = [ev for ev in collector.buffer if ev.timestamp >= cutoff]
            engine.detect(pending_new, window_events, now)
            pending_new = []

    # --- In-memory results: both C4 detector families fired. ---------------------
    assert engine.total >= 1
    assert CorrelationType.TEMPORAL.value in engine.by_type
    # Journeys span <4 s and quiesce after 2.5 s, so 12 s covers completion.
    assert CorrelationType.SESSION.value in engine.by_type
    stats = engine.stats(now=last_now)
    assert stats["total"] == engine.total
    assert stats["recent_count"] == engine.total  # everything within the last 60 s

    # --- Redis mirror agrees with the engine. ------------------------------------
    assert engine.total <= 2000
    assert redis_client.llen(KEY_CORRELATIONS_RECENT) == engine.total

    redis_stats = redis_client.hgetall(KEY_STATS)
    assert int(redis_stats["total"]) == engine.total
    type_counts = {
        key.removeprefix("type:"): int(value)
        for key, value in redis_stats.items()
        if key.startswith("type:")
    }
    assert sum(type_counts.values()) == engine.total
    assert type_counts == engine.by_type
    assert float(redis_stats["strength_sum"]) == pytest.approx(
        engine.strength_sum, rel=1e-6
    )

    for type_value, count in engine.by_type.items():
        assert count <= 500
        key = KEY_CORRELATIONS_BY_TYPE.format(type=type_value)
        assert redis_client.llen(key) == count

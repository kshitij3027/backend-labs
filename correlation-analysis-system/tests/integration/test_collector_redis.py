"""Integration tests: LogCollector -> RedisStore mirroring against a real Redis.

Covers the C3 Redis contract: a capped, newest-first ``corr:events:recent``
mirror of every tick's parsed events — plus graceful degradation when Redis is
a black hole (bogus host): no exception may ever escape the store.
"""

import random

from src.aggregation import MetricAggregator
from src.collector import LogCollector
from src.config import Settings
from src.generators import LogGenerator
from src.models import LogEvent, SourceType
from src.store import EVENTS_RECENT_MAX, KEY_EVENTS_RECENT, RedisStore

EPOCH = 1000.0


def make_collector(redis_url: str, seed: int = 42) -> LogCollector:
    """A collector wired to a real RedisStore, seeded for deterministic traffic."""
    settings = Settings(_env_file=None, events_per_second=120)
    generator = LogGenerator(settings, rng=random.Random(seed))
    return LogCollector(settings, generator, MetricAggregator(), store=RedisStore(redis_url))


def make_event(i: int) -> LogEvent:
    """A fabricated event with a strictly increasing timestamp per index."""
    return LogEvent(
        id=f"fabricated-{i}",
        timestamp=EPOCH + i * 0.001,
        source=SourceType.WEB,
        service="nginx",
        level="INFO",
        message=f"fabricated event {i}",
    )


def test_ticks_mirror_valid_events_newest_first(redis_client, redis_url):
    collector = make_collector(redis_url)
    for i in range(3):
        assert collector.tick(EPOCH + i), "every tick at 120 eps must emit events"

    length = redis_client.llen(KEY_EVENTS_RECENT)
    assert 0 < length <= EVENTS_RECENT_MAX

    # Entries are valid LogEvent JSON, and index 0 holds the newest mirror entry.
    newest = LogEvent.model_validate_json(redis_client.lindex(KEY_EVENTS_RECENT, 0))
    oldest = LogEvent.model_validate_json(redis_client.lindex(KEY_EVENTS_RECENT, -1))
    assert newest.timestamp >= oldest.timestamp


def test_recent_list_capped_at_1000(redis_client, redis_url):
    store = RedisStore(redis_url)
    store.push_recent_logs([make_event(i) for i in range(1200)])
    assert redis_client.llen(KEY_EVENTS_RECENT) == EVENTS_RECENT_MAX
    # The trim keeps the NEWEST entries: the last event pushed sits at index 0.
    head = LogEvent.model_validate_json(redis_client.lindex(KEY_EVENTS_RECENT, 0))
    assert head.id == "fabricated-1199"


def test_unreachable_redis_degrades_without_raising():
    store = RedisStore("redis://nonexistent-host:9999/0")
    assert store.ping() is False
    store.push_recent_logs([make_event(0)])  # must not raise
    assert store.available is False

"""Integration tests for :class:`~src.state.StateStore` against a *real* Redis.

These run inside Docker where ``REDIS_HOST=redis`` points at the compose ``redis``
service (see ``docker-compose.yml`` / ``make test-int``). When Redis is not reachable —
e.g. a bare host ``pytest`` run — the whole module is skipped, so these tests never gate
a Redis-less environment.

Beyond the same round-trips as the unit suite, each test reads the data back through a
*second, independent* client to prove the value really hit Redis (not a local dict).
"""

from __future__ import annotations

import pytest

from src.clients.redis import make_redis_client, redis_available
from src.config import load_config
from src.state import StateStore

# Probe Redis once at import time; skip the module entirely if it is unreachable so the
# suite stays green wherever Redis is absent (the engine's in-memory fallback is what is
# exercised by the unit tests in that case).
_probe = make_redis_client(load_config())
pytestmark = pytest.mark.skipif(
    not redis_available(_probe),
    reason="redis not reachable (set REDIS_HOST to a running Redis to run these)",
)
if _probe is not None:  # leave the import-time probe connection closed
    _probe.close()


@pytest.fixture
def store() -> StateStore:
    """A Redis-backed store on a freshly-cleared namespace, cleaned up after the test."""
    client = make_redis_client(load_config())
    assert client is not None, "redis client unexpectedly None despite skip guard"
    s = StateStore(client)
    s.clear()
    try:
        yield s
    finally:
        s.clear()
        s.close()


def _fresh_client():
    """Open a second, independent Redis client (cross-client visibility checks)."""
    client = make_redis_client(load_config())
    assert client is not None
    return client


def test_real_redis_selects_redis_backend(store: StateStore) -> None:
    """A live client makes the store report the Redis backend and be available."""
    assert store.backend == "redis"
    assert store.available() is True


def test_stats_round_trip_visible_cross_client(store: StateStore) -> None:
    """Stats saved via the store are readable through a second, independent store."""
    assert store.load_stats() is None

    snapshot = {
        "total_processed": 555,
        "throughput_per_sec": 12.0,
        "total_clusters": 6,
        "patterns_discovered": 4,
        "anomalies_detected": 1,
        "algorithms": ["kmeans", "dbscan"],
    }
    store.save_stats(snapshot)
    assert store.load_stats() == snapshot

    # Prove it actually persisted to Redis, not to a local dict.
    other = StateStore(_fresh_client())
    try:
        assert other.load_stats() == snapshot
    finally:
        other.close()


def test_patterns_round_trip_visible_cross_client(store: StateStore) -> None:
    """Pattern list persists to Redis and is visible to a separate client."""
    assert store.load_patterns() == []

    patterns = [
        {"pattern_id": "p1", "pattern_type": "error_pattern", "count": 9},
        {"pattern_id": "p2", "pattern_type": "performance_pattern", "count": 3},
    ]
    store.save_patterns(patterns)
    assert store.load_patterns() == patterns

    other = StateStore(_fresh_client())
    try:
        assert other.load_patterns() == patterns
    finally:
        other.close()


def test_anomalies_recent_first_and_cap_against_real_redis(store: StateStore) -> None:
    """LPUSH/LTRIM give recent-first ordering capped to ``cap`` on real Redis."""
    for i in range(10):
        store.push_anomaly({"message": f"a{i}", "score": float(i)}, cap=4)

    recent = store.recent_anomalies(limit=50)
    assert len(recent) == 4
    assert [a["message"] for a in recent] == ["a9", "a8", "a7", "a6"]

    # The cap is enforced in Redis itself — a fresh client sees the trimmed list.
    other = StateStore(_fresh_client())
    try:
        cross = other.recent_anomalies(limit=50)
        assert [a["message"] for a in cross] == ["a9", "a8", "a7", "a6"]
    finally:
        other.close()


def test_set_get_json_round_trip_cross_client(store: StateStore) -> None:
    """Generic namespaced JSON persists to Redis; missing keys return ``None``."""
    assert store.get_json("model_meta") is None

    value = {"version": "v7", "labels": [1, 2, 3], "active": True}
    store.set_json("model_meta", value)
    assert store.get_json("model_meta") == value

    other = StateStore(_fresh_client())
    try:
        assert other.get_json("model_meta") == value
    finally:
        other.close()


def test_clear_removes_all_namespaced_keys(store: StateStore) -> None:
    """``clear`` deletes every ``rtlpc:*`` key, observable from a second client."""
    store.save_stats({"total_processed": 1})
    store.save_patterns([{"pattern_id": "p1"}])
    store.push_anomaly({"message": "boom", "score": 9.0})
    store.set_json("k", {"v": 1})

    store.clear()

    other = StateStore(_fresh_client())
    try:
        assert other.load_stats() is None
        assert other.load_patterns() == []
        assert other.recent_anomalies() == []
        assert other.get_json("k") is None
    finally:
        other.close()

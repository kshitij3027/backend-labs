"""Unit tests for :class:`~src.state.StateStore` — in-memory backend only.

These never touch Redis: constructing ``StateStore()`` with no client forces the
in-process fallback, so the suite stays hermetic and Redis-free. The matching
cross-process Redis behaviour is covered by ``tests/integration/test_state_redis.py``.
"""

from __future__ import annotations

import pytest

from src.state import StateStore


@pytest.fixture
def store() -> StateStore:
    """A fresh, memory-backed store per test (no shared mutable state)."""
    s = StateStore()
    s.clear()
    return s


def test_no_client_uses_memory_backend(store: StateStore) -> None:
    """With no Redis client the store selects the in-memory backend and is available."""
    assert store.backend == "memory"
    assert store.available() is True


def test_stats_round_trip_and_empty_default(store: StateStore) -> None:
    """``load_stats`` is None before any save; after save it round-trips the dict."""
    assert store.load_stats() is None

    snapshot = {
        "total_processed": 1234,
        "throughput_per_sec": 42.5,
        "total_clusters": 8,
        "patterns_discovered": 3,
        "anomalies_detected": 2,
        "algorithms": ["kmeans", "dbscan", "hdbscan"],
        "silhouette": 0.61,
    }
    store.save_stats(snapshot)
    assert store.load_stats() == snapshot


def test_save_stats_overwrites_previous(store: StateStore) -> None:
    """A second save replaces the first (single-object semantics)."""
    store.save_stats({"total_processed": 1})
    store.save_stats({"total_processed": 99})
    assert store.load_stats() == {"total_processed": 99}


def test_patterns_round_trip_and_empty_default(store: StateStore) -> None:
    """``load_patterns`` is ``[]`` before save; after save it round-trips the list."""
    assert store.load_patterns() == []

    patterns = [
        {"pattern_id": "p1", "pattern_type": "error_pattern", "count": 5},
        {"pattern_id": "p2", "pattern_type": "security_pattern", "count": 2},
    ]
    store.save_patterns(patterns)
    assert store.load_patterns() == patterns


def test_push_and_recent_anomalies_are_recent_first(store: StateStore) -> None:
    """Anomalies come back newest-first and honour the requested limit."""
    for i in range(5):
        store.push_anomaly({"message": f"alert-{i}", "score": float(i)})

    recent = store.recent_anomalies(limit=3)
    assert [a["message"] for a in recent] == ["alert-4", "alert-3", "alert-2"]


def test_recent_anomalies_limit_zero_returns_empty(store: StateStore) -> None:
    """A non-positive limit yields an empty list without touching state."""
    store.push_anomaly({"message": "x", "score": 1.0})
    assert store.recent_anomalies(limit=0) == []


def test_push_anomaly_respects_cap(store: StateStore) -> None:
    """Only the most-recent ``cap`` anomalies are retained."""
    for i in range(10):
        store.push_anomaly({"message": f"a{i}", "score": float(i)}, cap=3)

    recent = store.recent_anomalies(limit=50)
    assert len(recent) == 3
    assert [a["message"] for a in recent] == ["a9", "a8", "a7"]


def test_set_and_get_json_round_trip(store: StateStore) -> None:
    """Generic namespaced JSON round-trips; missing keys return ``None``."""
    assert store.get_json("model_meta") is None

    value = {"version": "v3", "trained_at": "2026-06-23T00:00:00Z", "labels": [1, 2, 3]}
    store.set_json("model_meta", value)
    assert store.get_json("model_meta") == value


def test_clear_empties_everything(store: StateStore) -> None:
    """``clear`` resets stats, patterns, anomalies, and the generic kv space."""
    store.save_stats({"total_processed": 7})
    store.save_patterns([{"pattern_id": "p1"}])
    store.push_anomaly({"message": "boom", "score": 9.0})
    store.set_json("k", {"v": 1})

    store.clear()

    assert store.load_stats() is None
    assert store.load_patterns() == []
    assert store.recent_anomalies() == []
    assert store.get_json("k") is None


def test_close_is_safe_noop_for_memory(store: StateStore) -> None:
    """Closing a memory-backed store is a harmless no-op (no client to close)."""
    store.close()  # must not raise
    assert store.available() is True

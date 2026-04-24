"""Unit tests for :class:`~src.cache.query_cache.QueryCache`.

Covers the basic LRU contract (eviction order, hit promotion,
``invalidate_all``), the hit/miss counters that feed the
``/api/search/stats`` endpoint, and the latency ring used for the
p95 metric. Keeping the tests pure-Python (no HTTP) so failures
point at the cache implementation rather than FastAPI plumbing.
"""

from __future__ import annotations

from src.cache.query_cache import QueryCache


# ---------------------------------------------------------------------------
# Fresh-state semantics
# ---------------------------------------------------------------------------

def test_fresh_cache_is_empty() -> None:
    """Right after construction the cache has no entries and no stats."""
    cache = QueryCache(max_size=10)
    assert cache.size == 0
    assert cache.hit_ratio == 0.0
    assert cache.p95_latency_ms() == 0.0
    assert cache.hits == 0
    assert cache.misses == 0


# ---------------------------------------------------------------------------
# Basic get/put contract
# ---------------------------------------------------------------------------

def test_put_then_get_returns_value() -> None:
    """A put followed by a get returns the stored value and bumps hits."""
    cache = QueryCache(max_size=10)
    key = ("q1", None, 10, 0)
    cache.put(key, "payload")
    assert cache.get(key) == "payload"
    assert cache.hits == 1
    assert cache.misses == 0


def test_get_miss_returns_none_and_increments_miss_counter() -> None:
    """A miss returns ``None`` and increments the miss counter only."""
    cache = QueryCache(max_size=10)
    assert cache.get(("unknown",)) is None
    assert cache.hits == 0
    assert cache.misses == 1


# ---------------------------------------------------------------------------
# LRU eviction semantics
# ---------------------------------------------------------------------------

def test_eviction_removes_oldest_entry_when_max_size_exceeded() -> None:
    """Filling past ``max_size`` drops the least-recently-used entry."""
    cache = QueryCache(max_size=3)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)
    # Adding a fourth entry should evict ``a`` (oldest, never touched).
    cache.put("d", 4)
    assert cache.size == 3
    # ``a`` is gone — the get is a miss.
    assert cache.get("a") is None
    # ``b``, ``c``, ``d`` remain.
    assert cache.get("b") == 2
    assert cache.get("c") == 3
    assert cache.get("d") == 4


def test_get_promotes_key_to_mru_and_prevents_eviction() -> None:
    """Touching an older key saves it from being evicted first."""
    cache = QueryCache(max_size=3)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)
    # Touch ``a`` — it is now the most recently used.
    assert cache.get("a") == 1
    # Adding a fourth entry must evict ``b`` (now the oldest), not ``a``.
    cache.put("d", 4)
    assert cache.get("a") == 1  # still present
    assert cache.get("b") is None  # evicted
    assert cache.get("c") == 3
    assert cache.get("d") == 4


def test_put_on_existing_key_updates_value_and_promotes() -> None:
    """Re-putting an existing key updates its value and promotes it."""
    cache = QueryCache(max_size=2)
    cache.put("a", 1)
    cache.put("b", 2)
    # Update ``a`` — promotes it to MRU.
    cache.put("a", 99)
    # Adding ``c`` must evict ``b`` (LRU), not ``a``.
    cache.put("c", 3)
    assert cache.get("a") == 99
    assert cache.get("b") is None
    assert cache.get("c") == 3


# ---------------------------------------------------------------------------
# Invalidation
# ---------------------------------------------------------------------------

def test_invalidate_all_clears_store() -> None:
    """``invalidate_all`` drops every cached entry."""
    cache = QueryCache(max_size=10)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)
    assert cache.size == 3
    cache.invalidate_all()
    assert cache.size == 0
    # Subsequent gets are misses.
    assert cache.get("a") is None
    assert cache.get("b") is None
    assert cache.get("c") is None


# ---------------------------------------------------------------------------
# Latency tracking
# ---------------------------------------------------------------------------

def test_record_latency_tracks_values_and_p95_is_reasonable() -> None:
    """Recording 1..200 gives a p95 close to 190."""
    cache = QueryCache(max_size=10, latency_window=500)
    for i in range(1, 201):
        cache.record_latency(float(i))
    p95 = cache.p95_latency_ms()
    # ``int(200 * 0.95) - 1 == 189`` so the 190th value (index 189)
    # is returned. Allow a little wiggle room in case the threshold
    # is computed slightly differently in a future refactor.
    assert 185.0 <= p95 <= 195.0


def test_p95_respects_bounded_latency_window() -> None:
    """When the window is full the oldest samples slide off."""
    cache = QueryCache(max_size=10, latency_window=5)
    # Feed a lot more than the window size — only the last 5 survive.
    for v in [100.0, 200.0, 300.0, 400.0, 500.0, 1.0, 2.0, 3.0, 4.0, 5.0]:
        cache.record_latency(v)
    # p95 should now be computed over [1, 2, 3, 4, 5], not the big
    # initial values.
    p95 = cache.p95_latency_ms()
    assert p95 <= 5.0


# ---------------------------------------------------------------------------
# Hit ratio accuracy
# ---------------------------------------------------------------------------

def test_hit_ratio_after_mixed_hits_and_misses() -> None:
    """``hit_ratio`` equals hits / (hits + misses) regardless of order."""
    cache = QueryCache(max_size=10)
    cache.put("a", 1)
    # 3 hits and 2 misses.
    cache.get("a")  # hit
    cache.get("a")  # hit
    cache.get("b")  # miss
    cache.get("a")  # hit
    cache.get("c")  # miss
    assert cache.hits == 3
    assert cache.misses == 2
    assert cache.hit_ratio == 0.6

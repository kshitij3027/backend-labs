"""Unit tests for the thread-safe L1 TTL cache.

TTL behaviour is exercised with an *injected* fake clock (``FakeClock``) rather
than real time: ``cachetools.TTLCache`` evaluates expiry against whatever
``timer()`` returns, so advancing the fake clock deterministically expires
entries with no ``time.sleep`` and no flakiness.
"""
from __future__ import annotations

import threading

from src.l1_cache import L1Cache


class FakeClock:
    """A manually-advanced monotonic clock for deterministic TTL tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# -- basic hit / miss accounting ----------------------------------------


def test_set_then_get_returns_value_and_counts_hit() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    cache.set("a", 123)

    assert cache.get("a") == 123
    assert cache.hits == 1
    assert cache.misses == 0


def test_get_missing_returns_none_and_counts_miss() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())

    assert cache.get("nope") is None
    assert cache.misses == 1
    assert cache.hits == 0


def test_delete_reports_existence_and_removes() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    cache.set("a", 1)

    assert cache.delete("a") is True
    assert cache.delete("a") is False
    assert cache.get("a") is None


def test_clear_empties_and_resets_counters() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    cache.set("a", 1)
    cache.get("a")
    cache.get("missing")

    cache.clear()

    assert len(cache) == 0
    assert cache.hits == 0
    assert cache.misses == 0


def test_contains_and_len() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    cache.set("a", 1)
    cache.set("b", 2)

    assert "a" in cache
    assert "z" not in cache
    assert len(cache) == 2


# -- eviction ------------------------------------------------------------


def test_eviction_respects_max_size() -> None:
    cache = L1Cache(max_size=2, ttl=100, timer=FakeClock())
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)  # exceeds maxsize -> one entry must be evicted

    assert len(cache) <= 2


# -- TTL expiry via injected fake clock (no real sleep) -----------------


def test_ttl_expiry_via_fake_clock() -> None:
    clock = FakeClock(start=0.0)
    cache = L1Cache(max_size=10, ttl=10, timer=clock)
    cache.set("k", "v")

    # Still fresh.
    assert cache.get("k") == "v"
    assert cache.hits == 1

    # Advance the clock past the TTL window; the entry must now be expired.
    clock.advance(11)
    assert cache.get("k") is None
    assert cache.misses == 1  # the expired read counts as a miss


# -- thread safety -------------------------------------------------------


def test_thread_safe_concurrent_set_get() -> None:
    # Large maxsize so eviction churn doesn't dominate; still asserted bounded.
    max_size = 100_000
    cache = L1Cache(max_size=max_size, ttl=1000, timer=FakeClock())

    num_threads = 8
    ops_per_thread = 1000
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            for i in range(ops_per_thread):
                # Overlapping key space across threads to force contention.
                key = f"key:{i % 50}"
                cache.set(key, worker_id * 1000 + i)
                cache.get(key)
        except BaseException as exc:  # noqa: BLE001 - record, assert later
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(t,)) for t in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []  # no corruption / exceptions under concurrency

    # Each thread did ops_per_thread sets + ops_per_thread gets.
    total_ops = num_threads * ops_per_thread
    assert cache.hits + cache.misses == total_ops
    assert len(cache) <= max_size


# -- pattern deletion ----------------------------------------------------


def test_scan_delete_removes_only_matching_keys() -> None:
    cache = L1Cache(max_size=50, ttl=100, timer=FakeClock())
    cache.set("user:1", 1)
    cache.set("user:2", 2)
    cache.set("order:1", 3)

    removed = cache.scan_delete("user:*")

    assert removed == 2
    assert "user:1" not in cache
    assert "user:2" not in cache
    assert "order:1" in cache


# -- near-expiry detection ----------------------------------------------


def test_near_expiry_keys_returns_aging_entries() -> None:
    clock = FakeClock(start=0.0)
    cache = L1Cache(max_size=50, ttl=100, timer=clock)
    cache.set("a", 1)
    cache.set("b", 2)

    # fraction=0.2 -> qualifies once age >= (1 - 0.2) * 100 = 80.
    assert cache.near_expiry_keys(0.2) == []

    clock.advance(85)  # age now 85 >= 80 for both keys (not yet expired)
    near = set(cache.near_expiry_keys(0.2))

    assert near == {"a", "b"}


def test_near_expiry_keys_skips_keys_without_set_time() -> None:
    clock = FakeClock(start=0.0)
    cache = L1Cache(max_size=50, ttl=100, timer=clock)
    cache.set("a", 1)

    # Simulate a key present in the cache but missing from the side table.
    cache._cache["ghost"] = 99  # type: ignore[index]

    clock.advance(90)
    near = cache.near_expiry_keys(0.2)

    assert "a" in near
    assert "ghost" not in near


# -- approx bytes & stats -----------------------------------------------


def test_approx_bytes_grows_with_entries() -> None:
    cache = L1Cache(max_size=50, ttl=100, timer=FakeClock())
    assert cache.approx_bytes() == 0

    cache.set("a", {"hello": "world", "n": 42})
    assert cache.approx_bytes() > 0


def test_approx_bytes_handles_non_json_values() -> None:
    cache = L1Cache(max_size=50, ttl=100, timer=FakeClock())
    # ``default=str`` keeps this from raising on a non-serializable object.
    cache.set("obj", object())

    assert cache.approx_bytes() > 0


def test_stats_keys_present_and_hit_rate_correct() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    cache.set("a", 1)
    cache.set("b", 2)

    # 3 hits + 1 miss -> hit_rate 0.75
    cache.get("a")
    cache.get("a")
    cache.get("b")
    cache.get("missing")

    stats = cache.stats()

    expected_keys = {
        "hits",
        "misses",
        "total",
        "hit_rate",
        "entries",
        "max_size",
        "approx_bytes",
        "approx_mb",
    }
    assert set(stats.keys()) == expected_keys

    assert stats["hits"] == 3
    assert stats["misses"] == 1
    assert stats["total"] == 4
    assert stats["hit_rate"] == 0.75
    assert stats["entries"] == 2
    assert stats["max_size"] == 10
    assert stats["approx_bytes"] > 0
    assert stats["approx_mb"] == stats["approx_bytes"] / (1024 * 1024)


def test_stats_hit_rate_zero_when_no_requests() -> None:
    cache = L1Cache(max_size=10, ttl=100, timer=FakeClock())
    assert cache.stats()["hit_rate"] == 0.0

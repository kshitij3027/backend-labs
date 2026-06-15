"""Unit tests for :mod:`app.reconstruct` — the bounded LRU + deterministic gzip.

Two units under test, pinned hard:

* :class:`~app.reconstruct.ReconstructionCache` — a thread-safe bounded LRU mapping
  entry index → reconstructed entry. These tests prove the four contracts the API
  relies on for transparency and the latency gate:
    - **compute-once**: ``get_or_compute(i, fn)`` calls ``fn`` exactly once across a
      miss-then-hit pair, returning the right value both times (a counter closure
      proves it);
    - **LRU eviction**: at ``maxsize=3``, touching a key promotes it, and the
      least-recently-used key is the one evicted on overflow;
    - **deep-copy isolation in both directions**: a returned entry is a deep copy
      (mutating it can't corrupt a later read), and the stored slot is isolated from
      the object ``compute`` returned (mutating that original after storage can't
      change cached reads);
    - **disabled mode** (``maxsize<=0``): always computes, stores nothing, counts every
      lookup as a miss, ``enabled`` is False;
    - **exception path**: a raising ``compute`` propagates and stores nothing;
    - **clear vs reset_stats**: ``clear`` empties entries but keeps counters;
      ``reset_stats`` zeroes counters; together both;
    - a small **thread-safety** smoke (no exception, ``hits+misses == lookups``).

* :func:`~app.reconstruct.gzip_bytes` / :func:`~app.reconstruct.gunzip_bytes` — the
  byte-reproducible gzip round-trip (``mtime=0``): inverse for every input incl. empty
  and large, and byte-identical output across calls (determinism).
"""
from __future__ import annotations

import threading

import pytest

from app.reconstruct import ReconstructionCache, gunzip_bytes, gzip_bytes


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
class _Counter:
    """A compute factory that records how many times each index was computed.

    ``fn(index)`` returns a zero-arg callable suitable for
    ``get_or_compute(index, fn(index))``; each invocation bumps ``calls[index]`` and
    returns a *fresh* dict ``{"index": index, "payload": [...]}`` so callers can mutate
    the result without disturbing a future recompute.
    """

    def __init__(self) -> None:
        self.calls: dict[int, int] = {}

    def fn(self, index: int):
        def compute():
            self.calls[index] = self.calls.get(index, 0) + 1
            return {"index": index, "payload": [index, index + 1, index + 2]}

        return compute

    def total(self) -> int:
        return sum(self.calls.values())


# --------------------------------------------------------------------------- #
# Hit / miss & compute-once.
# --------------------------------------------------------------------------- #
def test_miss_then_hit_computes_once_and_returns_correct_value():
    """First lookup is a miss (compute called); second is a hit (compute NOT called)."""
    cache = ReconstructionCache(maxsize=8)
    ctr = _Counter()

    first = cache.get_or_compute(5, ctr.fn(5))
    assert first == {"index": 5, "payload": [5, 6, 7]}
    assert ctr.calls[5] == 1  # computed on the miss

    second = cache.get_or_compute(5, ctr.fn(5))
    assert second == {"index": 5, "payload": [5, 6, 7]}  # same value
    assert ctr.calls[5] == 1  # NOT recomputed on the hit

    st = cache.stats()
    assert st["hits"] == 1
    assert st["misses"] == 1
    assert st["size"] == 1
    assert st["maxsize"] == 8
    assert st["enabled"] is True
    assert st["hit_rate"] == 0.5


def test_stats_hit_rate_rounding_and_zero_lookups():
    """``hit_rate`` is round(hits/total,4); 0.0 before any lookup."""
    cache = ReconstructionCache(maxsize=4)
    # No lookups yet.
    st0 = cache.stats()
    assert st0 == {
        "size": 0,
        "maxsize": 4,
        "hits": 0,
        "misses": 0,
        "hit_rate": 0.0,
        "enabled": True,
    }

    ctr = _Counter()
    # 1 miss + 2 hits on index 1 => hit_rate = 2/3 = 0.6667.
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(1, ctr.fn(1))
    st = cache.stats()
    assert st["hits"] == 2
    assert st["misses"] == 1
    assert st["hit_rate"] == round(2 / 3, 4) == 0.6667


# --------------------------------------------------------------------------- #
# LRU eviction.
# --------------------------------------------------------------------------- #
def test_lru_evicts_least_recently_used():
    """maxsize=3: insert 1,2,3; touch 1; insert 4 -> evicts 2; 1/3/4 still cached."""
    cache = ReconstructionCache(maxsize=3)
    ctr = _Counter()

    # Fill: misses for 1, 2, 3.
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(2, ctr.fn(2))
    cache.get_or_compute(3, ctr.fn(3))
    assert ctr.calls == {1: 1, 2: 1, 3: 1}
    assert cache.stats()["size"] == 3

    # Access 1 -> now most-recently-used; LRU order is now [2, 3, 1].
    cache.get_or_compute(1, ctr.fn(1))
    assert ctr.calls[1] == 1  # served from cache, not recomputed

    # Insert 4 (miss) -> overflow evicts the LRU == 2.
    cache.get_or_compute(4, ctr.fn(4))
    assert ctr.calls[4] == 1
    assert cache.stats()["size"] == 3

    # 1, 3, 4 should all HIT now (no recompute); 2 should MISS (was evicted).
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(3, ctr.fn(3))
    cache.get_or_compute(4, ctr.fn(4))
    assert ctr.calls[1] == 1, "index 1 was evicted but should have survived"
    assert ctr.calls[3] == 1, "index 3 was evicted but should have survived"
    assert ctr.calls[4] == 1, "index 4 was evicted but should have survived"

    # Re-requesting 2 recomputes (it was the evicted LRU victim).
    cache.get_or_compute(2, ctr.fn(2))
    assert ctr.calls[2] == 2, "index 2 should have been evicted and recomputed"


def test_repeated_access_promotes_to_mru_protecting_from_eviction():
    """A hot key accessed every step survives a stream of cold inserts (maxsize=2)."""
    cache = ReconstructionCache(maxsize=2)
    ctr = _Counter()

    cache.get_or_compute(0, ctr.fn(0))  # hot key seeded
    for cold in range(1, 6):
        cache.get_or_compute(0, ctr.fn(0))  # touch hot (hit, promotes it)
        cache.get_or_compute(cold, ctr.fn(cold))  # cold insert (may evict the other)

    # Hot key 0 was computed exactly once despite 5 cold inserts past maxsize=2.
    assert ctr.calls[0] == 1


# --------------------------------------------------------------------------- #
# Deep-copy isolation — both directions.
# --------------------------------------------------------------------------- #
def test_returned_entry_is_deep_copy_mutation_does_not_affect_cache():
    """Mutating a hit's returned entry must not change a subsequent get_or_compute."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()

    a = cache.get_or_compute(2, ctr.fn(2))  # miss, computes
    # Corrupt the returned object (top-level and nested).
    a["index"] = -999
    a["payload"].append("POISON")
    a["new_key"] = True

    b = cache.get_or_compute(2, ctr.fn(2))  # hit
    assert ctr.calls[2] == 1  # still served from cache
    assert b == {"index": 2, "payload": [2, 3, 4]}, "cached entry leaked a mutation"
    assert "new_key" not in b
    assert "POISON" not in b["payload"]


def test_stored_entry_isolated_from_compute_return_value():
    """Mutating the object compute() returned after storage must not change cached reads."""
    cache = ReconstructionCache(maxsize=4)

    produced: list[dict] = []

    def compute():
        obj = {"index": 9, "payload": [9, 9, 9]}
        produced.append(obj)  # keep a handle on the exact object compute returned
        return obj

    first = cache.get_or_compute(9, compute)  # miss: stores a deep copy of `obj`
    assert first == {"index": 9, "payload": [9, 9, 9]}

    # Mutate the *original* object compute returned, AFTER it was stored.
    original = produced[0]
    original["index"] = 123
    original["payload"].append("POISON")
    original["extra"] = "x"

    # A subsequent hit must reflect the value at store time, not the later mutation.
    def boom():  # must not be called (this is a hit)
        raise AssertionError("compute should not run on a hit")

    second = cache.get_or_compute(9, boom)
    assert second == {"index": 9, "payload": [9, 9, 9]}, (
        "cache stored a reference to compute()'s object instead of a deep copy"
    )


def test_two_hits_return_independent_copies():
    """Two separate hits return distinct objects (mutating one can't affect the other)."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()
    cache.get_or_compute(3, ctr.fn(3))  # populate

    h1 = cache.get_or_compute(3, ctr.fn(3))
    h2 = cache.get_or_compute(3, ctr.fn(3))
    assert h1 is not h2
    assert h1["payload"] is not h2["payload"]
    h1["payload"].append("X")
    assert "X" not in h2["payload"]
    assert ctr.calls[3] == 1  # both were hits


# --------------------------------------------------------------------------- #
# Disabled mode (maxsize <= 0).
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("maxsize", [0, -1, -100])
def test_disabled_mode_always_computes_and_counts_misses(maxsize):
    """maxsize<=0: enabled False, always computes, size stays 0, every lookup a miss."""
    cache = ReconstructionCache(maxsize=maxsize)
    ctr = _Counter()

    assert cache.stats()["enabled"] is False

    # Five lookups on the SAME index -> five computes (no caching).
    for _ in range(5):
        out = cache.get_or_compute(7, ctr.fn(7))
        assert out == {"index": 7, "payload": [7, 8, 9]}
    assert ctr.calls[7] == 5  # recomputed every time

    st = cache.stats()
    assert st["enabled"] is False
    assert st["size"] == 0
    assert st["hits"] == 0
    assert st["misses"] == 5
    assert st["maxsize"] == maxsize
    assert st["hit_rate"] == 0.0


# --------------------------------------------------------------------------- #
# Exception path.
# --------------------------------------------------------------------------- #
def test_compute_exception_propagates_and_stores_nothing():
    """A raising compute propagates; nothing is cached, so the next lookup recomputes."""
    cache = ReconstructionCache(maxsize=4)

    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise IndexError("out of range")

    with pytest.raises(IndexError):
        cache.get_or_compute(42, boom)
    assert calls["n"] == 1

    # Nothing was stored for index 42: size unchanged, miss counted, key absent.
    st = cache.stats()
    assert st["size"] == 0
    assert st["misses"] == 1
    assert st["hits"] == 0

    # A second lookup must call compute again (proves nothing was cached for 42).
    with pytest.raises(IndexError):
        cache.get_or_compute(42, boom)
    assert calls["n"] == 2
    assert cache.stats()["misses"] == 2


def test_compute_exception_does_not_evict_existing_entries():
    """A failing compute for a new key leaves previously-cached good entries intact."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()
    cache.get_or_compute(1, ctr.fn(1))  # cache a good entry

    with pytest.raises(IndexError):
        cache.get_or_compute(2, lambda: (_ for _ in ()).throw(IndexError("boom")))

    # Index 1 still served from cache (not recomputed, not evicted).
    cache.get_or_compute(1, ctr.fn(1))
    assert ctr.calls[1] == 1
    assert cache.stats()["size"] == 1


# --------------------------------------------------------------------------- #
# clear / reset_stats.
# --------------------------------------------------------------------------- #
def test_clear_empties_entries_but_keeps_counters():
    """clear() drops cached entries (next lookup recomputes) but preserves hits/misses."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()
    cache.get_or_compute(1, ctr.fn(1))  # miss
    cache.get_or_compute(1, ctr.fn(1))  # hit
    assert cache.stats()["size"] == 1

    cache.clear()

    st = cache.stats()
    assert st["size"] == 0  # entries gone
    assert st["hits"] == 1  # counters preserved
    assert st["misses"] == 1

    # The cleared entry is gone -> a fresh lookup recomputes.
    cache.get_or_compute(1, ctr.fn(1))
    assert ctr.calls[1] == 2


def test_reset_stats_zeroes_counters_but_keeps_entries():
    """reset_stats() zeroes hits/misses but leaves cached entries in place."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()
    cache.get_or_compute(1, ctr.fn(1))  # miss
    cache.get_or_compute(1, ctr.fn(1))  # hit
    assert cache.stats()["size"] == 1

    cache.reset_stats()

    st = cache.stats()
    assert st["hits"] == 0
    assert st["misses"] == 0
    assert st["size"] == 1  # entry still cached

    # Entry still present -> next lookup is a hit (no recompute), and re-counts.
    cache.get_or_compute(1, ctr.fn(1))
    assert ctr.calls[1] == 1
    assert cache.stats()["hits"] == 1


def test_clear_then_reset_stats_empties_and_zeroes():
    """clear() + reset_stats() leaves an empty cache with zeroed counters."""
    cache = ReconstructionCache(maxsize=4)
    ctr = _Counter()
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(1, ctr.fn(1))
    cache.get_or_compute(2, ctr.fn(2))

    cache.clear()
    cache.reset_stats()

    st = cache.stats()
    assert st["size"] == 0
    assert st["hits"] == 0
    assert st["misses"] == 0
    assert st["hit_rate"] == 0.0


# --------------------------------------------------------------------------- #
# Thread-safety smoke (light).
# --------------------------------------------------------------------------- #
def test_threaded_get_or_compute_is_consistent():
    """Many threads hammering a small index range (maxsize < range): no crash,
    and hits + misses == total lookups (internal counters stay consistent)."""
    cache = ReconstructionCache(maxsize=4)  # smaller than the index range below
    index_range = 16
    threads_n = 8
    per_thread = 500
    total_lookups = threads_n * per_thread

    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            rng_state = worker_id
            for i in range(per_thread):
                # Cheap deterministic-ish index spread without importing random.
                rng_state = (rng_state * 1103515245 + 12345) & 0x7FFFFFFF
                idx = rng_state % index_range

                def compute(value=idx):
                    return {"index": value}

                out = cache.get_or_compute(idx, compute)
                assert out == {"index": idx}
        except BaseException as exc:  # noqa: BLE001 — capture for the assert
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"threaded get_or_compute raised: {errors!r}"

    st = cache.stats()
    assert st["hits"] + st["misses"] == total_lookups, (
        f"counter drift: hits={st['hits']} misses={st['misses']} "
        f"!= {total_lookups}"
    )
    assert 0 <= st["size"] <= 4  # never exceeds maxsize
    assert st["enabled"] is True


# --------------------------------------------------------------------------- #
# Deterministic gzip helpers.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"a",
        b"hello world",
        b"\x00\x01\x02\x03\xff\xfe",
        bytes(range(256)),
        b"the quick brown fox " * 1000,  # large, highly repetitive
        b"x" * 100_000,  # large
    ],
)
def test_gzip_round_trip(data):
    """gunzip_bytes(gzip_bytes(x)) == x for every input incl. empty and large."""
    assert gunzip_bytes(gzip_bytes(data)) == data


def test_gzip_is_byte_deterministic():
    """gzip_bytes(x) is byte-identical across calls (mtime=0, no embedded clock)."""
    for data in (b"", b"determinism", b"y" * 50_000, bytes(range(256))):
        a = gzip_bytes(data)
        b = gzip_bytes(data)
        assert a == b, "gzip_bytes is not deterministic (mtime not pinned to 0?)"


def test_gzip_actually_compresses_redundant_data():
    """A highly redundant payload gzips to strictly fewer bytes than the original.

    Sanity that ``gzip_bytes`` is real gzip (not identity), without coupling to an
    exact compressed size.
    """
    data = b"compress me please " * 5000
    blob = gzip_bytes(data)
    assert len(blob) < len(data)
    assert gunzip_bytes(blob) == data

"""Unit tests for :mod:`src.cache` (Commit 16) + the ``LogClassifier`` cache wiring.

Two surfaces are exercised, neither of which needs a running server:

* :class:`src.cache.PredictionCache` — the thread-safe, bounded LRU cache in
  isolation: miss → put → hit, copy-in/copy-out isolation, LRU eviction at
  ``maxsize``, the :meth:`stats` shape/values, :meth:`clear`, the disabled
  (``maxsize <= 0``) mode, and concurrent get/put under a thread pool.
* :class:`src.ensemble.LogClassifier` — that ``classify`` actually memoizes on the
  *normalized* pattern (a repeat is a hit returning an identical dict; a distinct
  message misses), surfaced via :meth:`cache_stats`.

The ``LogClassifier`` test trains a deliberately tiny dual ensemble
(``rf_n_estimators=4``, ``gb_n_estimators=4`` over 60 deterministic records) so the
fit stays fast — correctness of the *labels* is covered elsewhere; here we only
assert caching behaviour (hits/misses + identical outputs).
"""

from __future__ import annotations

import threading

import pytest

from src.cache import PredictionCache
from src.config import Settings
from src.ensemble import LogClassifier
from src.log_generator import generate_logs


# --------------------------------------------------------------------------- #
# PredictionCache — core get/put/hit/miss
# --------------------------------------------------------------------------- #


def test_miss_then_put_then_hit() -> None:
    """A cold ``get`` misses; after ``put`` the same key hits and returns the value."""
    cache = PredictionCache(maxsize=8)

    assert cache.get("k") is None
    assert cache.stats()["misses"] == 1
    assert cache.stats()["hits"] == 0

    value = {"severity": "ERROR", "category": "SYSTEM", "confidence": 0.9}
    cache.put("k", value)

    got = cache.get("k")
    assert got == value
    s = cache.stats()
    assert s["hits"] == 1
    assert s["misses"] == 1


def test_copy_out_semantics_mutation_does_not_corrupt() -> None:
    """Mutating the dict returned by ``get`` must not change the cached entry."""
    cache = PredictionCache(maxsize=8)
    cache.put("k", {"severity": "INFO", "confidence": 0.5})

    first = cache.get("k")
    first["severity"] = "MUTATED"
    first["new_key"] = "injected"

    # A fresh get returns the original, unmutated value.
    second = cache.get("k")
    assert second == {"severity": "INFO", "confidence": 0.5}
    assert "new_key" not in second


def test_copy_in_semantics_caller_mutation_does_not_corrupt() -> None:
    """Mutating the dict *passed to* ``put`` after putting must not change the cache."""
    cache = PredictionCache(maxsize=8)
    source = {"severity": "WARN", "confidence": 0.7}
    cache.put("k", source)

    # Mutate the caller's dict *after* the put — the cache stored a copy.
    source["severity"] = "MUTATED"
    source["confidence"] = 0.0

    got = cache.get("k")
    assert got == {"severity": "WARN", "confidence": 0.7}


# --------------------------------------------------------------------------- #
# PredictionCache — LRU eviction
# --------------------------------------------------------------------------- #


def test_lru_eviction_at_maxsize() -> None:
    """At capacity, the least-recently-*used* key is evicted on the next insert.

    Put k1, k2 (cap 2); ``get(k1)`` promotes k1 to MRU; putting k3 must evict k2
    (the LRU), leaving k1 and k3 resident.
    """
    cache = PredictionCache(maxsize=2)
    cache.put("k1", {"v": 1})
    cache.put("k2", {"v": 2})

    # Touch k1 so k2 becomes the least-recently-used.
    assert cache.get("k1") == {"v": 1}

    # Inserting a third key evicts k2.
    cache.put("k3", {"v": 3})

    assert cache.get("k2") is None, "k2 (LRU) should have been evicted"
    assert cache.get("k1") == {"v": 1}
    assert cache.get("k3") == {"v": 3}
    assert cache.stats()["size"] == 2


# --------------------------------------------------------------------------- #
# PredictionCache — stats / clear
# --------------------------------------------------------------------------- #


def test_stats_shape_and_values() -> None:
    """``stats()`` exposes the five keys with consistent hit_rate / size / capacity."""
    cache = PredictionCache(maxsize=4)
    cache.put("a", {"v": 1})
    cache.put("b", {"v": 2})

    cache.get("a")        # hit
    cache.get("a")        # hit
    cache.get("missing")  # miss

    s = cache.stats()
    assert set(s) == {"hits", "misses", "hit_rate", "size", "capacity"}
    assert s["hits"] == 2
    assert s["misses"] == 1
    assert s["hit_rate"] == pytest.approx(2 / 3)
    assert s["size"] == 2          # two distinct entries held
    assert s["capacity"] == 4      # configured maxsize


def test_stats_hit_rate_zero_when_no_lookups() -> None:
    """With no get() calls, hit_rate is 0.0 (no division by zero)."""
    cache = PredictionCache(maxsize=4)
    assert cache.stats()["hit_rate"] == 0.0


def test_clear_empties_and_resets_counters() -> None:
    """``clear()`` drops entries and zeroes hits/misses, leaving the cache usable."""
    cache = PredictionCache(maxsize=4)
    cache.put("a", {"v": 1})
    cache.get("a")
    cache.get("nope")

    cache.clear()

    s = cache.stats()
    assert s == {"hits": 0, "misses": 0, "hit_rate": 0.0, "size": 0, "capacity": 4}
    # Still usable after clear.
    assert cache.get("a") is None
    cache.put("a", {"v": 9})
    assert cache.get("a") == {"v": 9}


# --------------------------------------------------------------------------- #
# PredictionCache — disabled mode
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("maxsize", [0, -1, -100])
def test_disabled_cache_always_misses(maxsize: int) -> None:
    """``maxsize <= 0`` disables the cache: get always misses, put is a no-op."""
    cache = PredictionCache(maxsize=maxsize)

    cache.put("k", {"v": 1})           # no-op
    assert cache.get("k") is None      # always a miss
    assert cache.get("k") is None

    s = cache.stats()
    assert s["size"] == 0
    assert s["hits"] == 0
    assert s["misses"] == 2
    assert s["capacity"] == maxsize


# --------------------------------------------------------------------------- #
# PredictionCache — thread safety
# --------------------------------------------------------------------------- #


def test_thread_safety_concurrent_get_put() -> None:
    """Many threads hammering get/put raise no exception; hits+misses == total gets.

    Each of ``T`` threads does ``N`` get/put pairs over a small shared key space.
    Every iteration issues exactly one ``get`` (always counted as a hit or a miss),
    so the invariant ``hits + misses == T * N`` must hold after the join regardless
    of interleaving — proving the counters are updated under the lock without loss.
    """
    cache = PredictionCache(maxsize=64)
    n_threads = 8
    iters = 500
    errors: list[BaseException] = []
    keys = [f"key-{i}" for i in range(16)]

    def worker(tid: int) -> None:
        try:
            for n in range(iters):
                key = keys[(tid + n) % len(keys)]
                got = cache.get(key)           # exactly one get per iteration
                if got is None:
                    cache.put(key, {"tid": tid, "n": n})
        except BaseException as exc:  # noqa: BLE001 - capture to fail in main thread
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent access raised: {errors[:3]}"

    s = cache.stats()
    assert s["hits"] + s["misses"] == n_threads * iters
    # Bounded: never exceeds capacity despite the contention.
    assert s["size"] <= 64


# --------------------------------------------------------------------------- #
# LogClassifier — end-to-end cache wiring
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def tiny_clf() -> LogClassifier:
    """A ``LogClassifier`` fitted once on a tiny, fast dual ensemble.

    ``rf_n_estimators=4`` / ``gb_n_estimators=4`` over 60 deterministic records keeps
    the NB+RF+GB fit quick; label correctness is covered in ``test_ensemble`` — here
    we only assert caching behaviour, so a small model is sufficient.
    """
    cfg = Settings(rf_n_estimators=4, gb_n_estimators=4)
    return LogClassifier(cfg).fit(generate_logs(60, 42))


def test_classifier_caches_repeated_classify(tiny_clf: LogClassifier) -> None:
    """A repeated ``classify`` is a cache hit returning an identical result.

    Classifying the same message twice records at least one hit and the two result
    dicts are byte-for-byte equal (caching changes latency, never output). A *new*
    distinct message then increases the miss count.
    """
    tiny_clf._cache.clear()  # isolate this test's counters from the shared fixture

    first = tiny_clf.classify("Database connection failed with timeout error")
    second = tiny_clf.classify("Database connection failed with timeout error")

    assert first == second, "cached result must equal the freshly computed one"
    stats_after_repeat = tiny_clf.cache_stats()
    assert stats_after_repeat["hits"] >= 1, f"expected a cache hit: {stats_after_repeat}"

    misses_before = tiny_clf.cache_stats()["misses"]
    tiny_clf.classify("A completely different unrelated payment gateway message")
    misses_after = tiny_clf.cache_stats()["misses"]
    assert misses_after > misses_before, "a new distinct message must miss the cache"


def test_classifier_returns_isolated_copy_per_call(tiny_clf: LogClassifier) -> None:
    """Mutating one ``classify`` result must not corrupt a later cache hit.

    Because the cache hands back a copy, scribbling on the returned dict cannot
    leak into the next hit for the same message.
    """
    tiny_clf._cache.clear()
    log = "User authentication succeeded for session token"

    out1 = tiny_clf.classify(log)
    out1["severity"] = "TAMPERED"

    out2 = tiny_clf.classify(log)  # served from cache
    assert out2["severity"] != "TAMPERED", "cache must return an isolated copy"

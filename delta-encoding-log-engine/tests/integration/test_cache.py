"""Integration tests for the reconstruction cache as seen through the REST API.

Commit 10 wired :class:`~app.reconstruct.ReconstructionCache` into the app:
``GET /api/logs/{index}`` serves single-entry reconstructions through it, ``/api/compress``
and ``/api/reset`` clear it, and ``/api/stats`` surfaces its occupancy + hit-rate under
``performance.cache``. These tests drive the *real* wired app via ``TestClient`` and assert
the user-visible contract:

* **Transparency**: two reads of the same index return identical entries, and the second
  read registers a cache HIT in ``performance.cache.hits`` — while the entry still equals
  the generated one (the cache never changes the answer).
* **Staleness safety**: caching ``/api/logs/5`` from batch A, then generating + compressing
  a *different* batch B, must make ``/api/logs/5`` return B's entry — the cache was cleared
  on compress, so it can't serve a stale A entry.
* **Stats shape + reset**: ``performance.cache`` has the expected keys, and after
  ``/api/reset`` its ``size`` is 0 and hits/misses are zeroed.
* **Latency gate**: after reconstructing many single indices (cache HITs included),
  ``performance.reconstruct_p99_ms`` stays under the 100ms plan gate.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.codec import entries_equal
from app.main import app

_CACHE_KEYS = {"size", "maxsize", "hits", "misses", "hit_rate", "enabled"}


@pytest.fixture
def client():
    """TestClient with lifespan active; reset engine state before each test.

    Mirrors the integration fixture in ``test_api.py`` — ``POST /api/reset`` zeroes the
    store, the metrics registry, AND the reconstruction cache (entries + counters), so
    tests sharing the singleton ``app`` don't leak cached entries or hit/miss counts.
    """
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


def _generate(client, count, seed):
    """POST /api/generate; assert 200 + count; return the generated logs."""
    resp = client.post("/api/generate", json={"count": count, "seed": seed})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == count
    return body["logs"]


def _cache_stats(client) -> dict:
    """Return the ``performance.cache`` block from /api/stats."""
    resp = client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    perf = resp.json()["performance"]
    assert "cache" in perf, f"performance missing 'cache': {perf.keys()}"
    return perf["cache"]


# --------------------------------------------------------------------------- #
# Transparency: repeated reads are identical, second is a HIT.
# --------------------------------------------------------------------------- #
def test_repeated_logs_index_is_a_cache_hit_and_identical(client):
    """GET /api/logs/{i} twice: identical entries; cache.hits rises on the 2nd call."""
    generated = _generate(client, count=300, seed=7)
    cresp = client.post("/api/compress", json={"use_generated": True})
    assert cresp.status_code == 200, cresp.text

    idx = 150

    # First read: a cache miss (computes + caches).
    r1 = client.get(f"/api/logs/{idx}")
    assert r1.status_code == 200, r1.text
    entry1 = r1.json()["entry"]
    hits_after_first = _cache_stats(client)["hits"]

    # Second read: should be a cache HIT and byte-identical.
    r2 = client.get(f"/api/logs/{idx}")
    assert r2.status_code == 200, r2.text
    entry2 = r2.json()["entry"]
    hits_after_second = _cache_stats(client)["hits"]

    assert entry1 == entry2, "repeated reads of the same index differ"
    # The entry equals the generated one (cache changed nothing).
    assert entries_equal(entry2, generated[idx])
    # The second read registered a cache hit.
    assert hits_after_second > hits_after_first, (
        "second read of the same index did not register a cache hit "
        f"(hits {hits_after_first} -> {hits_after_second})"
    )


def test_cache_size_grows_with_distinct_indices(client):
    """Reading several distinct indices populates the cache (size grows)."""
    _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    assert _cache_stats(client)["size"] == 0  # fresh after compress-clear
    for idx in (0, 10, 20, 30, 40):
        assert client.get(f"/api/logs/{idx}").status_code == 200
    assert _cache_stats(client)["size"] == 5


# --------------------------------------------------------------------------- #
# Staleness: compress must clear the cache so reads reflect the NEW batch.
# --------------------------------------------------------------------------- #
def test_compress_clears_cache_so_reads_are_not_stale(client):
    """Cache logs/5 from batch A, then compress batch B -> logs/5 returns B, not A."""
    idx = 5

    # Batch A: generate + compress + cache entry 5.
    batch_a = _generate(client, count=200, seed=101)
    client.post("/api/compress", json={"use_generated": True})
    ra = client.get(f"/api/logs/{idx}")
    assert ra.status_code == 200, ra.text
    entry_a = ra.json()["entry"]
    assert entries_equal(entry_a, batch_a[idx])

    # Batch B: a DIFFERENT batch (different seed) -> different entry at index 5.
    batch_b = _generate(client, count=200, seed=202)
    assert not entries_equal(batch_a[idx], batch_b[idx]), (
        "test seeds happened to collide at index 5; pick different seeds"
    )
    client.post("/api/compress", json={"use_generated": True})

    # Reading index 5 now MUST reflect batch B (cache was cleared on compress).
    rb = client.get(f"/api/logs/{idx}")
    assert rb.status_code == 200, rb.text
    entry_after = rb.json()["entry"]
    assert entries_equal(entry_after, batch_b[idx]), (
        "stale cache: /api/logs/5 returned batch A's entry after recompressing batch B"
    )
    assert not entries_equal(entry_after, batch_a[idx])


# --------------------------------------------------------------------------- #
# Stats shape + reset.
# --------------------------------------------------------------------------- #
def test_stats_cache_block_has_expected_keys(client):
    """performance.cache exposes size/maxsize/hits/misses/hit_rate/enabled."""
    _generate(client, count=120, seed=7)
    client.post("/api/compress", json={"use_generated": True})
    client.get("/api/logs/0")
    client.get("/api/logs/0")  # generate at least one hit

    cache = _cache_stats(client)
    assert _CACHE_KEYS.issubset(cache.keys()), (
        f"cache block missing keys: {_CACHE_KEYS - set(cache.keys())}"
    )
    assert isinstance(cache["hits"], int)
    assert isinstance(cache["misses"], int)
    assert isinstance(cache["size"], int)
    assert isinstance(cache["enabled"], bool)
    assert 0.0 <= cache["hit_rate"] <= 1.0


def test_reset_clears_cache_size_and_counters(client):
    """After POST /api/reset, performance.cache.size == 0 and hits/misses == 0."""
    _generate(client, count=200, seed=7)
    client.post("/api/compress", json={"use_generated": True})
    # Populate the cache and rack up some hits/misses.
    for idx in (0, 50, 100, 150):
        client.get(f"/api/logs/{idx}")
        client.get(f"/api/logs/{idx}")  # second read -> a hit each
    pre = _cache_stats(client)
    assert pre["size"] > 0
    assert pre["hits"] > 0

    resp = client.post("/api/reset")
    assert resp.status_code == 200, resp.text

    post = _cache_stats(client)
    assert post["size"] == 0
    assert post["hits"] == 0
    assert post["misses"] == 0
    assert post["hit_rate"] == 0.0


# --------------------------------------------------------------------------- #
# Latency gate: <100ms reconstruction p99 even with the cache in the path.
# --------------------------------------------------------------------------- #
def test_reconstruct_p99_under_100ms_after_many_single_reads(client):
    """After many single-index reads (HITs + misses), reconstruct_p99_ms < 100."""
    _generate(client, count=500, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    # Hammer single-entry reads: a mix of fresh indices (misses) and repeats (hits).
    for _ in range(3):
        for idx in range(0, 500, 5):
            r = client.get(f"/api/logs/{idx}")
            assert r.status_code == 200, r.text

    resp = client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    perf = resp.json()["performance"]
    p99 = perf["reconstruct_p99_ms"]
    assert p99 < 100.0, f"reconstruct_p99_ms {p99} exceeded the 100ms gate"
    # The cache should be doing real work by now (some hits recorded).
    assert perf["cache"]["hits"] > 0

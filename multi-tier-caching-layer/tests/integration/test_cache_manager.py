"""Integration tests for the read-through cache manager (``src.cache_manager``).

These run against the REAL Redis + Postgres wired by the compose ``test``
service, via the ``cache_manager`` fixture (which seeds a deterministic
``raw_logs`` corpus and builds the manager with a 100ms slow-backend delay).

They verify the project's core success criteria for the cache hierarchy:

* a cold miss falls through to the **backend** and populates L1 + L2 + L3;
* an immediate repeat is served (faster) from **L1**;
* after L1 eviction the value comes from **L2** and L1 is re-backfilled;
* after L1 eviction *and* an L2 flush (L3 still present) it comes from **L3**
  and both L1 and L2 are re-backfilled — proving the L1 -> L2 -> L3 ordering;
* concurrent identical cold gets run the backend **exactly once** (single-flight);
* an unknown query propagates :class:`ValueError`;
* metrics and the pattern engine record served requests.
"""
from __future__ import annotations

import asyncio

import pytest

import src.cache_manager as cache_manager_mod
from src import l3_store
from src.keys import cache_key

# A representative aggregation query + filter used across the tier-walk tests.
QUERY = "error_rate"
PARAMS = {"source": "api", "start": 1_779_000_000, "end": 1_781_000_000}


def _key_for(params: dict) -> str:
    """Semantic key for QUERY + ``params`` at the fixture's 300s bucket width."""
    return cache_key(QUERY, params, bucket_seconds=300)


async def test_cold_miss_hits_backend_and_populates_all_tiers(cache_manager):
    """First get is a full miss -> served by backend, all tiers populated."""
    cm = cache_manager
    key = _key_for(PARAMS)

    res = await cm.get(QUERY, PARAMS)

    assert res.tier == "backend"
    assert isinstance(res.result, dict)
    assert res.key == key

    # Every faster tier was populated on the way back up.
    assert cm.l1.get(key) is not None
    assert await cm.l2.get(key) is not None
    assert await l3_store.get(cm.pg_pool, key) is not None


async def test_repeat_query_served_from_l1_and_faster(cache_manager):
    """An immediate identical repeat is an L1 hit and faster than the cold call."""
    cm = cache_manager

    first = await cm.get(QUERY, PARAMS)
    assert first.tier == "backend"

    second = await cm.get(QUERY, PARAMS)
    assert second.tier == "l1"
    assert second.result == first.result
    # The cold call paid the 100ms backend delay; the L1 hit must be faster.
    assert second.elapsed_ms < first.elapsed_ms


async def test_l1_evicted_falls_to_l2_and_rebackfills(cache_manager):
    """After clearing L1, the value is served from L2 and L1 is re-populated."""
    cm = cache_manager
    key = _key_for(PARAMS)

    await cm.get(QUERY, PARAMS)  # cold: populates all tiers
    cm.l1.clear()
    assert cm.l1.get(key) is None

    res = await cm.get(QUERY, PARAMS)
    assert res.tier == "l2"
    # L1 re-backfilled from L2.
    assert cm.l1.get(key) is not None


async def test_l2_flushed_l3_present_falls_to_l3_and_rebackfills(cache_manager):
    """L1 cleared + L2 flushed but L3 present -> L3 hit, backfills L1 and L2."""
    cm = cache_manager
    key = _key_for(PARAMS)

    await cm.get(QUERY, PARAMS)  # cold: populates all tiers (incl. L3)
    cm.l1.clear()
    await cm.l2.raw.flushdb()
    assert cm.l1.get(key) is None
    assert await cm.l2.get(key) is None
    # L3 still holds the materialized result.
    assert await l3_store.get(cm.pg_pool, key) is not None

    res = await cm.get(QUERY, PARAMS)
    assert res.tier == "l3"
    # Both faster tiers re-backfilled from L3.
    assert cm.l1.get(key) is not None
    assert await cm.l2.get(key) is not None


async def test_concurrent_cold_gets_compute_backend_once(cache_manager, monkeypatch):
    """~20 concurrent cold gets for a fresh key compute the backend exactly once."""
    cm = cache_manager

    # A FRESH, uncached key (distinct params -> distinct semantic key).
    fresh_params = {"source": "web", "start": 1_779_500_000, "end": 1_780_500_000}

    calls = {"n": 0}
    real_materialize = cache_manager_mod.materialize

    async def counting_materialize(*args, **kwargs):
        calls["n"] += 1
        return await real_materialize(*args, **kwargs)

    # Patch the name the cache manager resolves (module-level import).
    monkeypatch.setattr(cache_manager_mod, "materialize", counting_materialize)

    # Fire a herd of identical gets; the 100ms backend delay makes them overlap.
    results = await asyncio.gather(
        *[cm.get(QUERY, fresh_params) for _ in range(20)]
    )

    # Single-flight collapsed the herd onto one backend computation.
    assert calls["n"] == 1
    assert len(results) == 20
    first = results[0].result
    assert all(r.result == first for r in results)


async def test_unknown_query_propagates_value_error(cache_manager):
    """An unknown query name propagates ValueError out of get()."""
    cm = cache_manager
    with pytest.raises(ValueError):
        await cm.get("no_such_query", {})


async def test_metrics_and_patterns_recorded(cache_manager):
    """A few gets bump total_requests and yield non-empty recommendations."""
    cm = cache_manager

    await cm.get(QUERY, PARAMS)          # backend
    await cm.get(QUERY, PARAMS)          # l1
    await cm.get("top_sources", {"limit": 3})  # backend (different query)

    assert cm.metrics.total_requests >= 3
    recs = cm.patterns.recommendations()
    assert recs  # non-empty

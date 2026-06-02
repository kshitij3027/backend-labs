"""Integration tests for selective invalidation and incremental time-series.

These exercise the C12 additions to :class:`src.cache_manager.CacheManager`
against the REAL Redis + Postgres wired by the compose ``test`` service, via the
``cache_manager`` fixture (which seeds a deterministic ``raw_logs`` corpus and
builds the manager with a 100ms slow-backend delay).

Coverage:

* **Pattern invalidation** (``q:*``) evicts *every* cached query across L1, L2,
  and L3, and a subsequent ``get`` recomputes (``tier == "backend"``).
* **Tag selectivity** — invalidating ``source:api`` drops only the api-scoped
  key (from all three tiers) while a sibling ``source:web`` key still resolves
  from a fast tier on the next read.
* **Incremental append** adds a point to a cached series and re-stores it across
  all tiers **without** re-running the backend (asserted via a materialize
  call-counter that stays at 0).
* **Append on an absent series** returns ``False`` (nothing to append to).
* **invalidate() with no argument** raises :class:`ValueError`.
"""
from __future__ import annotations

import pytest

import src.cache_manager as cache_manager_mod
from src import l3_store
from src.keys import cache_key, tags_for

# A window that brackets the seeded corpus so every source returns data.
WINDOW = {"start": 1_779_000_000, "end": 1_781_000_000}


def _key(query: str, params: dict) -> str:
    """Semantic key for ``query`` + ``params`` at the fixture's 300s bucket."""
    return cache_key(query, params, bucket_seconds=300)


async def _present_everywhere(cm, key: str) -> tuple[bool, bool, bool]:
    """Return ``(in_l1, in_l2, in_l3)`` presence flags for ``key``."""
    in_l1 = cm.l1.get(key) is not None
    in_l2 = await cm.l2.get(key) is not None
    in_l3 = await l3_store.get(cm.pg_pool, key) is not None
    return in_l1, in_l2, in_l3


# --------------------------------------------------------------------------- #
# Pattern invalidation
# --------------------------------------------------------------------------- #
async def test_pattern_invalidation_clears_all_keys_all_tiers(cache_manager):
    """``invalidate(pattern="q:*")`` purges every cached key from L1+L2+L3."""
    cm = cache_manager

    # Two DIFFERENT semantic keys (different query) -> both populate all tiers.
    err_params = {"source": "api", **WINDOW}
    top_params = {"limit": 3, **WINDOW}
    err_key = _key("error_rate", err_params)
    top_key = _key("top_sources", top_params)

    await cm.get("error_rate", err_params)
    await cm.get("top_sources", top_params)

    # Sanity: both keys live in every tier before invalidation.
    assert all(await _present_everywhere(cm, err_key))
    assert all(await _present_everywhere(cm, top_key))

    # All cache keys are prefixed ``q:`` -> ``q:*`` matches them all.
    counts = await cm.invalidate(pattern="q:*")
    assert counts["l1"] >= 2
    assert counts["l2"] >= 2
    assert counts["l3"] >= 2

    # BOTH keys are gone from every tier.
    assert not any(await _present_everywhere(cm, err_key))
    assert not any(await _present_everywhere(cm, top_key))

    # A re-get after invalidation recomputes via the backend.
    again = await cm.get("error_rate", err_params)
    assert again.tier == "backend"


# --------------------------------------------------------------------------- #
# Tag selectivity
# --------------------------------------------------------------------------- #
async def test_tag_invalidation_is_selective(cache_manager):
    """Invalidating ``source:api`` drops only the api key; the web key survives."""
    cm = cache_manager

    api_params = {"source": "api", **WINDOW}
    web_params = {"source": "web", **WINDOW}
    api_key = _key("error_rate", api_params)
    web_key = _key("error_rate", web_params)

    # Populate two keys tagged source:api and source:web respectively.
    await cm.get("error_rate", api_params)
    await cm.get("error_rate", web_params)

    # The source:api tag set should record exactly the api key (tag_members).
    assert "source:api" in tags_for("error_rate", api_params)
    members = await cm.l2.tag_members("source:api")
    assert api_key in members
    assert web_key not in members

    # Both keys present everywhere before invalidation.
    assert all(await _present_everywhere(cm, api_key))
    assert all(await _present_everywhere(cm, web_key))

    counts = await cm.invalidate(tags=["source:api"])
    assert counts["l1"] >= 1
    assert counts["l3"] >= 1

    # The api key is gone from L1, L2, AND L3.
    assert not any(await _present_everywhere(cm, api_key))

    # The web key still resolves from a fast tier (l1/l2) on the next get.
    res = await cm.get("error_rate", web_params)
    assert res.tier in ("l1", "l2")


# --------------------------------------------------------------------------- #
# Incremental append — no backend recompute
# --------------------------------------------------------------------------- #
async def test_append_timeseries_no_recompute(cache_manager, monkeypatch):
    """append_timeseries appends a point WITHOUT calling the slow backend."""
    cm = cache_manager

    ts_params = {"source": "api", **WINDOW}
    key = _key("requests_over_time", ts_params)

    # Warm the series first (this legitimately runs the backend once).
    first = await cm.get("requests_over_time", ts_params)
    assert first.tier == "backend"
    assert isinstance(first.result, list)

    # Now instrument materialize so any further backend compute is counted.
    calls = {"n": 0}
    real_materialize = cache_manager_mod.materialize

    async def counting_materialize(*args, **kwargs):
        calls["n"] += 1
        return await real_materialize(*args, **kwargs)

    monkeypatch.setattr(cache_manager_mod, "materialize", counting_materialize)

    point = {"bucket": "2099-01-01T00:00:00+00:00", "count": 777}
    appended = await cm.append_timeseries("requests_over_time", ts_params, point)

    assert appended is True
    # The backend was NOT re-run: the counter is still 0.
    assert calls["n"] == 0

    # The appended point is visible on the next read (served from L1, fast).
    res = await cm.get("requests_over_time", ts_params)
    assert res.tier == "l1"
    assert isinstance(res.result, list)
    counts_777 = [p for p in res.result if p.get("count") == 777]
    assert counts_777, "appended point (count=777) should be present in the series"


async def test_append_timeseries_absent_series_returns_false(cache_manager):
    """Appending to a never-cached series returns False (nothing to append to)."""
    cm = cache_manager

    appended = await cm.append_timeseries(
        "requests_over_time",
        {"source": "nope-unseeded", "start": 0, "end": 1},
        {"bucket": "2099-01-01T00:00:00+00:00", "count": 1},
    )
    assert appended is False


# --------------------------------------------------------------------------- #
# Argument validation
# --------------------------------------------------------------------------- #
async def test_invalidate_requires_an_argument(cache_manager):
    """invalidate() with neither pattern nor tags raises ValueError."""
    cm = cache_manager
    with pytest.raises(ValueError):
        await cm.invalidate()

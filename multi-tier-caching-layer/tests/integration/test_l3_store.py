"""Integration tests for the L3 store (``src.l3_store``) and the materializer.

These run against the REAL Postgres wired by the compose ``test`` service via
the ``pg_pool`` fixture (schema applied, ``precomputed_aggregates`` truncated per
test). They exercise the round-trip codec (plain + compressed), upsert
conflict handling, metadata, deletion, pattern invalidation, and the
backend->L3 materialization path.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from src import l3_store
from src.db.seed import seed_raw_logs
from src.materializer import materialize

# Representative payloads: a dict aggregate and a list (time-series) aggregate.
_DICT_RESULT = {"total": 800, "errors": 41, "error_rate": 0.05125}
_LIST_RESULT = [
    {"bucket": "2026-05-01T00:00:00+00:00", "count": 120},
    {"bucket": "2026-05-01T01:00:00+00:00", "count": 130},
    {"bucket": "2026-05-01T02:00:00+00:00", "count": 140},
]


@pytest.mark.parametrize("compress", [False, True])
async def test_upsert_then_get_roundtrips_dict(pg_pool, compress):
    await l3_store.upsert(
        pg_pool, "q:dict", "error_rate", {"source": "api"}, _DICT_RESULT,
        tags=["query:abc", "source:api"], compress=compress,
    )
    got = await l3_store.get(pg_pool, "q:dict")
    assert got == _DICT_RESULT


@pytest.mark.parametrize("compress", [False, True])
async def test_upsert_then_get_roundtrips_list(pg_pool, compress):
    await l3_store.upsert(
        pg_pool, "q:list", "requests_over_time", {}, _LIST_RESULT,
        compress=compress,
    )
    got = await l3_store.get(pg_pool, "q:list")
    assert got == _LIST_RESULT


async def test_get_missing_returns_none(pg_pool):
    assert await l3_store.get(pg_pool, "q:does-not-exist") is None
    assert await l3_store.get_with_meta(pg_pool, "q:does-not-exist") is None


async def test_upsert_conflict_overwrites_and_keeps_single_row(pg_pool):
    await l3_store.upsert(pg_pool, "q:k", "error_rate", {}, {"v": 1})
    await l3_store.upsert(pg_pool, "q:k", "error_rate", {}, {"v": 2})

    # The latest write wins.
    assert await l3_store.get(pg_pool, "q:k") == {"v": 2}

    # Exactly one row exists for that key (ON CONFLICT updated, not inserted).
    async with pg_pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM precomputed_aggregates WHERE key = $1", "q:k"
        )
    assert n == 1


async def test_get_with_meta_returns_tags_and_computed_at(pg_pool):
    await l3_store.upsert(
        pg_pool, "q:meta", "top_sources", {"limit": 5}, [{"source": "api", "count": 9}],
        tags=["query:zzz", "source:api"],
    )
    meta = await l3_store.get_with_meta(pg_pool, "q:meta")

    assert meta is not None
    assert meta["result"] == [{"source": "api", "count": 9}]
    assert isinstance(meta["computed_at"], datetime)
    assert sorted(meta["tags"]) == ["query:zzz", "source:api"]


async def test_delete_removes_row(pg_pool):
    await l3_store.upsert(pg_pool, "q:del", "error_rate", {}, {"v": 1})
    assert await l3_store.get(pg_pool, "q:del") == {"v": 1}

    removed = await l3_store.delete(pg_pool, "q:del")
    assert removed == 1
    assert await l3_store.get(pg_pool, "q:del") is None

    # Deleting again removes nothing.
    assert await l3_store.delete(pg_pool, "q:del") == 0


async def test_invalidate_pattern_deletes_matching_only(pg_pool):
    await l3_store.upsert(pg_pool, "q:alpha", "error_rate", {}, {"v": 1})
    await l3_store.upsert(pg_pool, "q:beta", "error_rate", {}, {"v": 2})
    await l3_store.upsert(pg_pool, "other:gamma", "error_rate", {}, {"v": 3})

    deleted = await l3_store.invalidate_pattern(pg_pool, "q:%")
    assert deleted == 2

    # Matching keys gone, the non-matching one survives.
    assert await l3_store.get(pg_pool, "q:alpha") is None
    assert await l3_store.get(pg_pool, "q:beta") is None
    assert await l3_store.get(pg_pool, "other:gamma") == {"v": 3}


async def test_materialize_populates_l3_and_get_matches(pg_pool):
    # Seed the slow source so the backend has rows to aggregate.
    await seed_raw_logs(pg_pool, 400, seed=7, end_ts=1_780_000_000)

    key, result = await materialize(pg_pool, "error_rate", {"source": "api"}, delay_ms=0)

    # The materialized result is now readable straight from L3 under that key.
    stored = await l3_store.get(pg_pool, key)
    assert stored == result

    # And the metadata carries the source tag derived from params.
    meta = await l3_store.get_with_meta(pg_pool, key)
    assert meta is not None
    assert "source:api" in meta["tags"]

"""Integration tests for the slow log-query backend (``src.backend``).

These run against the REAL Postgres wired by the compose ``test`` service via
the ``pg_pool`` fixture (schema applied, both tables truncated per test). A
fixed, deterministic dataset is seeded first, and expected aggregate values are
recomputed in-process from :func:`generate_rows` so the SQL is checked against
an independent oracle rather than a hand-typed magic number.
"""
from __future__ import annotations

import time
from collections import Counter

import pytest

from src.backend import SUPPORTED_QUERIES, run_aggregation
from src.db.seed import SOURCES, generate_rows, seed_raw_logs

# Deterministic dataset knobs reused for both the seed and the in-process oracle.
N_ROWS = 800
SEED = 99
END_TS = 1_780_000_000

# Row tuple layout from generate_rows: (ts, source, level, latency_ms, status_code)
_SOURCE_IDX = 1
_LEVEL_IDX = 2


def _expected_rows():
    """Recompute the exact seeded rows in-process (the oracle)."""
    return generate_rows(N_ROWS, seed=SEED, end_ts=END_TS)


async def test_requests_over_time_buckets_sum_and_sorted(pg_pool):
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    result = await run_aggregation(pg_pool, "requests_over_time", {"bucket": "hour"})

    assert isinstance(result, list)
    assert result, "expected a non-empty list of buckets"
    # Each entry has the documented shape.
    for entry in result:
        assert set(entry.keys()) == {"bucket", "count"}
        assert isinstance(entry["bucket"], str)  # ISO string (JSON-serializable)
        assert isinstance(entry["count"], int)

    # Counts over the full window sum to the total seeded rows.
    assert sum(e["count"] for e in result) == N_ROWS

    # Buckets are returned in ascending order.
    buckets = [e["bucket"] for e in result]
    assert buckets == sorted(buckets)


async def test_error_rate_matches_independent_oracle(pg_pool):
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    rows = _expected_rows()
    expected_errors = sum(1 for r in rows if r[_LEVEL_IDX] == "ERROR")

    result = await run_aggregation(pg_pool, "error_rate")

    assert result["total"] == N_ROWS
    assert result["errors"] == expected_errors
    assert result["error_rate"] == pytest.approx(expected_errors / N_ROWS)


async def test_top_sources_ordered_and_complete(pg_pool):
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    # limit large enough to capture every source present.
    result = await run_aggregation(pg_pool, "top_sources", {"limit": 10})

    assert isinstance(result, list)
    assert result
    # Sources are all from the known set.
    for entry in result:
        assert set(entry.keys()) == {"source", "count"}
        assert entry["source"] in set(SOURCES)
        assert isinstance(entry["count"], int)

    # Ordered by count descending.
    counts = [e["count"] for e in result]
    assert counts == sorted(counts, reverse=True)

    # All rows accounted for, and counts match the in-process oracle.
    assert sum(counts) == N_ROWS
    oracle = Counter(r[_SOURCE_IDX] for r in _expected_rows())
    assert {e["source"]: e["count"] for e in result} == dict(oracle)


async def test_source_filter_restricts_to_one_source(pg_pool):
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    rows = _expected_rows()
    expected_api = sum(1 for r in rows if r[_SOURCE_IDX] == "api")

    result = await run_aggregation(pg_pool, "error_rate", {"source": "api"})

    assert result["total"] == expected_api


async def test_avg_latency_shape(pg_pool):
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    result = await run_aggregation(pg_pool, "avg_latency", {"bucket": "day"})

    assert isinstance(result, list)
    assert result
    for entry in result:
        assert set(entry.keys()) == {"bucket", "avg_latency_ms"}
        assert isinstance(entry["bucket"], str)
        assert isinstance(entry["avg_latency_ms"], float)
        # Seeded latencies are in [5, 500] ms.
        assert 5.0 <= entry["avg_latency_ms"] <= 500.0


async def test_unknown_query_raises_value_error(pg_pool):
    with pytest.raises(ValueError):
        await run_aggregation(pg_pool, "definitely_not_a_query")


async def test_supported_queries_membership():
    # Sanity: the documented names are exactly what the backend advertises.
    assert SUPPORTED_QUERIES == {
        "requests_over_time",
        "error_rate",
        "avg_latency",
        "top_sources",
    }


async def test_slow_backend_delay_knob(pg_pool):
    """The delay_ms knob makes the backend measurably slow; 0 keeps it fast."""
    await seed_raw_logs(pg_pool, N_ROWS, seed=SEED, end_ts=END_TS)

    # With a 200ms simulated scan cost, the call must take at least ~0.2s.
    start = time.monotonic()
    await run_aggregation(pg_pool, "error_rate", delay_ms=200)
    slow_elapsed = time.monotonic() - start
    assert slow_elapsed >= 0.2

    # With no delay the same query is fast (well under the slow threshold).
    start = time.monotonic()
    await run_aggregation(pg_pool, "error_rate", delay_ms=0)
    fast_elapsed = time.monotonic() - start
    assert fast_elapsed < 0.2

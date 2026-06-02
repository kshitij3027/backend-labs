"""Integration tests for the Postgres schema, pool, and synthetic seed.

These run against the REAL Postgres wired by the compose ``test`` service
(``DATABASE_URL`` env). They use the ``pg_pool`` fixture, which applies the
schema and truncates both tables for per-test isolation.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.db.pool import apply_schema, normalize_dsn
from src.db.seed import SOURCES, count_raw_logs, generate_rows, seed_raw_logs

# A fixed epoch so seed/window assertions are deterministic across runs.
FIXED_END_TS = 1_780_000_000
SPAN_SECONDS = 7 * 24 * 3600


async def test_apply_schema_idempotent_and_tables_exist(pg_pool):
    # Running apply_schema a second time must not error (idempotent DDL).
    await apply_schema(pg_pool)
    await apply_schema(pg_pool)

    async with pg_pool.acquire() as conn:
        raw = await conn.fetchval("SELECT to_regclass('raw_logs')")
        agg = await conn.fetchval("SELECT to_regclass('precomputed_aggregates')")
    assert raw == "raw_logs"
    assert agg == "precomputed_aggregates"


async def test_seed_raw_logs_inserts_exact_count(pg_pool):
    inserted = await seed_raw_logs(pg_pool, 500, seed=42, end_ts=FIXED_END_TS)
    assert inserted == 500
    assert await count_raw_logs(pg_pool) == 500


def test_generate_rows_is_deterministic():
    a = generate_rows(100, seed=7, end_ts=FIXED_END_TS)
    b = generate_rows(100, seed=7, end_ts=FIXED_END_TS)
    assert a == b
    assert len(a) == 100


async def test_basic_group_by_and_time_window(pg_pool):
    await seed_raw_logs(pg_pool, 500, seed=42, end_ts=FIXED_END_TS)

    async with pg_pool.acquire() as conn:
        grouped = await conn.fetch(
            "SELECT source, count(*) AS c FROM raw_logs GROUP BY source"
        )
        bounds = await conn.fetchrow(
            "SELECT min(ts) AS lo, max(ts) AS hi FROM raw_logs"
        )

    # Group counts sum to the total and every source is a known one.
    total = sum(r["c"] for r in grouped)
    assert total == 500
    assert {r["source"] for r in grouped}.issubset(set(SOURCES))

    # All timestamps fall inside the generation window.
    lo_expected = datetime.fromtimestamp(FIXED_END_TS - SPAN_SECONDS, tz=timezone.utc)
    hi_expected = datetime.fromtimestamp(FIXED_END_TS, tz=timezone.utc)
    assert bounds["lo"] >= lo_expected
    assert bounds["hi"] <= hi_expected


def test_normalize_dsn_strips_asyncpg_suffix():
    assert (
        normalize_dsn("postgresql+asyncpg://u:p@h/db")
        == "postgresql://u:p@h/db"
    )
    # A plain DSN is returned unchanged.
    assert normalize_dsn("postgresql://u:p@h/db") == "postgresql://u:p@h/db"

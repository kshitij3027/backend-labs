"""Deterministic synthetic ``raw_logs`` generation and bulk insert.

The seeded log corpus is the *slow source of truth* the cache fronts. Rows are
generated from a seeded ``random.Random`` so a given ``(n, seed, end_ts)`` always
yields byte-identical tuples — this keeps tests deterministic and makes the
``db-init`` seed reproducible across container rebuilds.
"""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

# Synthetic log sources. Random per-row choice gives a realistic spread for the
# GROUP BY aggregations in C10.
SOURCES = ["api", "web", "db", "auth", "worker"]

# Severity levels with their sampling weights (INFO dominates, ERROR is rare),
# mirroring a healthy production log stream.
LEVELS = ["INFO", "WARN", "ERROR"]
LEVEL_WEIGHTS = [0.80, 0.15, 0.05]

# Default time window the rows span (one week ending at ``end_ts``).
_DEFAULT_SPAN_SECONDS = 7 * 24 * 3600

# Columns inserted into ``raw_logs`` (id is BIGSERIAL, populated by Postgres).
_COLUMNS = ["ts", "source", "level", "latency_ms", "status_code"]

# Plausible HTTP status codes keyed by level.
_ERROR_CODES = [500, 503]
_WARN_CODES = [400, 404, 429]


def _status_for_level(level: str, rng) -> int:
    """Return a status code correlated with ``level``.

    ERROR -> 5xx, WARN -> mostly 4xx (occasionally a clean 200), else 200.
    """
    if level == "ERROR":
        return rng.choice(_ERROR_CODES)
    if level == "WARN":
        # Most WARN lines carry a 4xx, but a minority are benign 200s.
        if rng.random() < 0.7:
            return rng.choice(_WARN_CODES)
        return 200
    return 200


def generate_rows(
    n: int,
    *,
    seed: int,
    end_ts: float,
    span_seconds: int = _DEFAULT_SPAN_SECONDS,
) -> list[tuple]:
    """Generate ``n`` deterministic synthetic log rows.

    Each row is ``(ts, source, level, latency_ms, status_code)`` where ``ts`` is
    a timezone-aware UTC :class:`datetime` uniformly distributed in
    ``[end_ts - span_seconds, end_ts]``. Determinism is guaranteed by seeding a
    private :class:`random.Random` with ``seed``. Rows are returned sorted by
    ``ts`` ascending for nicer (append-friendly) inserts.
    """
    import random

    rng = random.Random(seed)
    start_ts = end_ts - span_seconds

    rows: list[tuple] = []
    for _ in range(n):
        epoch = rng.uniform(start_ts, end_ts)
        ts = datetime.fromtimestamp(epoch, tz=timezone.utc)
        source = rng.choice(SOURCES)
        level = rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
        # Latency: a right-skewed spread roughly in [5, 500] ms.
        latency_ms = round(rng.uniform(5.0, 500.0), 3)
        status_code = _status_for_level(level, rng)
        rows.append((ts, source, level, latency_ms, status_code))

    rows.sort(key=lambda r: r[0])
    return rows


async def seed_raw_logs(
    pool: asyncpg.Pool,
    n: int,
    *,
    seed: int,
    end_ts: float,
    batch: int = 5000,
) -> int:
    """Insert ``n`` deterministic synthetic rows into ``raw_logs``.

    Uses asyncpg's binary ``COPY`` (``copy_records_to_table``) in batches for
    fast bulk loading. Returns the number of rows inserted.
    """
    rows = generate_rows(n, seed=seed, end_ts=end_ts)
    if not rows:
        return 0

    async with pool.acquire() as conn:
        for start in range(0, len(rows), batch):
            chunk = rows[start : start + batch]
            await conn.copy_records_to_table(
                "raw_logs",
                records=chunk,
                columns=_COLUMNS,
            )
    return len(rows)


async def count_raw_logs(pool: asyncpg.Pool) -> int:
    """Return the current row count of ``raw_logs``."""
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT count(*) FROM raw_logs")

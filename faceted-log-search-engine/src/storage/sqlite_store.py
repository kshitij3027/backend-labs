"""Async SQLite store for log rows.

Applies the performance pragmas from the plan (WAL, 64MB cache,
256MB mmap, temp in memory) on every connection, and owns the
``logs`` table schema including STORED generated columns for
``latency_bucket`` and ``hour_bucket`` plus the composite indexes
that match the faceted filter shape.

``connect`` + ``migrate`` are idempotent and safe to run at startup
on a pre-existing database.
"""

from __future__ import annotations

import logging
import os
from typing import AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

# Pragmas applied in-order on every newly opened connection.
# Order matters: journal_mode must be set before synchronous takes effect
# the way we want; cache_size/mmap_size/temp_store are per-connection.
_PRAGMAS: tuple[str, ...] = (
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA cache_size=-64000;",   # 64MB page cache (negative = KiB)
    "PRAGMA mmap_size=268435456;",  # 256MB
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA foreign_keys=ON;",
)

# DDL. Both CREATE TABLE and CREATE INDEX statements use IF NOT EXISTS
# so ``migrate`` is safe to call on an existing database.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS logs (
    id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    region TEXT NOT NULL,
    response_time_ms REAL NOT NULL,
    source_ip TEXT,
    request_id TEXT,
    message TEXT NOT NULL,
    metadata TEXT,
    latency_bucket TEXT GENERATED ALWAYS AS (
        CASE
            WHEN response_time_ms < 100 THEN '0-100ms'
            WHEN response_time_ms < 500 THEN '100-500ms'
            WHEN response_time_ms < 2000 THEN '500ms-2s'
            ELSE '2s+'
        END
    ) STORED,
    hour_bucket INTEGER GENERATED ALWAYS AS (
        CAST(strftime('%H', ts, 'unixepoch') AS INTEGER)
    ) STORED
);
"""

_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_ts_service_level ON logs(ts, service, level);",
    "CREATE INDEX IF NOT EXISTS idx_region_ts ON logs(region, ts);",
    "CREATE INDEX IF NOT EXISTS idx_level_ts ON logs(level, ts);",
    "CREATE INDEX IF NOT EXISTS idx_latency_bucket ON logs(latency_bucket);",
    "CREATE INDEX IF NOT EXISTS idx_hour_bucket ON logs(hour_bucket);",
)


async def connect(db_path: str) -> aiosqlite.Connection:
    """Open a connection to ``db_path`` and apply tuning pragmas.

    Creates the parent directory if it does not exist. Returns a
    live ``aiosqlite.Connection`` with ``row_factory`` set to ``aiosqlite.Row``
    so callers can use dict-style row access.
    """
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    for pragma in _PRAGMAS:
        await conn.execute(pragma)
    await conn.commit()
    logger.info("sqlite connected path=%s pragmas applied=%d", db_path, len(_PRAGMAS))
    return conn


async def migrate(conn: aiosqlite.Connection) -> None:
    """Create the logs table and indexes if they do not already exist.

    Idempotent: ``CREATE TABLE IF NOT EXISTS`` means this survives
    being called on an already-migrated database without touching
    existing rows or trying to re-declare GENERATED columns.
    """
    await conn.execute(_CREATE_TABLE_SQL)
    for idx_sql in _INDEXES:
        await conn.execute(idx_sql)
    await conn.commit()
    logger.info("sqlite migrated table=logs indexes=%d", len(_INDEXES))


async def close(conn: aiosqlite.Connection) -> None:
    """Close the connection. Safe to call on an already-closed conn."""
    try:
        await conn.close()
    except Exception:  # noqa: BLE001 - best-effort shutdown
        logger.exception("error closing sqlite connection")


async def get_db() -> AsyncIterator[aiosqlite.Connection]:
    """FastAPI dependency that yields the app-wide connection.

    Intended for use with ``Depends(get_db)`` after ``main.py`` sets
    ``app.state.db`` via the lifespan hook. Importers should prefer
    ``request.app.state.db`` inside endpoints; this function exists
    for the dependency-injection pattern in later commits.
    """
    # Late import to avoid circulars; the real handle is attached at
    # startup by the lifespan context manager in ``src/main.py``.
    from src.main import app  # pragma: no cover - wired at runtime

    db: aiosqlite.Connection | None = getattr(app.state, "db", None)
    if db is None:
        raise RuntimeError("database not initialized; app lifespan did not run")
    yield db

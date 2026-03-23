import uuid
from datetime import datetime, timezone

import asyncpg
import structlog

from src.config import settings

logger = structlog.get_logger()

pool: asyncpg.Pool | None = None

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'PENDING',
    input_path TEXT NOT NULL,
    map_fn TEXT NOT NULL,
    reduce_fn TEXT NOT NULL,
    num_mappers INTEGER NOT NULL DEFAULT 2,
    num_reducers INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    worker_id TEXT,
    partition_id INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'ALIVE',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tasks_completed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
"""


def _dsn() -> str:
    """Convert the async DSN to a plain postgresql:// DSN for asyncpg."""
    return settings.POSTGRES_URL.replace("postgresql+asyncpg://", "postgresql://")


async def init_db() -> None:
    global pool
    dsn = _dsn()
    logger.info("connecting_to_postgres", dsn=dsn)
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(DDL)
    logger.info("database_initialized")


async def close_db() -> None:
    global pool
    if pool:
        await pool.close()
        pool = None
        logger.info("database_pool_closed")


async def create_job(
    input_path: str,
    map_fn: str,
    reduce_fn: str,
    num_mappers: int = 2,
    num_reducers: int = 2,
) -> dict:
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            job_id,
            "PENDING",
            input_path,
            map_fn,
            reduce_fn,
            num_mappers,
            num_reducers,
            now,
            now,
        )
    logger.info("job_created", job_id=job_id)
    return {
        "id": job_id,
        "status": "PENDING",
        "input_path": input_path,
        "map_fn": map_fn,
        "reduce_fn": reduce_fn,
        "num_mappers": num_mappers,
        "num_reducers": num_reducers,
        "created_at": now,
        "updated_at": now,
    }


async def get_job(job_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    if row is None:
        return None
    return dict(row)


async def list_jobs() -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM jobs ORDER BY created_at DESC")
    return [dict(r) for r in rows]


async def update_job_status(job_id: str, status: str) -> None:
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = $1, updated_at = $2 WHERE id = $3",
            status,
            now,
            job_id,
        )
    logger.info("job_status_updated", job_id=job_id, status=status)


async def get_job_results(job_id: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM results WHERE job_id = $1 ORDER BY key",
            job_id,
        )
    return [dict(r) for r in rows]

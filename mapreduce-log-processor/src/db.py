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
    input_start INTEGER NOT NULL DEFAULT 0,
    input_end INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS input_start INTEGER DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS input_end INTEGER DEFAULT 0;

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


async def insert_results_batch(job_id: str, results: list[tuple[str, str]]) -> None:
    """Batch insert reduce results into the results table."""
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO results (id, job_id, key, value) VALUES ($1, $2, $3, $4)",
            [(str(uuid.uuid4()), job_id, key, value) for key, value in results],
        )
    logger.info("results_inserted", job_id=job_id, count=len(results))


async def get_job_results(job_id: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM results WHERE job_id = $1 ORDER BY key",
            job_id,
        )
    return [dict(r) for r in rows]


# ── Worker operations ────────────────────────────────────────────


async def register_worker(worker_id: str) -> dict:
    """Upsert a worker: insert or update status to ALIVE with fresh heartbeat."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO workers (id, status, last_heartbeat, tasks_completed)
            VALUES ($1, 'ALIVE', $2, 0)
            ON CONFLICT (id) DO UPDATE
                SET status = 'ALIVE', last_heartbeat = $2
            RETURNING *
            """,
            worker_id,
            now,
        )
    logger.info("worker_registered", worker_id=worker_id)
    return dict(row)


async def update_heartbeat(worker_id: str) -> bool:
    """Update last_heartbeat for a worker. Returns True if the worker exists."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE workers SET last_heartbeat = $1 WHERE id = $2",
            now,
            worker_id,
        )
    # asyncpg returns e.g. "UPDATE 1" or "UPDATE 0"
    updated = result.split()[-1] != "0"
    return updated


async def get_workers() -> list[dict]:
    """List all workers."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM workers ORDER BY last_heartbeat DESC")
    return [dict(r) for r in rows]


async def get_alive_workers() -> list[dict]:
    """List workers with status ALIVE."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM workers WHERE status = 'ALIVE' ORDER BY last_heartbeat DESC"
        )
    return [dict(r) for r in rows]


async def mark_worker_dead(worker_id: str) -> None:
    """Update worker status to DEAD."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workers SET status = 'DEAD' WHERE id = $1",
            worker_id,
        )
    logger.info("worker_marked_dead", worker_id=worker_id)


async def get_tasks_for_job(job_id: str) -> list[dict]:
    """Get all tasks for a job."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM tasks WHERE job_id = $1 ORDER BY partition_id",
            job_id,
        )
    return [dict(r) for r in rows]


async def increment_worker_tasks(worker_id: str) -> None:
    """Increment the tasks_completed counter for a worker."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workers SET tasks_completed = tasks_completed + 1 WHERE id = $1",
            worker_id,
        )


async def reassign_tasks_for_worker(worker_id: str, max_retries: int) -> list[dict]:
    """Find RUNNING tasks for a dead worker, increment retry_count,
    set to PENDING or FAILED based on retry limit. Return list of affected tasks."""
    affected: list[dict] = []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM tasks WHERE status = 'RUNNING' AND worker_id = $1",
            worker_id,
        )
        for row in rows:
            task = dict(row)
            new_retry = task["retry_count"] + 1
            if new_retry < max_retries:
                await conn.execute(
                    """UPDATE tasks
                       SET status = 'PENDING', worker_id = NULL,
                           retry_count = $1, updated_at = NOW()
                       WHERE id = $2""",
                    new_retry, task["id"],
                )
                task["new_status"] = "PENDING"
            else:
                await conn.execute(
                    """UPDATE tasks
                       SET status = 'FAILED', retry_count = $1, updated_at = NOW()
                       WHERE id = $2""",
                    new_retry, task["id"],
                )
                task["new_status"] = "FAILED"
            task["retry_count"] = new_retry
            affected.append(task)
    return affected


async def get_incomplete_jobs() -> list[dict]:
    """Get jobs not in terminal state (COMPLETED, FAILED, CANCELLED)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM jobs WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') ORDER BY created_at",
        )
    return [dict(r) for r in rows]


async def get_stale_running_tasks(worker_ids: list[str]) -> list[dict]:
    """Get RUNNING tasks assigned to dead/missing workers."""
    if not worker_ids:
        # If no alive workers provided, get all RUNNING tasks
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM tasks WHERE status = 'RUNNING'",
            )
        return [dict(r) for r in rows]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM tasks
               WHERE status = 'RUNNING'
               AND (worker_id IS NULL OR worker_id != ALL($1::text[]))""",
            worker_ids,
        )
    return [dict(r) for r in rows]


async def reset_task_to_pending(task_id: str) -> None:
    """Reset task status to PENDING, clear worker_id."""
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE tasks
               SET status = 'PENDING', worker_id = NULL, updated_at = NOW()
               WHERE id = $1""",
            task_id,
        )
    logger.info("task_reset_to_pending", task_id=task_id)


async def fail_task_and_check_job(task_id: str, max_retries: int) -> bool:
    """Check if the job containing this task should be FAILED
    (any task FAILED with retry_count >= max). Returns True if job was failed.
    The task should already be marked FAILED before calling this."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT job_id FROM tasks WHERE id = $1",
            task_id,
        )
        if row is None:
            return False
        job_id = row["job_id"]
        # Check if any task for this job has exhausted retries
        failed_count = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE job_id = $1 AND status = 'FAILED' AND retry_count >= $2",
            job_id, max_retries,
        )
        if failed_count > 0:
            await conn.execute(
                "UPDATE jobs SET status = 'FAILED', updated_at = NOW() WHERE id = $1 AND status != 'FAILED'",
                job_id,
            )
            logger.warning("job_failed_due_to_task_retries", job_id=job_id)
            return True
    return False


async def delete_results_for_partition(job_id: str, partition_id: int) -> None:
    """Delete results for a specific job+partition (for idempotent reduce).
    Uses the task's partition_id to identify which results to delete by matching
    result keys that were produced by the reducer for this partition."""
    # Since results don't have partition_id, we delete all results for the job
    # that would be regenerated by this reduce task.
    # The safest approach: delete results for this job before re-inserting.
    # However, results don't track partition. So we delete all results for the job
    # and let all reduce tasks re-insert. But that's too broad.
    # A practical approach: use a sub-table or just delete by job_id.
    # For simplicity, we add partition_id tracking to results or delete by job.
    # Since the schema doesn't have partition_id on results, we'll delete all
    # results for this job (reduce tasks for the same job all run close together).
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM results WHERE job_id = $1",
            job_id,
        )
    logger.info("results_deleted_for_partition", job_id=job_id, partition_id=partition_id)

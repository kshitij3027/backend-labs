"""Coordinator crash recovery logic.

On coordinator startup, recover in-progress jobs that may have been
interrupted by a previous coordinator crash.
"""

from datetime import datetime, timezone

import structlog

from src.config import settings
import src.db as db
from src.coordinator.scheduler import create_reduce_tasks

logger = structlog.get_logger()


async def on_startup_recovery() -> None:
    """Recover in-progress jobs after coordinator restart.

    1. Find all jobs not in terminal state (COMPLETED, FAILED, CANCELLED)
    2. For each:
       - Find tasks with status=RUNNING (stale from before crash) -> reset to PENDING
       - If all MAP tasks COMPLETED but no REDUCE tasks exist -> create reduce tasks
       - If some REDUCE tasks are COMPLETED and job is not COMPLETED -> check if all done
    3. Mark workers with old heartbeats as DEAD
    4. Log recovery actions
    """
    logger.info("recovery_starting")

    # Step 3 (first): Mark stale workers as DEAD
    await _recover_stale_workers()

    # Steps 1-2: Recover incomplete jobs
    incomplete_jobs = await db.get_incomplete_jobs()
    logger.info("recovery_found_incomplete_jobs", count=len(incomplete_jobs))

    for job in incomplete_jobs:
        await _recover_job(job)

    logger.info("recovery_complete")


async def _recover_stale_workers() -> None:
    """Mark workers with old heartbeats as DEAD."""
    workers = await db.get_alive_workers()
    now = datetime.now(timezone.utc)
    stale_count = 0

    for w in workers:
        last_hb = w["last_heartbeat"]
        if last_hb.tzinfo is None:
            last_hb = last_hb.replace(tzinfo=timezone.utc)
        elapsed = (now - last_hb).total_seconds()
        if elapsed > settings.HEARTBEAT_TIMEOUT:
            await db.mark_worker_dead(w["id"])
            logger.info(
                "recovery_worker_marked_dead",
                worker_id=w["id"],
                elapsed_seconds=elapsed,
            )
            stale_count += 1

    if stale_count:
        logger.info("recovery_stale_workers_marked_dead", count=stale_count)


async def _recover_job(job: dict) -> None:
    """Recover a single incomplete job."""
    job_id = job["id"]
    job_status = job["status"]

    async with db.pool.acquire() as conn:
        all_tasks = await conn.fetch(
            "SELECT * FROM tasks WHERE job_id = $1 ORDER BY partition_id",
            job_id,
        )

    tasks = [dict(t) for t in all_tasks]
    map_tasks = [t for t in tasks if t["type"] == "MAP"]
    reduce_tasks = [t for t in tasks if t["type"] == "REDUCE"]

    # Reset any RUNNING tasks to PENDING (they were running when coordinator crashed)
    stale_running = [t for t in tasks if t["status"] == "RUNNING"]
    for task in stale_running:
        await db.reset_task_to_pending(task["id"])
        logger.info(
            "recovery_task_reset",
            task_id=task["id"],
            job_id=job_id,
            type=task["type"],
        )

    # If all MAP tasks are COMPLETED but no REDUCE tasks exist, create reduce tasks
    all_maps_completed = all(t["status"] == "COMPLETED" for t in map_tasks) if map_tasks else False
    if all_maps_completed and not reduce_tasks:
        num_reducers = job["num_reducers"]
        await create_reduce_tasks(job_id, num_reducers)
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status = 'REDUCING', updated_at = NOW() WHERE id = $1",
                job_id,
            )
        logger.info("recovery_created_reduce_tasks", job_id=job_id, num_reducers=num_reducers)
        return

    # If reduce tasks exist, check if all are COMPLETED
    if reduce_tasks:
        all_reduces_completed = all(t["status"] == "COMPLETED" for t in reduce_tasks)
        if all_reduces_completed:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1",
                    job_id,
                )
            logger.info("recovery_job_completed", job_id=job_id)
            return

    logger.info(
        "recovery_job_state",
        job_id=job_id,
        status=job_status,
        map_tasks=len(map_tasks),
        reduce_tasks=len(reduce_tasks),
        stale_running=len(stale_running),
    )

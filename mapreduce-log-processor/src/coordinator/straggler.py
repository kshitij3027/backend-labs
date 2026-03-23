"""Straggler detection and speculative execution for MapReduce tasks."""

import uuid
from datetime import datetime, timezone

import structlog

import src.db as db

logger = structlog.get_logger()


async def detect_stragglers() -> list[dict]:
    """Detect tasks taking significantly longer than average.

    For each job with RUNNING tasks:
    1. Compute average duration of completed tasks of same type in same job
    2. If a running task's elapsed time > 2x average AND at least 3 tasks have completed
    3. Mark as straggler, create speculative copy
    """
    stragglers = []
    async with db.pool.acquire() as conn:
        # Get all running tasks that don't already have a speculative copy
        running = await conn.fetch(
            """SELECT id, job_id, type, worker_id, created_at, partition_id, input_start, input_end
               FROM tasks
               WHERE status = 'RUNNING'
               AND id NOT IN (SELECT speculative_of FROM tasks WHERE speculative_of IS NOT NULL AND status != 'FAILED')"""
        )

        for task in running:
            # Get completed tasks of same type in same job
            completed = await conn.fetch(
                """SELECT EXTRACT(EPOCH FROM (updated_at - created_at)) as duration
                   FROM tasks
                   WHERE job_id = $1 AND type = $2 AND status = 'COMPLETED'""",
                task["job_id"], task["type"],
            )

            if len(completed) < 3:
                continue

            avg_duration = sum(r["duration"] for r in completed) / len(completed)
            elapsed = (datetime.now(timezone.utc) - task["created_at"].replace(tzinfo=timezone.utc)
                       if task["created_at"].tzinfo is None
                       else datetime.now(timezone.utc) - task["created_at"]).total_seconds()

            if elapsed > 2 * avg_duration:
                logger.warning(
                    "straggler_detected",
                    task_id=task["id"],
                    job_id=task["job_id"],
                    elapsed=elapsed,
                    avg_duration=avg_duration,
                    worker_id=task["worker_id"],
                )
                stragglers.append(dict(task))

                # Create speculative copy
                spec_id = str(uuid.uuid4())
                await conn.execute(
                    """INSERT INTO tasks (id, job_id, type, status, partition_id, input_start, input_end, speculative_of)
                       VALUES ($1, $2, $3, 'PENDING', $4, $5, $6, $7)""",
                    spec_id,
                    task["job_id"],
                    task["type"],
                    task["partition_id"],
                    task["input_start"],
                    task["input_end"],
                    task["id"],
                )
                logger.info("speculative_task_created", original=task["id"], speculative=spec_id)

    return stragglers

"""Core scheduling logic for map/reduce task assignment."""

import uuid

import structlog

import src.db as db

logger = structlog.get_logger()


async def partition_input(input_path: str, num_mappers: int) -> list[tuple[int, int]]:
    """Split a log file into num_mappers partitions by line ranges.
    Returns list of (start_line, end_line) tuples."""
    total_lines = 0
    with open(input_path) as f:
        for _ in f:
            total_lines += 1

    if total_lines == 0:
        return [(0, 0)] * num_mappers

    chunk_size = total_lines // num_mappers
    partitions = []
    for i in range(num_mappers):
        start = i * chunk_size
        end = total_lines if i == num_mappers - 1 else (i + 1) * chunk_size
        partitions.append((start, end))
    return partitions


async def create_map_tasks(job_id: str, input_path: str, num_mappers: int) -> list[dict]:
    """Create map task records in DB for a job."""
    partitions = await partition_input(input_path, num_mappers)
    tasks = []
    async with db.pool.acquire() as conn:
        for i, (start, end) in enumerate(partitions):
            task_id = str(uuid.uuid4())
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, partition_id, input_start, input_end)
                   VALUES ($1, $2, 'MAP', 'PENDING', $3, $4, $5)""",
                task_id, job_id, i, start, end,
            )
            tasks.append({"id": task_id, "partition_id": i, "start": start, "end": end})
    logger.info("map_tasks_created", job_id=job_id, count=len(tasks))
    return tasks


async def create_reduce_tasks(job_id: str, num_reducers: int) -> list[dict]:
    """Create reduce task records in DB for a job (one per reducer partition)."""
    tasks = []
    async with db.pool.acquire() as conn:
        for i in range(num_reducers):
            task_id = str(uuid.uuid4())
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, partition_id, input_start, input_end)
                   VALUES ($1, $2, 'REDUCE', 'PENDING', $3, 0, 0)""",
                task_id, job_id, i,
            )
            tasks.append({"id": task_id, "partition_id": i})
    logger.info("reduce_tasks_created", job_id=job_id, count=len(tasks))
    return tasks


async def assign_task(worker_id: str) -> dict | None:
    """Atomically assign a pending task to a worker using SELECT FOR UPDATE SKIP LOCKED."""
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """SELECT t.*, j.input_path, j.map_fn, j.reduce_fn, j.num_reducers
                   FROM tasks t
                   JOIN jobs j ON t.job_id = j.id
                   WHERE t.status = 'PENDING'
                   ORDER BY t.created_at
                   LIMIT 1
                   FOR UPDATE OF t SKIP LOCKED""",
            )
            if row is None:
                return None
            await conn.execute(
                "UPDATE tasks SET status = 'RUNNING', worker_id = $1, updated_at = NOW() WHERE id = $2",
                worker_id, row["id"],
            )
    logger.info("task_assigned", task_id=row["id"], worker_id=worker_id, type=row["type"])
    return dict(row)


async def complete_task(task_id: str) -> None:
    """Mark a task as completed. Check if all map tasks for the job are done."""
    async with db.pool.acquire() as conn:
        # Update task status
        row = await conn.fetchrow(
            "UPDATE tasks SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1 RETURNING job_id, type, worker_id",
            task_id,
        )
        if row is None:
            logger.warning("complete_task_not_found", task_id=task_id)
            return

        job_id = row["job_id"]
        task_type = row["type"]
        worker_id = row["worker_id"]

        # Increment worker's tasks_completed counter
        if worker_id:
            await conn.execute(
                "UPDATE workers SET tasks_completed = tasks_completed + 1 WHERE id = $1",
                worker_id,
            )

        if task_type == "MAP":
            # Check if all map tasks for this job are completed
            pending = await conn.fetchval(
                "SELECT COUNT(*) FROM tasks WHERE job_id = $1 AND type = 'MAP' AND status != 'COMPLETED'",
                job_id,
            )
            if pending == 0:
                # All map tasks done - get num_reducers from job
                job_row = await conn.fetchrow(
                    "SELECT num_reducers FROM jobs WHERE id = $1", job_id,
                )
                logger.info("all_map_tasks_completed", job_id=job_id)

                # Create reduce tasks and transition to REDUCING
                await create_reduce_tasks(job_id, job_row["num_reducers"])
                await conn.execute(
                    "UPDATE jobs SET status = 'REDUCING', updated_at = NOW() WHERE id = $1",
                    job_id,
                )
                logger.info("job_reducing", job_id=job_id)

        elif task_type == "REDUCE":
            # Check if all reduce tasks for this job are completed
            pending = await conn.fetchval(
                "SELECT COUNT(*) FROM tasks WHERE job_id = $1 AND type = 'REDUCE' AND status != 'COMPLETED'",
                job_id,
            )
            if pending == 0:
                await conn.execute(
                    "UPDATE jobs SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1",
                    job_id,
                )
                logger.info("all_reduce_tasks_completed", job_id=job_id)

    logger.info("task_completed", task_id=task_id, type=task_type)


async def fail_task(task_id: str) -> None:
    """Mark a task as failed."""
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET status = 'FAILED', updated_at = NOW() WHERE id = $1",
            task_id,
        )
    logger.warning("task_failed", task_id=task_id)

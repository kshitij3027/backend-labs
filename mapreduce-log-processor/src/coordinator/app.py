import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from src.config import settings
from src.coordinator.heartbeat import heartbeat_checker
from src.coordinator.metrics import metrics
from src.coordinator.recovery import on_startup_recovery
from src.coordinator.scheduler import assign_task, complete_task, create_map_tasks, create_reduce_tasks, fail_task
import src.db as db
from src.db import (
    close_db,
    create_job,
    get_job,
    get_job_results,
    get_workers,
    init_db,
    list_jobs,
    register_worker,
    update_heartbeat,
    update_job_status,
)
from src.models import (
    JobCreate,
    JobResponse,
    JobResultResponse,
    JobStatus,
    ResultItem,
    WorkerInfo,
)
from src.redis_client import close_redis, init_redis

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("coordinator_starting")
    await init_db()
    await init_redis()

    # Run crash recovery before starting normal operations
    await on_startup_recovery()

    # Start heartbeat checker background task
    hb_task = asyncio.create_task(
        heartbeat_checker(
            interval=settings.HEARTBEAT_INTERVAL,
            timeout=settings.HEARTBEAT_TIMEOUT,
        )
    )
    logger.info("coordinator_ready")
    yield
    logger.info("coordinator_shutting_down")

    # Cancel heartbeat checker
    hb_task.cancel()
    try:
        await hb_task
    except asyncio.CancelledError:
        pass

    await close_redis()
    await close_db()
    logger.info("coordinator_stopped")


app = FastAPI(
    title="MapReduce Log Processor",
    description="Distributed MapReduce framework for log processing",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/stats")
async def get_stats():
    """Return current system stats: jobs, tasks, and workers."""
    async with db.pool.acquire() as conn:
        total_jobs = await conn.fetchval("SELECT COUNT(*) FROM jobs")
        active_jobs = await conn.fetchval(
            "SELECT COUNT(*) FROM jobs WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')"
        )
        pending_tasks = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'PENDING'"
        )
        running_tasks = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'RUNNING'"
        )
        completed_tasks = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'COMPLETED'"
        )
        failed_tasks = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'FAILED'"
        )
        workers_alive = await conn.fetchval(
            "SELECT COUNT(*) FROM workers WHERE status = 'ALIVE'"
        )
        workers_dead = await conn.fetchval(
            "SELECT COUNT(*) FROM workers WHERE status = 'DEAD'"
        )
    return {
        "total_jobs": total_jobs,
        "active_jobs": active_jobs,
        "pending_tasks": pending_tasks,
        "running_tasks": running_tasks,
        "completed_tasks": completed_tasks,
        "failed_tasks": failed_tasks,
        "workers_alive": workers_alive,
        "workers_dead": workers_dead,
    }


@app.get("/metrics")
async def get_metrics():
    """Return in-memory metrics: job/task counts, durations, shuffle volumes."""
    return metrics.to_dict()


@app.post("/jobs", status_code=201, response_model=JobResponse)
async def submit_job(job: JobCreate):
    metrics.record_job_submitted()
    logger.info(
        "job_submitted",
        input_path=job.input_path,
        map_fn=job.map_fn,
        reduce_fn=job.reduce_fn,
    )
    result = await create_job(
        input_path=job.input_path,
        map_fn=job.map_fn,
        reduce_fn=job.reduce_fn,
        num_mappers=job.num_mappers,
        num_reducers=job.num_reducers,
    )

    job_id = result["id"]

    # Create map tasks for this job
    try:
        tasks = await create_map_tasks(job_id, job.input_path, job.num_mappers)
        await update_job_status(job_id, JobStatus.MAPPING.value)
        result["status"] = JobStatus.MAPPING.value
        logger.info("job_map_tasks_created", job_id=job_id, num_tasks=len(tasks))
    except Exception as e:
        logger.error("job_map_task_creation_failed", job_id=job_id, error=str(e))
        await update_job_status(job_id, JobStatus.FAILED.value)
        result["status"] = JobStatus.FAILED.value

    return JobResponse(**result)


@app.get("/jobs", response_model=list[JobResponse])
async def get_jobs():
    rows = await list_jobs()
    return [JobResponse(**r) for r in rows]


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_by_id(job_id: str):
    row = await get_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse(**row)


@app.get("/jobs/{job_id}/result", response_model=JobResultResponse)
async def get_result(job_id: str):
    row = await get_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    results = await get_job_results(job_id)
    return JobResultResponse(
        job_id=job_id,
        status=JobStatus(row["status"]),
        results=[ResultItem(**r) for r in results],
    )


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    row = await get_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    await update_job_status(job_id, JobStatus.CANCELLED.value)
    logger.info("job_cancelled", job_id=job_id)
    return {"id": job_id, "status": "CANCELLED"}


# ── Task endpoints ──────────────────────────────────────────────


@app.get("/tasks/next")
async def get_next_task(worker_id: str):
    """Assign the next pending task to the requesting worker."""
    task = await assign_task(worker_id)
    if task is None:
        return Response(status_code=204)
    return JSONResponse(content={
        "id": task["id"],
        "job_id": task["job_id"],
        "type": task["type"],
        "status": "RUNNING",
        "partition_id": task["partition_id"],
        "input_start": task["input_start"],
        "input_end": task["input_end"],
        "input_path": task["input_path"],
        "map_fn": task["map_fn"],
        "reduce_fn": task["reduce_fn"],
        "num_reducers": task["num_reducers"],
    })


@app.post("/tasks/{task_id}/complete")
async def mark_task_complete(task_id: str):
    """Mark a task as completed."""
    await complete_task(task_id)
    return {"task_id": task_id, "status": "COMPLETED"}


@app.post("/tasks/{task_id}/failed")
async def mark_task_failed(task_id: str):
    """Mark a task as failed, with retry logic."""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM tasks WHERE id = $1", task_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Task not found")

    task = dict(row)
    new_retry = task["retry_count"] + 1

    if new_retry < settings.MAX_RETRIES:
        # Reset to PENDING for retry
        async with db.pool.acquire() as conn:
            await conn.execute(
                """UPDATE tasks
                   SET status = 'PENDING', worker_id = NULL,
                       retry_count = $1, updated_at = NOW()
                   WHERE id = $2""",
                new_retry, task_id,
            )
        logger.info(
            "task_reset_for_retry",
            task_id=task_id,
            retry_count=new_retry,
        )
        return {"task_id": task_id, "status": "PENDING", "retry_count": new_retry}
    else:
        # Max retries exceeded — mark FAILED and check job
        async with db.pool.acquire() as conn:
            await conn.execute(
                """UPDATE tasks
                   SET status = 'FAILED', retry_count = $1, updated_at = NOW()
                   WHERE id = $2""",
                new_retry, task_id,
            )
        metrics.record_task_failed(task["type"])
        job_failed = await db.fail_task_and_check_job(task_id, settings.MAX_RETRIES)
        if job_failed:
            metrics.record_job_failed()
        logger.warning(
            "task_permanently_failed",
            task_id=task_id,
            retry_count=new_retry,
            job_failed=job_failed,
        )
        return {"task_id": task_id, "status": "FAILED", "retry_count": new_retry}


# ── Worker endpoints ────────────────────────────────────────────


class WorkerRegisterRequest(BaseModel):
    worker_id: str


@app.post("/workers/register")
async def register_worker_endpoint(body: WorkerRegisterRequest):
    worker = await register_worker(body.worker_id)
    logger.info("worker_registered_via_api", worker_id=body.worker_id)
    return {"worker_id": worker["id"], "status": worker["status"]}


@app.post("/workers/{worker_id}/heartbeat")
async def worker_heartbeat(worker_id: str):
    updated = await update_heartbeat(worker_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Worker not found")
    return {"worker_id": worker_id, "status": "ok"}


@app.get("/workers", response_model=list[WorkerInfo])
async def list_workers():
    rows = await get_workers()
    return [WorkerInfo(**r) for r in rows]

import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.coordinator.heartbeat import heartbeat_checker
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


@app.post("/jobs", status_code=201, response_model=JobResponse)
async def submit_job(job: JobCreate):
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

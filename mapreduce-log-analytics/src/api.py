"""FastAPI application with REST endpoints for MapReduce job management."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from src.analyzers.registry import list_analyzers
from src.config import Config
from src.job_manager import JobManager
from src.models import JobSubmission
from src.websocket import ConnectionManager

logger = logging.getLogger(__name__)

ws_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: setup and teardown."""
    config = Config.from_env()

    # Setup logging
    log_level = config.log_level.upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Ensure directories exist
    os.makedirs(config.upload_dir, exist_ok=True)
    os.makedirs(config.output_dir, exist_ok=True)

    # Create job manager
    app.state.job_manager = JobManager(config)
    app.state.config = config

    # Wire up sync-to-async WebSocket broadcast bridge
    loop = asyncio.get_event_loop()

    def sync_broadcast(data: dict):
        """Bridge sync->async for WebSocket broadcast from background threads."""
        asyncio.run_coroutine_threadsafe(ws_manager.broadcast(data), loop)

    app.state.job_manager.set_ws_broadcast(sync_broadcast)

    logger.info(f"MapReduce Log Analytics started on port {config.port}")
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title="MapReduce Log Analytics",
    description="Distributed batch processing engine for log analysis",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/jobs/submit", status_code=201)
async def submit_job(submission: JobSubmission):
    """Submit a new MapReduce job."""
    # Validate input files
    for f in submission.input_files:
        if not os.path.isfile(f):
            raise HTTPException(400, f"File not found: {f}")

    job = app.state.job_manager.submit_job(submission)
    return job.model_dump()


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    jobs = app.state.job_manager.list_jobs()
    return [j.model_dump() for j in jobs]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status and results."""
    job = app.state.job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job not found: {job_id}")
    return job.model_dump()


@app.get("/api/functions")
async def list_functions():
    """List available analysis functions."""
    return list_analyzers()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time job updates."""
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve the web dashboard."""
    dashboard_path = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")
    with open(dashboard_path) as f:
        return HTMLResponse(content=f.read())

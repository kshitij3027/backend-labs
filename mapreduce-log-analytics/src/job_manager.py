"""Job lifecycle manager with background execution."""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import uuid4

from src.config import Config
from src.engine import MapReduceEngine
from src.models import AnalysisType, JobInfo, JobStatus, JobSubmission

logger = logging.getLogger(__name__)


class JobManager:
    """Manages job lifecycle: submission, background execution, status tracking."""

    def __init__(self, config: Config):
        self.config = config
        self.engine = MapReduceEngine(
            num_workers=config.num_workers,
            chunk_size=config.chunk_size,
        )
        self._jobs: dict[str, JobInfo] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=3)
        self._ws_broadcast = None  # Set by API layer for WebSocket updates

    def set_ws_broadcast(self, broadcast_fn):
        """Set the WebSocket broadcast callback."""
        self._ws_broadcast = broadcast_fn

    def submit_job(self, submission: JobSubmission) -> JobInfo:
        """Submit a new job for background processing. Returns immediately."""
        job_id = f"job_{int(time.time())}_{uuid4().hex[:8]}"

        job = JobInfo(
            job_id=job_id,
            status=JobStatus.PENDING,
            analysis_type=submission.analysis_type,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        if submission.num_workers:
            # Override engine workers for this job
            pass  # handled in _run_job

        with self._lock:
            self._jobs[job_id] = job

        self._executor.submit(self._run_job, job_id, submission)
        logger.info(f"Job {job_id} submitted: {submission.analysis_type.value}")
        return job

    def get_job(self, job_id: str) -> JobInfo | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobInfo]:
        with self._lock:
            return list(self._jobs.values())

    def _run_job(self, job_id: str, submission: JobSubmission):
        """Background job execution."""
        job = self._jobs[job_id]
        start_time = time.time()

        try:
            # Validate input files exist
            for f in submission.input_files:
                if not os.path.isfile(f):
                    raise FileNotFoundError(f"Input file not found: {f}")

            # Determine analysis function names
            analysis_name = submission.analysis_type.value.lower()

            # Create engine with custom workers if specified
            num_workers = submission.num_workers or self.config.num_workers
            engine = MapReduceEngine(
                num_workers=num_workers,
                chunk_size=self.config.chunk_size,
            )

            def progress_callback(phase, progress, info):
                with self._lock:
                    job.current_phase = phase
                    job.progress = progress
                    if phase == "mapping":
                        job.status = JobStatus.MAPPING
                        job.completed_chunks = info.get("completed_chunks", 0)
                        job.total_chunks = info.get("total_chunks", 0)
                        job.records_processed = info.get("records_processed", 0)
                    elif phase == "shuffling":
                        job.status = JobStatus.SHUFFLING
                    elif phase == "reducing":
                        job.status = JobStatus.REDUCING

                # Broadcast via WebSocket if available
                if self._ws_broadcast:
                    try:
                        self._ws_broadcast({
                            "type": "job_update",
                            "job_id": job_id,
                            "status": job.status.value,
                            "progress": job.progress,
                            "current_phase": job.current_phase,
                            "records_processed": job.records_processed,
                        })
                    except Exception:
                        pass  # Don't fail the job because of WS errors

            results = engine.run(
                submission.input_files,
                analysis_name,
                analysis_name,
                progress_callback=progress_callback,
            )

            elapsed = time.time() - start_time
            with self._lock:
                job.set_completed(results, elapsed)

            logger.info(f"Job {job_id} completed in {elapsed:.2f}s")

            # Broadcast completion
            if self._ws_broadcast:
                try:
                    self._ws_broadcast({
                        "type": "job_completed",
                        "job_id": job_id,
                        "status": "COMPLETED",
                        "execution_time": elapsed,
                        "results": results,
                    })
                except Exception:
                    pass

        except Exception as e:
            elapsed = time.time() - start_time
            with self._lock:
                job.set_failed(str(e), elapsed)
            logger.error(f"Job {job_id} failed: {e}")

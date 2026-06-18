# syntax=docker/dockerfile:1.6
#
# Minimal worker image for the OPT-IN Docker worker backend.
#
# src.workers.DockerWorkerPool spawns one container of this image per "worker" and
# reconciles their count to track demand. The workload itself is intentionally trivial:
# worker.py just sleeps in a loop and prints a heartbeat, so the container stays running
# and is observable/countable. It performs no real processing — the point of this image
# is purely to give the pool something concrete to start, count, and gracefully stop.
#
# Build (tag must match Settings.worker_image, default "adaptive-worker:latest"):
#     docker build -t adaptive-worker:latest workers/

FROM python:3.11-slim

# Unbuffered stdout so the heartbeat streams live to `docker logs`.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Run as a non-root user (uid 1000) — the worker needs no privileges.
RUN useradd --uid 1000 --create-home appuser

COPY worker.py ./worker.py

USER appuser

# Long-running heartbeat loop; the DockerWorkerPool keeps this alive or retires it.
CMD ["python", "worker.py"]

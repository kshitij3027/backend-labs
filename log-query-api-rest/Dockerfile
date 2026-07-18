# syntax=docker/dockerfile:1.6
#
# Production runtime image for the Log Query API (REST).
#
# A single uvicorn/FastAPI process on :8000 serving the whole surface — the unversioned
# /health liveness probe, the /api/v1 data routes, the SSE tail, and (from a later commit)
# the static dashboard. ALL state is in-memory in this one process: the append-only log
# ring and the rate limiter's token buckets both live here. There is no database, no Redis
# and no message queue, so this is the ONLY runtime image the project ships.
#
# Only the runtime surface is copied in: src/ (which also carries src/static/ once the
# dashboard lands). Tests, scripts, plan.md and other junk are excluded by .dockerignore
# and never reach the production image — see Dockerfile.test for the tester variant.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live rather than sitting in a pipe buffer), no .pyc
# files on a read-only-ish layer, and PYTHONPATH=/app so the `src` package resolves no
# matter what CWD uvicorn is launched from.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# curl only, and solely for the HEALTHCHECK below. Every pin in requirements.txt resolves
# to a prebuilt manylinux wheel on python:3.11-slim — including bcrypt and orjson — so no
# build-essential/gcc toolchain is needed and the image stays small.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Dependencies first, so this layer caches and is only invalidated when requirements.txt
# itself changes. Application edits then rebuild only the thin COPY layer below it.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code only. Later commits only rebuild from this layer down.
COPY src/ ./src/

# Run as a non-root user (uid 1000). The app writes nothing to disk, so it needs no write
# access beyond its own home.
RUN useradd --uid 1000 --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits the UNVERSIONED /health — it is dependency-free and needs no token,
# so the container is healthy as soon as uvicorn binds. The 20s start-period covers the
# startup corpus seed (SEED_ENTRIES) without letting a slow seed flap the container.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# The container ALWAYS binds :8000; the host-side port is a compose concern
# ("${API_PORT:-8010}:8000"), which is why API_PORT is informational inside the app.
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

# syntax=docker/dockerfile:1.6
#
# Production runtime image for the RCA Analysis Engine.
#
# A single uvicorn/FastAPI process on :8000 serving the RCA API (REST +, from C6, the
# /ws WebSocket). ALL analysis state is in-memory in this one process — there is no
# database, no Redis and no message queue — so this is the only backend image. The
# React dashboard (C11) is a separate nginx image built from frontend/.
#
# Only the runtime surface is copied in: src/ (including the externalized
# src/config/service_dependency_map.json). Tests, plan.md, project_requirements.md and
# other junk are excluded by .dockerignore and never land in the production image.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live), no .pyc files, and PYTHONPATH=/app so
# the `src` package resolves regardless of CWD.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# curl only, for the HEALTHCHECK. numpy/scipy install as prebuilt manylinux wheels on
# python:3.11-slim, so no build-essential/gcc toolchain is needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached unless requirements.txt changes).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code, including src/config/service_dependency_map.json. Nothing else.
COPY src/ ./src/

# Run as a non-root user (uid 1000).
RUN useradd --uid 1000 --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits /api/health. It is dependency-free (a constant spec body), so
# the app is healthy as soon as uvicorn binds; 20s start-period is a safe start-up
# margin. NOTE the /api/health path — the spec namespaces health under /api.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

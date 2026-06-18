# syntax=docker/dockerfile:1.6
#
# Production runtime image for the Adaptive Resource Allocation System.
#
# A single eventlet/Flask-SocketIO process on :8080. The orchestrator holds all
# state (metrics history, worker pool, scaling decisions) in process memory, so
# this is intentionally a single process — never scale it to multiple workers or
# the in-memory state diverges.
#
# Only the runtime surface is copied in: src/ (the app), templates/ + static/
# (the live dashboard), and config/ (default YAML). Tests, plan.md, and
# project_requirements.md are excluded by .dockerignore and are never COPYed, so
# they cannot land in the production image.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live), no .pyc files, and PYTHONPATH=/app
# so `python -m src.main` resolves the `src` package regardless of CWD.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# curl only — required by the HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached unless requirements.txt changes).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run as a non-root user (uid 1000).
RUN useradd --uid 1000 --create-home appuser

# Application + dashboard + default config. Nothing else.
COPY src/ ./src/
COPY templates/ ./templates/
COPY static/ ./static/
COPY config/ ./config/

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

# Liveness probe hits the always-200 /health route. start-period gives the
# eventlet hub time to bind before failures count against the container.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS http://localhost:8080/health || exit 1

# Eventlet bootstrap: monkey-patches the stdlib then runs socketio on :8080.
CMD ["python", "-m", "src.main"]

# syntax=docker/dockerfile:1.6

# ---------- Stage 1: builder ----------
# Build wheels into an isolated venv that the runtime stage copies verbatim.
# Doing this in a dedicated stage keeps the runtime image free of compilers
# and apt caches, and gives us a clean "all deps prebuilt" layer that
# Docker can cache across source edits.
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Pin pip for reproducible builds.
RUN pip install --no-cache-dir --upgrade pip==24.3.1

# Isolated venv we copy verbatim into the runtime stage.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build

# Install Python deps FIRST so Docker can cache this layer across source
# edits — the requirements.txt only changes when we update dependencies.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# curl is needed for the HEALTHCHECK only.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for the runtime process.
RUN useradd --create-home --shell /bin/bash --uid 1000 app

WORKDIR /app

# Bring the prebuilt venv across.
COPY --from=builder /opt/venv /opt/venv

# C01 ships the bare FastAPI shell — src/ has main.py with the health
# endpoint; templates/, static/, config/ are placeholders (kept by
# .gitkeep) so the COPYs succeed now and later commits can drop real
# assets in without touching the Dockerfile.
COPY --chown=app:app src/ /app/src/
COPY --chown=app:app templates/ /app/templates/
COPY --chown=app:app static/ /app/static/
COPY --chown=app:app config/ /app/config/

EXPOSE 8000

# ``start_period: 15s`` gives uvicorn time to bind the port before the
# orchestrator starts counting unhealthy ticks.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

USER app

# ``--workers 1`` is required: APScheduler runs in-process and is only
# safe under a single worker (multi-worker would race on the job store).
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

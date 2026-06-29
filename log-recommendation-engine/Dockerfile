# syntax=docker/dockerfile:1.6
#
# Production runtime image for the Log Recommendation Engine.
#
# A single uvicorn/FastAPI process on :8000 serving the recommendation API. Later
# commits reuse this same image (only the CMD changes via docker-compose) — this
# project has no scheduler/worker, so there is no Celery in the picture. The
# Postgres data + generated datasets live on mounted volumes so they survive
# container restarts.
#
# The embedding model (all-MiniLM-L6-v2) is baked into the image at build time so
# startup is offline and deterministic (no network fetch on first request).
#
# Only the runtime surface is copied in: src/ (the app), config/ (default YAML), and
# scripts/ (CLI helpers + entrypoint). Tests, plan.md, project_requirements.md, and
# other junk are excluded by .dockerignore and never land in the production image.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live), no .pyc files, and PYTHONPATH=/app so
# the `src` package resolves regardless of CWD.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Hugging Face / sentence-transformers cache locations. Pinned under /app so the
# model baked at build time (and its ownership) is predictable and readable by the
# non-root runtime user.
ENV HF_HOME=/app/.cache/hf \
    SENTENCE_TRANSFORMERS_HOME=/app/.cache/st

WORKDIR /app

# Build toolchain so sentence-transformers / scikit-learn / numpy build & link
# (libgomp1 is the OpenMP runtime pulled in transitively), plus curl for the
# HEALTHCHECK. Removed from the layer's apt cache to keep the image lean.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached unless requirements.txt changes).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run as a non-root user (uid 1000).
RUN useradd --uid 1000 --create-home appuser

# Bake the embedding model into the image AFTER pip install so torch +
# sentence-transformers are present. Downloading it here (into HF_HOME /
# SENTENCE_TRANSFORMERS_HOME under /app/.cache) means the container never fetches
# the model at runtime — startup is offline and deterministic. The cache is
# chown'd to appuser below so the non-root runtime user can read it.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# Application code + default config. Nothing else.
COPY src/ ./src/
COPY config/ ./config/

# CLI helpers (e.g. seed / backfill in later commits) + the entrypoint. The whole
# dir is copied so the `scripts` package resolves under WORKDIR /app; entrypoint.sh
# also landing here is harmless since the ENTRYPOINT uses /usr/local/bin/entrypoint.sh.
COPY scripts/ ./scripts/
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Pre-create the volume mountpoints + cache dir in the image and hand everything to
# appuser. Doing this (and especially the recursive chown) BEFORE `USER appuser`
# ensures the baked model cache under /app/.cache is readable at runtime, and that a
# fresh named volume mounted onto /app/data inherits appuser-owned permissions.
RUN mkdir -p /app/data /app/.cache \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits the /health route. /health is dependency-free in this build, so
# the app is healthy as soon as uvicorn binds; the 60s start-period is a safe margin
# for image start-up (and the model load).
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Entrypoint runs `alembic upgrade head` (guarded: only if alembic.ini exists, which
# it does not yet in C1) then exec's the CMD (uvicorn). This makes the schema
# self-applying once migrations land in C2, without breaking the C1 build.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]

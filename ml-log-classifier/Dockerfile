# syntax=docker/dockerfile:1.6
#
# Production runtime image for the ML Log Classifier.
#
# A single uvicorn/FastAPI process on :8000 serving the classification API and the
# live-metrics WebSocket. Trained models and generated data live on mounted volumes
# (/app/models, /app/data) so they survive container restarts.
#
# Only the runtime surface is copied in: src/ (the app) and config/ (default YAML).
# Tests, plan.md, project_requirements.md, and other junk are excluded by
# .dockerignore and never land in the production image. NLTK's punkt + stopwords
# corpora are baked in at build time so inference never reaches out to the network.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live), no .pyc files, PYTHONPATH=/app so the
# `src` package resolves regardless of CWD, and a fixed NLTK_DATA path matching the
# build-time download below.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    NLTK_DATA=/usr/share/nltk_data

WORKDIR /app

# curl only — required by the HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached unless requirements.txt changes).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake the NLTK corpora into the image so preprocessing has no network dependency.
RUN python -m nltk.downloader -d /usr/share/nltk_data punkt stopwords

# Run as a non-root user (uid 1000).
RUN useradd --uid 1000 --create-home appuser

# Application code + default config. Nothing else.
COPY src/ ./src/
COPY config/ ./config/

# Pre-create the volume mountpoints in the image and hand them to appuser. When a
# fresh named volume (models:/app/models, data:/app/data) is first mounted onto an
# existing image directory, Docker initializes it with that directory's ownership —
# so creating them here as appuser-owned means the volumes are writable by the
# non-root user. Without this, Docker creates the mountpoints as root and startup
# crashes trying to persist the trained model (PermissionError on /app/models).
RUN mkdir -p /app/models /app/data

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Liveness probe hits the /health route. start-period gives uvicorn time to load the
# model and bind before failures count against the container. On first boot the app
# trains a model during startup (~10-15s) and does not serve until that completes, so
# the grace period is 60s to avoid a spurious "unhealthy" during first-boot training.
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# src/main.py (the ASGI app) arrives in a later commit. The image still builds fine
# since CMD is only evaluated at container start, not during the build.
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

# syntax=docker/dockerfile:1.6
#
# Production runtime image for the Predictive Log Analytics Engine.
#
# A single uvicorn/FastAPI process on :8000 serving the forecasting API. In later
# commits the same image also runs the Celery worker + beat (different CMD via
# docker-compose). Fitted models and generated data live on mounted volumes
# (/app/models, /app/data) so they survive container restarts.
#
# Only the runtime surface is copied in: src/ (the app) and config/ (default YAML).
# Tests, plan.md, project_requirements.md, and other junk are excluded by
# .dockerignore and never land in the production image.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live), no .pyc files, and PYTHONPATH=/app so
# the `src` package resolves regardless of CWD.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Build toolchain so statsmodels / scikit-learn / xgboost build & link (libgomp1 is
# XGBoost's OpenMP runtime), plus curl for the HEALTHCHECK. Removed from the layer's
# apt cache to keep the image lean.
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

# Application code + default config. Nothing else.
COPY src/ ./src/
COPY config/ ./config/

# Pre-create the volume mountpoints in the image and hand them to appuser. When a
# fresh named volume (models:/app/models, data:/app/data) is first mounted onto an
# existing image directory, Docker initializes it with that directory's ownership —
# so creating them here as appuser-owned means the volumes are writable by the
# non-root user.
RUN mkdir -p /app/models /app/data \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits the /health route. /health is dependency-free in this build, so
# the app is healthy as soon as uvicorn binds; the 60s start-period is a safe margin
# for image start-up (and future engine warm-up).
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]

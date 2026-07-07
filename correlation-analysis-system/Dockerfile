# syntax=docker/dockerfile:1.6
#
# Production runtime image for the Correlation Analysis System.
#
# A single uvicorn/FastAPI process on :8000 serving the correlation API. The whole
# pipeline (generator -> parsers -> collector -> engine) runs inside this one process
# as an asyncio task (from C3), so this is the only application image. Redis runs as
# a separate compose service.
#
# Only the runtime surface is copied in: src/. Tests, plan.md, project_requirements.md
# and other junk are excluded by .dockerignore and never land in the production image.

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

# Application code. Nothing else.
COPY src/ ./src/

# Run as a non-root user (uid 1000).
RUN useradd --uid 1000 --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits the /health route. /health is dependency-free (Redis state is
# reported in the body, never via a non-2xx), so the app is healthy as soon as
# uvicorn binds; 20s start-period is a safe margin for image start-up.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

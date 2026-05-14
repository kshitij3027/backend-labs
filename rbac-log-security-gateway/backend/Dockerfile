# syntax=docker/dockerfile:1.6

# ---------- Stage 1: builder ----------
# Install build deps + create an isolated venv so the runtime stage stays small.
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Pin pip for reproducible builds before any other install.
RUN pip install --no-cache-dir --upgrade pip==24.3.1

# Isolated venv we'll copy verbatim into the runtime stage.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
# Build context is the project root, so requirements.txt is at the root of it.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# curl is needed for HEALTHCHECK only.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for the runtime process.
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Bring the prebuilt venv across.
COPY --from=builder /opt/venv /opt/venv

# Source tree. backend/src/ is .gitkeep-only in C2 — populated in C3.
# Build context is the project root, so the path is prefixed with backend/.
COPY backend/src/ /app/src/

RUN chown -R appuser:appuser /app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:8000/health || exit 1

USER appuser

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

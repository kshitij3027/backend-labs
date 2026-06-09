# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# build-essential + gcc to compile native wheels (mmh3 / bitarray) when no
# manylinux wheel is available for this platform.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-slim AS runtime

# PYTHONPATH=/app so any script entrypoint can import the `src` package
# regardless of CWD/sys.path[0].
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PATH=/home/appuser/.local/bin:$PATH

# curl only — needed by the HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 1000 --shell /bin/bash --create-home appuser

WORKDIR /app

COPY --from=builder /root/.local /home/appuser/.local
# Runtime image ships only the application package — tests and tooling stay in
# Dockerfile.test, which COPYs the whole tree instead.
COPY src/ ./src/
# Dashboard page + assets (vendored Chart.js). ONE image serves both
# processes: compose runs this same image twice — `app` with the default CMD
# below and `dashboard` with `uvicorn src.dashboard:app ... --port 8002` —
# so the dashboard tree ships here too (a few hundred KB, mostly Chart.js).
COPY dashboard/ ./dashboard/

# /app/data backs the persisted filter snapshots (*.bloom); compose bind-mounts
# ./data over it, but the image still works standalone.
RUN mkdir -p /app/data \
    && chown -R appuser:appuser /app /home/appuser/.local

USER appuser

EXPOSE 8001

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS http://localhost:8001/health || exit 1

# SINGLE uvicorn worker, always: bloom-filter state lives in-process, so extra
# workers would each hold a divergent copy of every filter. Never add --workers.
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8001"]

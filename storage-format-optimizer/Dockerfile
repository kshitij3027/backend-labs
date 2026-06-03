# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# build-essential + gcc to compile native wheels (pyarrow / lz4) when no
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
COPY . .

RUN mkdir -p /app/data /app/logs \
    && chown -R appuser:appuser /app /home/appuser/.local

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

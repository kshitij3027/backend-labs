# syntax=docker/dockerfile:1.6
#
# Runtime image for the delta-encoding log engine: a single FastAPI process on
# :8080. Multi-stage so the final image carries only the installed dependencies
# and the `app/` package — no build toolchain, no tests, no pip cache.

FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .
# Install into the per-user site so the whole tree can be copied to the runtime
# stage in one shot (pure-Python deps — no compiler needed).
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-slim AS runtime

# PYTHONPATH=/app so `uvicorn app.main:app` imports the `app` package regardless
# of CWD; PATH includes the per-user bin so the copied `uvicorn` is on PATH.
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
# Runtime image ships only the application package. The dashboard tree does not
# exist yet (added in a later commit), so it is intentionally NOT copied here.
COPY app/ ./app/

RUN chown -R appuser:appuser /app /home/appuser/.local

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS http://localhost:8080/health || exit 1

# SINGLE uvicorn worker (the default): the segment store + metrics live in
# process memory, so extra workers would each diverge. Never add --workers.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

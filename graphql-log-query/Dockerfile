# syntax=docker/dockerfile:1.6
#
# Production runtime image for the GraphQL Log Query Platform.
#
# A single uvicorn/FastAPI process on :8000 serving the entire surface from ONE origin:
# POST /graphql (queries + mutations), WS /graphql (subscriptions), GET /health, GET /metrics,
# and — from C13 — the React SPA at / and /assets/*. Unlike every other service in this repo,
# state does NOT live in this process: log rows live in Postgres and the query cache, persisted
# query documents and cross-worker subscription fan-out all live in Redis. This image is the
# stateless middle.
#
# THIS FILE IS DELIBERATELY SINGLE-STAGE FOR NOW. C13 turns it into a multi-stage build — a
# `node:20-alpine` stage that runs `npm ci && npm run build` over web/, then
# `COPY --from=web /app/dist ./src/web/` into this same python:3.11-slim runtime. That is why
# the runtime layers below are already ordered deps -> source: the Node stage slots in above
# `FROM python:3.11-slim` without disturbing any of it, and the SPA arrives as one extra COPY.
# Serving the SPA from this image (rather than an nginx sidecar, which is the convention in the
# other nine projects here) is what makes the browser's GraphQL calls same-origin: no CORS
# preflight on the HTTP transport and no cross-origin WebSocket upgrade for subscriptions.
#
# Only the runtime surface is copied in: src/ (which gains src/web/ at C13). Tests, scripts,
# plan.md and the spec are excluded by .dockerignore and never reach this image — see
# Dockerfile.test for the tester variant.

FROM python:3.11-slim

# Unbuffered stdout/stderr (logs stream live rather than sitting in a pipe buffer), no .pyc
# files on a read-only-ish layer, and PYTHONPATH=/app so the `src` package resolves no matter
# what CWD uvicorn is launched from.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# curl only, and solely for the HEALTHCHECK below. Every pin in requirements.txt resolves to a
# prebuilt manylinux wheel on python:3.11-slim — asyncpg included, which is the one people
# expect to need a compiler — so there is no build-essential/gcc layer and the image stays small.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Dependencies first, so this layer caches and is only invalidated when requirements.txt itself
# changes. Application edits then rebuild only the thin COPY layer below it.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code only. Later commits only rebuild from this layer down.
COPY src/ ./src/

# Run as a non-root user (uid 1000). The app writes nothing to disk — every byte of state is in
# Postgres or Redis — so it needs no write access beyond its own home.
RUN useradd --uid 1000 --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Liveness probe hits /health, which is deliberately dependency-free: it does NOT touch Postgres
# or Redis, so the container reports healthy as soon as uvicorn binds. That is the whole point —
# compose's `condition: service_healthy` gates the e2e/loadtest harnesses on "the API is
# listening", and a probe that also graded its dependencies would turn a slow first connection
# into a container restart loop. Readiness of the data layer is C2's `init_db` retry loop, which
# is a startup concern, not a liveness one. The 25s start-period covers image start plus that
# retry loop without letting a slow Postgres flap the container.
HEALTHCHECK --interval=10s --timeout=5s --start-period=25s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# The container ALWAYS binds :8000; the host-side port is a compose concern
# ("${API_PORT:-8020}:8000"), which is why HOST/PORT are informational inside the app.
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

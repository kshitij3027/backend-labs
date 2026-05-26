# syntax=docker/dockerfile:1.6
#
# Two-stage build for the Multi-Framework Compliance Reporting Engine.
#
# Stage 1 (builder) installs Python deps with the compilers and libpq
# headers asyncpg + psycopg2-binary need at install time. Stage 2
# (runtime) is a slim image that carries only the prebuilt site-packages
# plus libpq5 (the runtime shared library) -- no compilers, no caches.
#
# Layout assumes ``src/``, ``templates/``, and ``static/`` exist at the
# build context root. Commit 1 populates ``templates/`` and ``static/``
# with ``.gitkeep`` placeholders so the COPYs succeed before any
# Jinja/CSS assets are written.

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# pip install --user installs into /root/.local, generating console-script
# wrappers under /root/.local/bin (so uvicorn et al. are real executables,
# not just importable modules). We copy that whole tree into the runtime
# image and put /home/appuser/.local/bin on PATH below.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --no-cache-dir -r /tmp/requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# libpq5 is the runtime shared library asyncpg + psycopg2-binary load;
# curl is needed for the HEALTHCHECK probe.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user. Fixed UID keeps bind-mount permission auditing
# deterministic across hosts.
RUN useradd --create-home --shell /bin/bash --uid 1000 appuser

# Copy the prebuilt user-site packages from the builder stage and put
# their console scripts on PATH so uvicorn / pytest / alembic resolve.
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local
ENV PATH=/home/appuser/.local/bin:$PATH

WORKDIR /app

COPY --chown=appuser:appuser src ./src
COPY --chown=appuser:appuser templates ./templates
COPY --chown=appuser:appuser static ./static

USER appuser
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

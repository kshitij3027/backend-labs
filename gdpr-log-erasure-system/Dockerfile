# syntax=docker/dockerfile:1.6
#
# Two-stage build for the GDPR Log Erasure System.
#
# Stage 1 (builder) installs Python deps with the compilers and libpq
# headers asyncpg needs to build its C extensions. Stage 2 (runtime)
# is a slim image that carries only the prebuilt user-site packages
# plus libpq5 (the runtime shared library) — no compilers, no caches.
#
# Layout assumes ``src/``, ``templates/``, and ``static/`` exist at the
# build context root. C1 populates ``templates/`` and ``static/`` with
# ``.gitkeep`` placeholders so the COPYs succeed before any HTML/CSS
# assets are written.

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# build-essential + gcc for any C extensions; libpq-dev so asyncpg can
# link against the PostgreSQL client headers at install time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy ONLY requirements.txt first so the pip install layer is reused
# across source edits — invalidated only when deps change.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --no-cache-dir -r /tmp/requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# libpq5 is the runtime shared library asyncpg needs; the -dev package
# and the compilers are intentionally NOT pulled into the runtime image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user. Using a fixed UID makes bind-mount permission auditing
# deterministic across hosts.
RUN useradd --create-home --shell /bin/bash --uid 1000 appuser

# Copy the prebuilt user-site packages from the builder stage and put
# their console scripts on PATH.
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local
ENV PATH=/home/appuser/.local/bin:$PATH

WORKDIR /app

# Source + template/static placeholders. ``templates/`` and ``static/``
# carry ``.gitkeep`` files so the COPY succeeds before any real assets
# exist; later commits drop real files in without touching this Dockerfile.
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser templates/ /app/templates/
COPY --chown=appuser:appuser static/ /app/static/

USER appuser
EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

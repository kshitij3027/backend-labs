# syntax=docker/dockerfile:1.6

# ---------- Stage 1: builder ----------
# Build deps + the spaCy ``en_core_web_sm`` model are installed into an
# isolated venv. Baking the model into the image (rather than downloading
# it at runtime) means cold-start time is constant and the container can
# boot in environments without outbound network access.
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Pin pip for reproducible builds.
RUN pip install --no-cache-dir --upgrade pip==24.3.1

# Isolated venv we copy verbatim into the runtime stage.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build

# Install Python deps FIRST so Docker can cache this layer across source
# edits — the requirements.txt only changes when we update dependencies.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-bake the spaCy NER model into the venv. C2 onwards needs this; doing
# it here (in the builder) keeps the model artifact in the same layer as
# the venv so the runtime copy is one shot. The ~12 MB model download adds
# a few seconds to first build but is cached by Docker on subsequent builds.
RUN python -m spacy download en_core_web_sm

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
RUN useradd --create-home --shell /bin/bash --uid 1000 app

WORKDIR /app

# Bring the prebuilt venv (including spaCy + the en_core_web_sm model) across.
COPY --from=builder /opt/venv /opt/venv

# C1 only ships ``src/``. ``config/``, ``templates/``, and ``static/`` are
# added by their respective later commits (C2 for config, C8 for the
# dashboard assets). Splitting the COPYs across commits keeps the layer
# diff small and lets each commit be reviewed independently.
COPY --chown=app:app src/ /app/src/
COPY --chown=app:app config/ /app/config/
COPY --chown=app:app templates/ /app/templates/
COPY --chown=app:app static/ /app/static/

EXPOSE 8000

# ``start_period: 15s`` gives uvicorn time to bind the port and load the
# spaCy model before the orchestrator starts counting unhealthy ticks.
HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=15s \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

USER app

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

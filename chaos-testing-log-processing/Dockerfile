# syntax=docker/dockerfile:1.6

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Isolated venv so we can copy it into the runtime stage cleanly.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# curl is required for the HEALTHCHECK directive below.
# ca-certificates lets the Docker SDK / httpx talk to TLS endpoints if needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user (uid 1000) — keeps the framework off-root even though it
# still needs the Docker socket mounted by compose for fault injection.
RUN groupadd --system --gid 1000 chaos \
 && useradd  --system --uid 1000 --gid chaos --shell /usr/sbin/nologin --create-home chaos

WORKDIR /app

# Bring the prebuilt venv across.
COPY --from=builder /opt/venv /opt/venv

# Source + config. config/ is intentionally empty in C1 — populated in C3.
RUN mkdir -p /app/config /app/data \
 && chown -R chaos:chaos /app
COPY --chown=chaos:chaos src/ /app/src/
COPY --chown=chaos:chaos config/ /app/config/

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

USER chaos

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

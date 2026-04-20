## Multi-stage build for the real-time log indexing engine.
##
## Stage 1 installs wheels into /install so only the resolved runtime
## artifacts (no compiler toolchain, no pip cache) flow into the
## final image. Stage 2 copies those wheels on top of a clean
## python:3.12-slim layer and runs as a non-root user.

# ------------------------------ builder ------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ------------------------------ runtime ------------------------------
FROM python:3.12-slim AS runtime

# curl is used by the compose healthcheck and by start.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root execution — matches the reference images in the repo.
RUN useradd --create-home --uid 1000 app

WORKDIR /app

# Carry over installed site-packages + console scripts from builder.
COPY --from=builder /install /usr/local

# App source.
COPY src/ /app/src/
COPY static/ /app/static/
COPY templates/ /app/templates/

# Segment persistence directory. Owned by the app user so the
# process can write without root privileges.
RUN mkdir -p /data/segments && chown -R app:app /app /data

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

USER app

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]

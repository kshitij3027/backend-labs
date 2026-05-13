# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

# Fault-injection toolchain (iproute2 for `tc netem`, stress-ng for CPU/mem
# pressure, iptables for finer-grained partitions) plus curl for HEALTHCHECK
# and ca-certificates for TLS. Container intentionally runs as root so that
# future commits (C7-C9) can `docker exec` into it with NET_ADMIN cap and
# manipulate qdiscs without setcap gymnastics.
RUN apt-get update && apt-get install -y --no-install-recommends \
        iproute2 \
        stress-ng \
        iptables \
        procps \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# NOTE: deliberately no USER directive — see comment above.

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

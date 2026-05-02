# Multi-Region Log Replication Engine

A simulated multi-region distributed log store that replicates writes from a primary region to secondary regions using vector clocks for causal ordering and deterministic conflict resolution.

## Tech Stack

- Python 3.11
- FastAPI + Uvicorn
- Pydantic v2
- Vue 3 + Tailwind (CDN, raw HTML)
- Docker + Docker Compose

## Run

```bash
make build   # build the app image
make test    # run unit tests inside Docker
make run     # docker compose up the full stack (app + redis)
```

> Status: scaffold only (commit 1). The HTTP API, replication path, dashboard, and E2E scripts land in subsequent commits. See `plan.md` for the full commit plan.

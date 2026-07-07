# Correlation Analysis System

A real-time engine that ingests logs from multiple sources and automatically detects temporal,
session-based, error-cascade, and metric correlations between events, exposed through a REST API
and a live React dashboard.

## Tech Stack
- Language: Python
- Backend: FastAPI
- Frontend: React (dashboard)
- Cache / pub-sub: Redis
- Testing: pytest

## Architecture

Long-lived server processes running together:

| Service          | Port | Role                                  |
|-------------------|------|----------------------------------------|
| FastAPI backend   | 8000 | Log ingestion, correlation engine, REST API |
| React dashboard   | 3000 | Live visualization of detected correlations |
| Redis             | —    | Shared state / pub-sub between ingestion and detection |

Correlation types detected:
- **Temporal** — events clustering in time windows
- **Session-based** — events linked by a shared session/request ID
- **Error-cascade** — chains of failures triggered by an upstream error
- **Metric** — statistical correlation between numeric metrics over time

## How to Run

```bash
./start.sh
# or
docker-compose up
```

This brings up the FastAPI backend (`:8000`), the React dashboard (`:3000`), and Redis together.
Interact via the dashboard in a browser or directly through the REST API.

*(Startup scripts and service implementations are not yet built — scaffold only.)*

## What I Learned
<!-- Fill in as the project evolves -->

## API Docs / Usage
<!-- Fill in as endpoints are implemented -->

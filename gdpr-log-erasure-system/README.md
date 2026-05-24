# GDPR Log Erasure System

A backend service that processes user data erasure requests across a distributed log processing system, supporting **selective deletion vs anonymization** with a full **audit trail** for compliance (GDPR Article 17 — "right to erasure").

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize the **register → request → process → audit** lifecycle of a data-subject erasure request as it appears in real log/observability pipelines that fall under GDPR.

## How it runs

A long-lived FastAPI server, backed by PostgreSQL (durable state: data locations, erasure requests, audit entries) and Redis (in-flight job queue + status cache). Users interact via HTTP endpoints to:

1. **Register** where a given user's data lives across the log system (which topics, indices, tables, files).
2. **Submit** an erasure request for a user — choosing **delete** or **anonymize**.
3. **Monitor** the request as it is processed across registered locations.
4. **Pull** compliance and audit reports proving what was erased, when, and by whom.

The whole stack runs via **Docker Compose** on a single host.

| Service | Purpose |
|---|---|
| Backend | FastAPI + uvicorn. REST API for registration, erasure requests, status, audit reports. |
| PostgreSQL | Durable store for data-location registry, erasure-request lifecycle, and immutable audit log. |
| Redis | Job queue for in-flight erasure work + short-TTL cache of request status. |

## Tech stack

- **Language:** Python 3.11
- **Framework:** FastAPI + uvicorn
- **Database:** PostgreSQL (via SQLAlchemy async + asyncpg, migrations via Alembic)
- **Cache / queue:** Redis (async client)
- **Containers:** Docker + Docker Compose
- **Testing:** pytest + pytest-asyncio + httpx

## How to Run

_To be filled in once the service is built._

## API endpoints

_To be filled in once the API is implemented. Expected surface:_

- `POST /api/data-locations` — register where a user's data lives (topic / index / table / path).
- `GET  /api/data-locations/{user_id}` — list all registered locations for a user.
- `POST /api/erasure-requests` — submit a new erasure request (`mode: delete | anonymize`).
- `GET  /api/erasure-requests/{id}` — fetch lifecycle state of a single request.
- `GET  /api/erasure-requests/{id}/status` — fine-grained per-location processing status.
- `GET  /api/audit/requests` — paginated audit entries for compliance reviewers.
- `GET  /api/audit/reports/{request_id}` — full audit report for a single erasure request.
- `GET  /health` — liveness probe.
- `GET  /docs` — auto-generated OpenAPI / Swagger.

## What I learned

_To be filled in as the project evolves._

## Layout

_To be filled in once the source tree exists._

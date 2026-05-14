# RBAC Log Security Gateway

A security middleware that controls who can access what log data in a distributed log processing system, using JWT authentication, role-based authorization, and full audit logging.

## Overview

This gateway sits in front of a log query backend and enforces:

- **Authentication** — every request must carry a valid JWT (issued by the gateway's `/auth/login` endpoint or a trusted external IdP).
- **Authorization** — each principal (user or service) is bound to one or more **roles**, and each role grants a scoped set of **permissions** on log resources (e.g., `logs:read:app=billing`, `logs:read:env=prod`, `logs:admin:*`).
- **Audit** — every authentication attempt, authorization decision (allow / deny), and log query is recorded to an immutable audit log with the caller identity, requested resource, decision, and reason.

It is intended to be deployed in front of any log query API (Elasticsearch, Loki, an in-house service, etc.) so that no consumer talks to the log backend directly.

## How It Runs

- A **long-lived FastAPI server** on **port 8000** exposes:
  - The auth endpoints (`/auth/login`, `/auth/refresh`, `/auth/logout`).
  - The admin endpoints for managing users, roles, and permissions.
  - The protected log query endpoints (`/logs/search`, `/logs/{id}`, `/logs/stream`), which the gateway authorizes and then proxies to the configured log backend.
  - The audit query endpoints (`/audit/events`, `/audit/events/{id}`) for compliance / forensic review.
- An **optional React frontend** on **port 3000** provides:
  - A login screen.
  - An admin UI for users / roles / permissions.
  - A log explorer that calls the gateway's protected endpoints.
  - An audit timeline view.
- Users and services authenticate over HTTP, receive a short-lived JWT, and then make authorized log queries through the same API. The gateway evaluates every request against the caller's role bindings before it touches the backend.

## Tech Stack

- **Language:** Python 3.11+
- **Backend:** FastAPI + Uvicorn
- **AuthN:** JWT (HS256 for dev, RS256 for prod), `python-jose`
- **Password hashing:** `passlib[bcrypt]`
- **AuthZ:** In-process policy engine (role → permission rules), pluggable
- **Persistence:** SQLite (via SQLAlchemy + `aiosqlite`) for users, roles, permissions, refresh tokens, and audit events
- **Validation / config:** Pydantic v2 + `pydantic-settings`
- **Observability:** `prometheus-client` + `structlog` (JSON logs)
- **HTTP client (backend proxy):** `httpx`
- **Frontend (optional):** React (Vite) on port 3000
- **Testing:** Pytest, pytest-asyncio, httpx test client

## How to Run

_TBD — implementation not started yet. This README documents the intended shape of the project; runnable instructions will be filled in once the scaffold lands._

## Planned API

| Method | Path                              | Auth required | Purpose                                                                 |
| ------ | --------------------------------- | ------------- | ----------------------------------------------------------------------- |
| POST   | `/auth/login`                     | No            | Exchange username + password for an access + refresh JWT                |
| POST   | `/auth/refresh`                   | Refresh JWT   | Mint a new access token from a refresh token                            |
| POST   | `/auth/logout`                    | Access JWT    | Revoke the current refresh token                                        |
| GET    | `/auth/me`                        | Access JWT    | Return the caller's identity, roles, and effective permissions          |
| POST   | `/admin/users`                    | `admin:users` | Create a user / service principal                                       |
| GET    | `/admin/users`                    | `admin:users` | List principals                                                         |
| POST   | `/admin/roles`                    | `admin:roles` | Create a role                                                           |
| POST   | `/admin/roles/{role}/permissions` | `admin:roles` | Attach permissions to a role                                            |
| POST   | `/admin/users/{user}/roles`       | `admin:users` | Bind a role to a user                                                   |
| POST   | `/logs/search`                    | `logs:read`   | Authorized search against the log backend (gateway filters by scope)    |
| GET    | `/logs/{id}`                      | `logs:read`   | Fetch a single log record (subject to per-resource scope)               |
| GET    | `/logs/stream`                    | `logs:read`   | Server-sent stream of newly indexed logs the caller is permitted to see |
| GET    | `/audit/events`                   | `audit:read`  | Query the audit log (filters: principal, decision, resource, time)      |
| GET    | `/audit/events/{id}`              | `audit:read`  | Fetch a single audit event                                              |
| GET    | `/health`                         | No            | Liveness / readiness                                                    |
| GET    | `/metrics`                        | No            | Prometheus scrape endpoint                                              |

Full OpenAPI docs will be available at `http://localhost:8000/docs` once the stack is up.

## Authorization Model (Planned)

- **Principal** — a user or service account, identified by a stable subject (`sub`) claim in the JWT.
- **Role** — a named bundle of permissions (e.g., `viewer-billing`, `oncall-prod`, `auditor`).
- **Permission** — a tuple of `(action, resource_pattern)`, where:
  - `action` is one of `logs:read`, `logs:admin`, `audit:read`, `admin:users`, `admin:roles`.
  - `resource_pattern` is a constrained selector over log attributes (`app`, `env`, `region`, `tenant`, etc.) with `*` wildcards.
- **Decision** — for each request, the policy engine evaluates the caller's effective permissions against the requested resource. Both the decision (`allow` / `deny`) and the matching rule (or the lack of one) are persisted to the audit log.
- **Deny by default** — no permission, no access.

## Audit Model (Planned)

Every event records:

- `event_id` (UUID), `timestamp` (UTC, monotonic-clock corroborated)
- `principal_sub`, `principal_kind` (`user` / `service`)
- `action` (e.g., `auth.login`, `authz.decision`, `logs.search`)
- `resource` (the requested resource pattern, when applicable)
- `decision` (`allow` / `deny` / `n/a`)
- `reason` (matched rule, missing permission, expired token, bad signature, …)
- `request_id`, `source_ip`, `user_agent`
- `latency_ms`

Audit events are append-only and queryable via `/audit/events`. They are **never** mutated or deleted by the API.

## What I Learned

_To be filled in as the project evolves._

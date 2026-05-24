# Automated Log Retention

A policy-driven retention system that automatically manages log lifecycle (archive, compress, delete) across configurable storage tiers with compliance validation and audit trails.

## Tech Stack
- **Language**: Python 3.11+
- **Web Framework**: FastAPI (REST API on port 8000)
- **Scheduling**: APScheduler (background jobs for periodic policy evaluation)
- **Dashboard**: Jinja2 templates + HTMX (server-rendered web UI for monitoring retention status)
- **Storage**: SQLite (policy/audit metadata) + local filesystem tiers (hot / warm / cold / archive)
- **Compression**: gzip / zstandard for archive tier
- **Validation**: Pydantic v2 (policy schemas, request/response models)
- **Testing**: pytest, httpx (API tests), freezegun (time-travel for scheduler tests)

## What It Does

The system enforces configurable retention policies over log data:

1. **Policy definition** — declarative rules (e.g., "logs in `app/auth` older than 7 days → compress and move to warm tier; older than 90 days → archive; older than 365 days → delete") with compliance tags (SOC 2, PCI DSS, GDPR).
2. **Tiered storage lifecycle** — logs move through hot → warm → cold → archive tiers based on age and access patterns; each tier has its own compression and retention rules.
3. **Scheduled evaluation** — background scheduler periodically (e.g., hourly) walks the catalog, evaluates each log batch against active policies, and executes lifecycle actions.
4. **Compliance validation** — pre-flight check on every delete/archive action: blocks operations that would violate a retention minimum (e.g., "must retain ≥ 7 years for PCI"), and emits a compliance report.
5. **Audit trail** — every action (archive, compress, delete, policy change) is logged to an immutable audit table with actor, timestamp, before/after state, and policy reference — queryable via the API and dashboard.
6. **Dashboard** — web UI showing per-tier storage usage, upcoming actions, policy violations, recent audit events, and compliance status.

## How It Runs

- **Long-lived FastAPI server** on port `8000` exposing REST endpoints for policies, logs, audit queries, and compliance reports.
- **Scheduled background jobs** running inside the same process (APScheduler) for periodic policy evaluation and tier transitions.
- **Web dashboard** served from the same FastAPI process at `/` for monitoring retention status, tier breakdown, and audit history.

## How to Run
<!-- To be filled in once implementation begins -->

## API (planned)
<!-- To be filled in once routes are implemented -->

- `POST /policies` — create a retention policy
- `GET /policies` — list active policies
- `GET /logs` — query the log catalog (filter by tier, age, tag)
- `POST /evaluate` — manually trigger a policy-evaluation pass
- `GET /audit` — query the audit trail
- `GET /compliance/report` — generate a compliance status report
- `GET /` — dashboard

## What I Learned
<!-- To be filled in as the project evolves -->

# GDPR Log Erasure System

A backend service that processes user data erasure requests across a distributed log processing system, supporting **selective deletion vs anonymization** with a full **audit trail** for compliance (GDPR Article 17 — "right to erasure").

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize the **register → request → process → audit** lifecycle of a data-subject erasure request as it appears in real log/observability pipelines that fall under GDPR.

## How it runs

A long-lived FastAPI server, backed by PostgreSQL (durable state: data locations, erasure requests, audit entries) and Redis (health-probed; reserved for future cache/queue work). Users interact via HTTP endpoints to:

1. **Register** where a given user's data lives across the log system (which topics, indices, tables, files).
2. **Submit** an erasure request for a user — choosing **delete** or **anonymize**.
3. **Monitor** the request as it is processed across registered locations.
4. **Pull** compliance and audit reports proving what was erased, when, and by whom.

The whole stack runs via **Docker Compose** on a single host.

| Service | Purpose |
|---|---|
| Backend | FastAPI + uvicorn. REST API + HTMX compliance dashboard. |
| PostgreSQL 16 | Durable store for data-location registry, erasure-request lifecycle, and immutable audit log. |
| Redis 7 | Health-probed; reserved for cache/queue work. |

## Tech stack

- **Language:** Python 3.11
- **Framework:** FastAPI 0.115 + uvicorn
- **Database:** PostgreSQL 16 (via SQLAlchemy 2.0 async + asyncpg)
- **Cache / health-probed:** Redis 7
- **Templating:** Jinja2 + vendored HTMX 1.9.12 (polling partials)
- **Containers:** Docker + Docker Compose
- **Testing:** pytest + pytest-asyncio + httpx (>80 unit/integration tests)
- **Audit chain:** SHA-256 hash-chained, append-only

## How to Run

```bash
# from gdpr-log-erasure-system/
make build       # build images
make up          # start postgres + redis + app (detached)
make seed        # populate ~50 users with 3-5 mappings each
open http://localhost:8000/        # HTMX dashboard
curl http://localhost:8000/health  # liveness + db_ok + redis_ok
curl http://localhost:8000/api/statistics
```

To run the full happy-path probe:
```bash
make e2e
```

To run a 100-request load test:
```bash
make load
```

To run the test suite inside Docker:
```bash
make test        # full suite
make test-unit
make test-int
```

To stop and clean up:
```bash
make down        # stop containers
make clean       # stop + remove volumes + prune images
```

## API endpoints

| Method | Path | Body | Response |
|---|---|---|---|
| `GET`  | `/`                                  | —                                                           | HTMX dashboard HTML |
| `GET`  | `/partials/{stats,requests,completed,audit}` | —                                                  | HTML fragment for dashboard polling |
| `GET`  | `/health`                            | —                                                           | `{"status":"ok","db_ok":true,"redis_ok":true}` |
| `POST` | `/api/user-data-tracking`            | `{user_id, data_type, storage_location, data_path?, metadata?}` | `201` — registered mapping (idempotent on the unique tuple) |
| `GET`  | `/api/data-locations/{user_id}`      | —                                                           | `200` — list of mappings for the user |
| `POST` | `/api/erasure-requests`              | `{user_id, request_type: "DELETE" \| "ANONYMIZE"}`           | `202` — request id + initial state + audit timeline; coordinator runs via BackgroundTasks |
| `GET`  | `/api/erasure-requests/{id}`         | —                                                           | `200` — full lifecycle state + full audit timeline (404 if missing) |
| `GET`  | `/api/statistics`                    | —                                                           | `200` — `{total_mappings, unique_users, completion_rate, data_type_counts}` |
| `GET`  | `/docs`                              | —                                                           | Auto-generated OpenAPI / Swagger |

## Architecture

State machine driven by `ErasureCoordinator.process(request_id)`:

```
  ┌─────────┐   ┌────────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
  │ PENDING │──>│DISCOVERING │──>│ EXECUTING │──>│ VERIFYING │──>│ COMPLETED │
  └─────────┘   └────────────┘   └───────────┘   └───────────┘   └───────────┘
        │              │               │               │
        │              │               │               │
        └──────────────┴───────────────┴───────────────┴──────> FAILED
```

- **DISCOVERING** loads every `UserDataMapping` for the request's user.
- **EXECUTING** runs per-location erasure in parallel via `asyncio.gather` bounded by `asyncio.Semaphore(MAX_PARALLEL_LOCATION_ERASURES)`. Each location is either:
  - **DELETE** — the mapping row is removed.
  - **ANONYMIZE** — identifiers are hashed (salted SHA-256, 128-bit), IPs are masked (IPv4 /24, IPv6 /48), and the row is kept with an `_anonymized: true` marker. Falls back to DELETE for data_types not in the allowlist (`ANONYMIZABLE_DATA_TYPES`).
- **VERIFYING** confirms each location's outcome (row gone for DELETE, marker present for ANONYMIZE). Disable with `VERIFICATION_ENABLED=false`.
- Every state transition + per-location action is appended to `erasure_audit_log`. Each row commits the previous row's hash; tampering is detected by `src/audit/verifier.py::verify_chain`.
- Concurrent appenders are serialised by `pg_advisory_xact_lock` on Postgres and a per-event-loop `asyncio.Lock` on SQLite (tests).

## Configuration

All settings are read from environment variables. Defaults are in `src/settings.py`.

| Env var | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://erasure_user:changeme@postgres:5432/gdpr_erasure` | Async Postgres DSN |
| `REDIS_URL` | `redis://redis:6379/0` | Redis client target (health-probed) |
| `API_HOST` | `0.0.0.0` | uvicorn bind host |
| `API_PORT` | `8000` | uvicorn bind port |
| `ANONYMIZABLE_DATA_TYPES` | `analytics_events,performance_metrics,system_logs,aggregated_data` | CSV of data_types eligible for ANONYMIZE; everything else falls back to DELETE |
| `ANONYMIZATION_HASH_SALT` | `change-me-in-production` | Salt for identifier hashing (must be set in prod) |
| `MAX_PARALLEL_LOCATION_ERASURES` | `10` | Semaphore size for per-location work |
| `ERASURE_RETRY_COUNT` | `3` | Per-location tenacity retry attempts |
| `ERASURE_RETRY_BACKOFF_SECONDS` | `2` | Exponential backoff base |
| `CORS_ALLOWED_ORIGINS` | `*` | CSV or `*` |
| `LOG_LEVEL` | `INFO` | structlog level |
| `VERIFICATION_ENABLED` | `true` | If false, EXECUTING → COMPLETED without the VERIFYING phase |
| `DASHBOARD_REFRESH_MS` | `5000` | HTMX poll interval (ms) |

## Testing

The suite runs inside the `tester` profile of `docker-compose.yml` — no host-Python required.

```bash
make test         # unit + integration (~83 tests)
make test-unit    # ~63 unit tests
make test-int     # ~20 integration tests + 1 postgres concurrent test
make e2e          # happy-path probe (bash script via tester)
make load         # 100 concurrent erasure requests; asserts success_rate >= 0.99
```

Highlights:
- `test_audit_chain_concurrent.py` — 10 parallel appenders against real Postgres, must produce contiguous sequences (passes via `pg_advisory_xact_lock`).
- `test_anonymization.py::test_hash_identifier_zero_reidentification_risk` — 100-user inverse-lookup check enforcing GDPR Recital 26 ("irreversible identification" requirement).
- `test_erasure_flow.py` — full track → request → poll → COMPLETED + chain verify on every commit.

## What I learned

- **Erasure is a distributed-systems problem, not a `DELETE`.** Real systems have replicas, caches, immutable logs, and downstream consumers — a single SQL DELETE rarely satisfies Article 17. Modelling it as a state machine with explicit DISCOVERING / EXECUTING / VERIFYING phases is the right abstraction.
- **Audit logs must survive erasure.** The audit chain itself can't store personal data (otherwise it becomes a re-identification side channel). Storing only `(request_id, user_id_hash, event_type)` keeps the trail regulator-defensible without leaking the very identifiers it's meant to prove removal of.
- **`SELECT ... FOR UPDATE` is not enough to serialise inserts.** Concurrent transactions can each lock the "current max" row and compute the same next sequence. `pg_advisory_xact_lock` (or a table-level lock) is needed for correctness on hash-chain appenders.
- **`asyncio.Lock` declared at module scope binds to the first event loop that touches it.** pytest-asyncio gives each test a fresh loop, so a module-level lock breaks the second test. A per-loop registry (`dict[loop, Lock]`) is the safe pattern.
- **HTMX polling beats a SPA for small dashboards.** Four cards + polling partial endpoints, all server-rendered with Jinja2 — no Node toolchain, no build step, and the same Docker stack serves the UI.

## Layout

```
gdpr-log-erasure-system/
├── Dockerfile               # two-stage builder + runtime
├── Dockerfile.test          # test runner image
├── docker-compose.yml       # postgres + redis + app + tester (profile: test)
├── Makefile                 # build, up, down, logs, test, test-unit, test-int, seed, e2e, load, clean
├── requirements.txt
├── pytest.ini               # asyncio_mode = auto
├── .env.example             # all settings keys
├── README.md
├── src/
│   ├── main.py              # FastAPI app + lifespan + routes + /health
│   ├── settings.py          # Pydantic BaseSettings
│   ├── logging_config.py    # structlog JSON renderer
│   ├── persistence/
│   │   ├── db.py            # make_engine, make_session_factory, init_db (+ genesis seed)
│   │   └── models.py        # UserDataMapping, ErasureRequest, ErasureAuditLog
│   ├── audit/
│   │   ├── chain.py         # canonical hashing + concurrent-safe append_audit_entry
│   │   └── verifier.py      # verify_chain integrity replay
│   ├── erasure/
│   │   ├── anonymization.py # hash_identifier, mask_ip, decide_action
│   │   ├── state_machine.py # RequestState transition guard
│   │   ├── executor.py      # per-location DELETE/ANONYMIZE with tenacity retry
│   │   ├── verifier.py      # post-erasure verification
│   │   └── coordinator.py   # ErasureCoordinator.process — state-machine driver
│   ├── services/
│   │   └── stats_service.py # compute_statistics
│   └── api/
│       ├── dependencies.py  # get_session
│       ├── schemas.py       # Pydantic request/response models
│       ├── routes_tracking.py
│       ├── routes_erasure.py
│       ├── routes_stats.py
│       └── routes_dashboard.py
├── templates/
│   ├── dashboard.html
│   ├── _stats_card.html
│   ├── _requests_card.html
│   ├── _completed_card.html
│   └── _audit_card.html
├── static/
│   ├── dashboard.css
│   └── htmx.min.js          # HTMX 1.9.12, vendored
├── scripts/
│   ├── seed_demo.py
│   ├── e2e.sh
│   └── load_test.py
└── tests/
    ├── conftest.py          # in-memory sqlite engine + session_factory fixtures
    ├── unit/                # ~63 tests
    └── integration/         # ~20 tests (incl. postgres-gated concurrent chain test)
```

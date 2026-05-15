# RBAC Log Security Gateway

A security middleware that controls **who can access what log data** in a distributed log processing system, using **JWT authentication**, **role-based authorization** with a deny-overrides permission DSL, and full **audit logging**.

Built as a learning exercise in the `backend-labs` mono-repo. The point is to internalize the **auth → authz → audit** pattern as it appears in real log/observability pipelines.

## How it runs

| Service | URL | What it is |
|---|---|---|
| Backend | http://localhost:8000 | FastAPI + uvicorn. JWT auth, RBAC engine, audit middleware. |
| Backend API docs | http://localhost:8000/docs | Auto-generated OpenAPI / Swagger. |
| Frontend | http://localhost:3000 | React + Vite SPA served by nginx; `/api/*` reverse-proxied to the backend. |
| Tester | (no port) | One-shot container that runs the pytest suite (`docker compose run --rm tester pytest`). |

The whole stack is **in-memory and single-host** — no database, no Redis, no external IdP. The 4 demo users live in the source code and bcrypt hashes are computed at import time. The audit log is an append-only Python list shared across requests via module-level singletons in `backend/src/shared.py`.

## Quick start

```bash
# Optional: pre-set a JWT secret; otherwise start.sh generates one.
export JWT_SECRET_KEY="any-long-random-string"

# Bring up the stack (auto-generates .env if missing, then docker compose up --wait).
make demo

# Open http://localhost:3000 and log in as any demo user (see below).
# When done:
make down
```

The first build is slow (~3-5 minutes — pip install + npm install + vite build). Subsequent builds are cached.

## Demo users

| Username | Password    | Role          | Default scope    | What's interesting |
|----------|-------------|---------------|------------------|--------------------|
| `alice`  | `admin123`  | administrator | `*`              | Reads everything; the single deny `!logs:export:business.financial` shows up as 403 on the export endpoint. |
| `bob`    | `dev123`    | developer     | `application`    | Application-scoped; business reads return 403 via `!logs:read:business.*`. |
| `carol`  | `analyst123`| analyst       | `business`       | Sees only **aggregated** counts on `business.*` reads (the `aggregated_only` permission tag). |
| `dave`   | `support123`| support       | `application.auth` | Sees individual records but with **PII masked** to `***` (the `mask_pii` tag). |

## Tech stack

- **Backend:** Python 3.11, FastAPI, uvicorn, pydantic-settings, python-jose (JWT, HS256), passlib[bcrypt], structlog, pytest + pytest-asyncio + httpx.
- **Frontend:** React 18, Vite, react-router-dom v6, axios. Served in production by nginx with `/api` reverse-proxied to the backend.
- **Containers:** multi-stage Dockerfiles for backend (python:3.11-slim) and frontend (node:20-alpine builder → nginx:1.27-alpine serve). Compose orchestrates `backend`, `frontend`, and `tester` services.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Browser → http://localhost:3000  (React via nginx)        │
│      │                                                      │
│      │   /api/* and /health proxied to backend:8000         │
│      ▼                                                      │
│  ┌────────────────────────────────────────────────────┐    │
│  │  FastAPI on backend:8000                           │    │
│  │                                                     │    │
│  │   AuditMiddleware  ← runs first, captures every    │    │
│  │     │                request + 401/403 event       │    │
│  │   Routers:                                          │    │
│  │     /api/auth/{login,profile}                       │    │
│  │     /api/logs/{search,export}      (RBAC-gated)    │    │
│  │     /api/admin/*                   (admin-only)    │    │
│  │     /health, /docs                                  │    │
│  │                                                     │    │
│  │   Module-level singletons in shared.py:            │    │
│  │     auth_service  (bcrypt verify + JWT)            │    │
│  │     rbac_engine   (matcher + role policies)        │    │
│  │     audit_service (append-only list)               │    │
│  └────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

## Permission DSL

Permissions look like `logs:<action>:<resource>` (locked — not the generic `read:resource` of most RBAC tutorials).

- **Actions:** `read`, `export`, `audit`, `admin`.
- **Resources (8 leaves):** `application.{auth,api,worker}`, `business.{metrics,financial,customer}`, `system.{kernel,audit}`.
- **Wildcards:** `*` and `?` via `fnmatch.fnmatchcase`.
- **Deny rules:** prefix with `!`. Denies are **evaluated first** and any match short-circuits the decision.

Examples:

```
logs:read:application.*       # allow reading any application.* resource
!logs:export:business.financial  # deny export of business.financial (overrides admin's logs:export:*)
logs:read:business.*          # with tag "aggregated_only" → analyst-style reduced view
```

Each role's full permission list (administrator / developer / analyst / support) lives in `backend/src/rbac/roles.py`, and the matcher is in `backend/src/rbac/permissions.py`. The 64-cell parametrized regression test in `backend/tests/integration/test_rbac_matrix.py` locks the role table — break a string in `roles.py` and that test will tell you exactly which cell flipped.

## API endpoints

| Method | Path                                | Auth          | Purpose |
|--------|-------------------------------------|---------------|---------|
| POST   | `/api/auth/login`                   | none          | Username + password → JWT access token + user info. |
| GET    | `/api/auth/profile`                 | bearer JWT    | Caller's identity, roles, display name. |
| GET    | `/api/logs/search?resource=&limit=` | bearer JWT    | Search logs; RBAC-gated on `logs:read:<resource>`. Honors `aggregated_only` and `mask_pii` tags. |
| GET    | `/api/logs/export?resource=&limit=` | bearer JWT    | Export logs; RBAC-gated on `logs:export:<resource>`. |
| GET    | `/api/admin/audit-summary`          | admin role    | Aggregate counts of audit entries. |
| GET    | `/api/admin/audit-entries`          | admin role    | Recent audit entries (newest first). |
| GET    | `/api/admin/security-events`        | admin role    | Recent 401/403 / auth-failure events. |
| GET    | `/api/admin/rbac-policies`          | admin role    | Role → permission strings + default scopes. |
| GET    | `/api/admin/system-status`          | admin role    | Uptime, counts, known roles, known resources. |
| GET    | `/health`                           | none          | `{"status":"ok"}` (also bypassed by nginx proxy). |
| GET    | `/docs`                             | none          | Auto-generated OpenAPI / Swagger. |

## Make targets

```
make build              # build the backend image
make build-test         # build the tester image
make up                 # start backend (detached + wait)
make down               # stop the stack
make logs               # tail logs from all services
make test               # full pytest suite in Docker (218 tests)
make test-unit          # unit tests only (~85 tests)
make test-integration   # integration tests only (~133 tests)
make e2e                # run scripts/e2e.sh — full curl matrix + pytest + frontend probe
make demo               # auto-generate .env if needed, then start both backend + frontend
make ui-e2e             # reminder: Chrome MCP UI tests are main-thread only
make clean              # docker compose down -v + image prune
```

## Demo flow

After `make demo`, open http://localhost:3000:

1. **Log in as alice** (`admin123`). Dashboard shows role chip `administrator`, default scope `*`, plus admin tiles "Audit dashboard" and "RBAC policies".
2. Navigate to **Logs**, pick `business.financial`, click Search. Records appear unmasked. Try `business.financial` again on the **export** path via curl — you'll get 403 because of the symbolic `!logs:export:business.financial` deny.
3. Open **Admin**. The four sections (System status, Audit summary, Recent security events, RBAC policies) hydrate from `/api/admin/*`. Audit counts are non-zero because the lifespan startup seeds three demo entries + one security event.
4. **Log out**, log in as **bob** (`dev123`). Dashboard scope shows `application`. The Admin nav link is missing; visiting `/admin` shows a friendly Forbidden message. Searching `business.metrics` returns a 403 with the matched rule `!logs:read:business.*`.
5. Log in as **carol** (`analyst123`). Searching `business.metrics` returns an **aggregated view** (count + by-level breakdown, no individual rows).
6. Log in as **dave** (`support123`). Searching `business.customer` returns rows with PII fields (email, ip, phone, user_id) replaced by `***` and a "PII fields are masked for your role" banner.

## Test suite

- **218 tests** total, all run inside Docker (`docker compose run --rm tester pytest`).
- **Unit tests** (~85): password hashing, user store, JWT encode/decode, permission DSL matcher (incl. deny precedence + wildcards), role policy table, RBAC engine, shared singletons, audit service.
- **Integration tests** (~133): `/health`, full auth flow (login/profile), audit middleware behavior, log search across all 4 roles, log export across all 4 roles, the **64-cell RBAC matrix** (`test_rbac_matrix.py`), all 5 admin endpoints × 4 roles.

The pytest `conftest.py` injects `JWT_SECRET_KEY` via `os.environ.setdefault` at module-top, BEFORE any `src.*` import — this is the same pattern the original article emphasized. Without it, `Settings()` raises `RuntimeError`.

## Configuration

All settings come from environment variables, loaded by `pydantic-settings`. The `.env` file is auto-generated by `scripts/start.sh` on first run with a fresh random `JWT_SECRET_KEY` (and is in `.gitignore` — never committed).

| Variable | Default | Required? |
|----------|---------|-----------|
| `JWT_SECRET_KEY` | (none) | **yes** — service fails fast on startup if missing |
| `JWT_ALGORITHM` | `HS256` | no |
| `JWT_EXPIRY_MINUTES` | `60` | no |
| `APP_HOST` | `0.0.0.0` | no |
| `APP_PORT` | `8000` | no |
| `APP_LOG_LEVEL` | `info` | no |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3000` | no |

## What I learned

- **The permission DSL string shape (`domain:action:resource`) is the load-bearing contract.** Tutorials default to a generic `read:resource` pattern; using a three-segment string with a stable `logs:` prefix made the wildcard semantics (`logs:read:application.*`) much cleaner and let the audit log surface the exact matched rule for free.
- **Deny rules need to run first, before allows.** Concatenating role permissions in declaration order works fine because the matcher does two passes: deny first (short-circuit), allow second. The 64-cell parametrized matrix test was the cheapest way to lock the table.
- **Singletons must live in one module.** I caught myself nearly instantiating a second `AuditService()` inside the audit router. The requirement doc's explicit "singletons live in `shared.py`" was load-bearing — without it, admin endpoints would show an empty audit log while middleware was writing to a different one.
- **Fail-fast on missing JWT secret is a real pattern.** `config.py` raises `RuntimeError` if `JWT_SECRET_KEY` is missing, and the autouse pytest fixture injects a test secret via `os.environ.setdefault` BEFORE any `src.*` import. Both halves of that contract matter.
- **Permission tags (`aggregated_only`, `mask_pii`) are nicer than separate response shapes.** The matcher carries tags through the `Decision`, and the route handler turns them into post-filters (aggregate / mask). Adding a new tag is a one-line change to the role table — no new endpoint, no new schema.
- **nginx + Vite both proxy `/api` so the frontend code never knows the backend URL.** This made the dev/prod story cheap — same JS in both modes, just two different proxies in front of the same backend.
- **The Docker healthcheck must use `127.0.0.1`, not `localhost`** when busybox-wget runs inside an alpine container — busybox prefers IPv6 and silently fails when the server only binds IPv4. Caught this during C11a.

## Layout

```
rbac-log-security-gateway/
├── backend/
│   ├── Dockerfile, Dockerfile.test, pytest.ini
│   ├── src/{main,config,shared}.py
│   ├── src/auth/{passwords,users,jwt,service,dependencies}.py
│   ├── src/rbac/{permissions,roles,engine}.py
│   ├── src/audit/{models,service}.py
│   ├── src/middleware/audit.py
│   ├── src/api/{auth,logs,admin}.py
│   ├── src/schemas/{auth,logs,admin}.py
│   ├── src/data/mock_logs.py
│   └── tests/{conftest,unit/*,integration/*}.py
├── frontend/
│   ├── Dockerfile, nginx.conf
│   ├── package.json, vite.config.js, index.html
│   └── src/{main,App,router,styles}.jsx + api/, contexts/, hooks/, components/, pages/
├── scripts/{start,stop,cleanup,e2e}.sh
├── docker-compose.yml, Makefile
├── .env.example, .dockerignore, .gitignore
├── README.md, project_requirements.md, requirements.txt
```

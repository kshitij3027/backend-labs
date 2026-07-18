# Log Query API (REST)

A **versioned REST service that exposes a log store over HTTP**. One long-lived ASGI process on port `8000` serves four read paths over the same in-memory corpus — **paginated retrieval**, **filtered / advanced search**, **real-time SSE streaming**, and **aggregate statistics** — behind three cross-cutting gates: **JWT authentication**, **role-based access control**, and **per-tier rate limiting**. Every route lives under an explicit `/api/v1` prefix, every schema is a Pydantic model, and the whole surface is self-documented at `/docs` (Swagger UI) and `/redoc`.

> **Status: scaffold only.** This folder currently contains `README.md`, `requirements.txt`, and `.gitignore` — no source, no tests, no Docker files. Everything below is the intended design, written down before implementation starts. Nothing here has been built or measured yet.

---

## What It Does

A log store is useless without a query surface, and a query surface is dangerous without a gate in front of it. This project is the gate and the surface:

| Capability | Route family | What it solves |
|---|---|---|
| **Paginated retrieval** | `GET /api/v1/logs` | Walk the corpus newest-first without ever loading it all — stable pagination that doesn't skip or duplicate entries while new logs are being appended |
| **Filtered / advanced search** | `GET /api/v1/logs` + `POST /api/v1/logs/search` | Narrow by level, service, host, time range, and free-text message; combine predicates with explicit boolean structure when query strings stop being expressive enough |
| **Real-time streaming** | `GET /api/v1/logs/stream` | Follow matching logs live over Server-Sent Events — the same filter vocabulary as search, applied to the tail instead of the history |
| **Aggregate statistics** | `GET /api/v1/stats` | Counts by level / service / time bucket, top error messages, ingest throughput — the numbers a dashboard needs without pulling raw rows |
| **Auth + RBAC + rate limiting** | `POST /api/v1/auth/token`, all routes | Who you are (JWT), what you may do (role), and how hard you may ask (tier bucket) |

The store itself is deliberately simple: an **append-only in-memory ring** of structured log entries with secondary indexes on the fields worth filtering by, seeded at startup by a built-in generator and appendable at runtime via a write route. No database, no Redis, no message queue — the interesting problems in this project are the *API contract*, the *auth model*, and the *back-pressure*, not storage.

---

## Planned Architecture

One FastAPI process. A single `LogStore` instance and a single `RateLimiter` live on `app.state.runtime`, both built once in the `lifespan`; every handler reads them defensively and degrades to a safe fallback rather than a `500`.

```
                       ┌──────────────────── middleware / dependency chain ───────────────────┐
HTTP request ──────────┤ request-id → JWT decode → role check → tier token bucket → handler   │
                       └──────────────────────────────────────────────────────────────────────┘
                                                       │
        ┌──────────────────────────┬───────────────────┼────────────────────┬─────────────────────┐
        ▼                          ▼                   ▼                    ▼                     ▼
  GET /logs                 POST /logs/search    GET /logs/stream      GET /stats          POST /logs
  cursor pagination         structured filter    SSE, filtered tail    rolling aggregates  append (writer role)
        │                          │                   │                    │                     │
        └──────────────────────────┴─────────┬─────────┴────────────────────┘                     │
                                             ▼                                                    │
                                    LogStore (in-memory)  ◄───────────────────────────────────────┘
                                    append-only deque + secondary indexes
                                    (level · service · host · time) + subscriber fan-out
```

**Request pipeline.** Auth and limiting are FastAPI **dependencies**, not ad-hoc checks inside handlers, so the contract is declared per-route and shows up in the OpenAPI schema:

1. **`request_id`** — every response carries `X-Request-ID` (echoed if the client supplied one) for log correlation.
2. **`current_principal`** — decodes the `Authorization: Bearer <jwt>` header, verifies signature + expiry, and yields a `Principal {subject, role, tier}`. Failure → `401` with a `WWW-Authenticate` header.
3. **`require_role(...)`** — asserts the principal's role satisfies the route's minimum. Failure → `403`. Authenticated-but-unauthorized is never conflated with unauthenticated.
4. **`rate_limit`** — a per-principal token bucket sized by the principal's tier. Every response carries `X-RateLimit-Limit` / `-Remaining` / `-Reset`; exhaustion → `429` with `Retry-After`.

**Versioning.** `/api/v1` is a hard prefix on every data route. Unversioned paths (`/health`, `/docs`, `/redoc`, `/openapi.json`) are the only exceptions. A future `v2` mounts a second router beside `v1` rather than mutating it — the point of the prefix is that breaking changes get a new namespace, not a changelog entry.

### Planned module layout

```
src/
├── config.py        # pydantic-settings Settings + get_settings() (all env tunables)
├── models.py        # single source of truth for the API vocabulary: LogEntry, filters,
│                    #   paginated envelopes, search request/response, stats snapshot
├── store.py         # LogStore — append-only ring, secondary indexes, cursor scan, SSE fan-out
├── generators.py    # deterministic seeded log corpus (startup seed + test ground truth)
├── auth.py          # JWT issue/verify, password hashing, Principal, Role/Tier enums
├── ratelimit.py     # token-bucket RateLimiter keyed by principal, sized by tier
├── deps.py          # FastAPI dependencies: current_principal, require_role, rate_limit
├── stats.py         # rolling aggregate computation over the store
├── api/
│   ├── v1.py        # the /api/v1 router (logs, search, stream, stats, auth)
│   └── health.py    # unversioned liveness
└── main.py          # Runtime dataclass + lifespan + `app` + OpenAPI metadata

scripts/
├── verify_e2e.py    # black-box E2E verifier (auth → paginate → search → SSE → stats → 401/403/429)
└── load_test.py     # concurrent perf/load harness with hard gates
```

---

## Tech Stack

- **Language / runtime:** Python 3.11
- **API:** FastAPI + `uvicorn[standard]`, auto-generated OpenAPI 3.1 at `/docs` and `/redoc`
- **Streaming:** `sse-starlette` — Server-Sent Events over plain HTTP (no WebSocket upgrade, so it survives ordinary proxies and `curl -N`)
- **Auth:** `PyJWT` (HS256) for tokens, `bcrypt` for password hashing
- **Models / config:** Pydantic v2 + pydantic-settings
- **Storage:** in-process, in-memory — no database, no Redis, no queue
- **Testing:** pytest + `httpx` (+ `httpx-sse` for the stream)
- **Infra (planned):** Docker + Docker Compose, optionally k3d for a single-node Kubernetes run

---

## Planned API Surface

All data routes are prefixed `/api/v1`. Every list response is a **paginated envelope**, never a bare array — a bare top-level array is a compatibility dead end.

| Method | Path | Role | Purpose |
|---|---|---|---|
| `GET`  | `/health`                | — (public) | Liveness; dependency-free, always `200` while alive |
| `POST` | `/api/v1/auth/token`     | — (public) | Exchange credentials for a signed JWT (+ its expiry, role, tier) |
| `GET`  | `/api/v1/auth/me`        | viewer | Echo the decoded principal — the fastest way to prove a token works |
| `GET`  | `/api/v1/logs`           | viewer | Paginated retrieval with simple query-string filters |
| `GET`  | `/api/v1/logs/{id}`      | viewer | Fetch one entry by id (`404` when absent) |
| `POST` | `/api/v1/logs/search`    | analyst | Advanced search — structured boolean filter in the request body |
| `GET`  | `/api/v1/logs/stream`    | analyst | SSE tail of matching entries |
| `GET`  | `/api/v1/stats`          | viewer | Aggregate snapshot (counts by level/service, time buckets, top messages) |
| `POST` | `/api/v1/logs`           | writer | Append an entry (feeds the store, the stream, and the stats) |
| `GET`  | `/api/v1/debug/memory`   | admin  | Backend RSS in MB — the load-test memory probe |

### Pagination

`GET /api/v1/logs` supports both styles, because they answer different questions:

- **Cursor** (`?cursor=<opaque>&limit=50`) — the default and the correct one for a growing log store. The cursor encodes the last-seen entry's position, so concurrent appends can't cause skips or duplicates.
- **Offset** (`?offset=200&limit=50`) — supported for human/ad-hoc use and for a "jump to page N" UI, with the honest caveat that it drifts under concurrent writes.

The envelope is the same either way:

```json
{
  "items": [ { "id": "…", "ts": "2026-07-27T10:31:04.512Z", "level": "ERROR",
               "service": "auth-svc", "host": "node-3", "message": "invalid token" } ],
  "page": { "limit": 50, "returned": 50, "next_cursor": "b64:…", "has_more": true, "total": 12840 }
}
```

`limit` is clamped to a configured maximum (`MAX_PAGE_SIZE`) rather than rejected, so a client asking for 10,000 rows gets the ceiling and a header saying so — not a `422`.

### Filtering & advanced search

Query-string filters on `GET /api/v1/logs` cover the common case: `level`, `service`, `host`, `since`, `until`, `q` (substring over `message`). They are **ANDed**, which is the only thing a flat query string can honestly express.

Anything more expressive moves to `POST /api/v1/logs/search`, where the body carries a structured filter tree (`{"all": [...]}` / `{"any": [...]}` / `{"not": {...}}` over leaf predicates like `{"field": "level", "op": "in", "value": ["ERROR", "FATAL"]}`), plus `sort`, `limit`, and the same cursor. Using `POST` for a read is deliberate: a nested filter does not fit in a URL, and it keeps sensitive search terms out of proxy access logs.

### SSE streaming

```
GET /api/v1/logs/stream?level=ERROR&service=auth-svc
Accept: text/event-stream
```

The connection stays open; each matching append is emitted as one `data:` frame of the same `LogEntry` JSON that the paginated route returns — one schema, two delivery modes. Periodic comment-only heartbeat frames keep idle proxies from closing the connection, `Last-Event-ID` is honored on reconnect so a client can resume from where it dropped, and a slow consumer is **dropped rather than buffered without bound** — a stalled reader must never be able to grow the server's memory.

### Statistics

`GET /api/v1/stats` returns counts by level and service, a time-bucketed histogram over the configured window, the top recurring error messages, and ingest throughput. It respects the same filter vocabulary, so "stats for this search" and "results for this search" are guaranteed to describe the same set.

---

## Auth, RBAC & Rate Limiting

**Authentication.** `POST /api/v1/auth/token` takes credentials and returns a short-lived HS256 JWT whose claims carry `sub`, `role`, `tier`, `iat`, and `exp`. Bootstrap users come from configuration (with bcrypt-hashed passwords); there is no user-registration surface, because user management is not what this project is about. The signing key is read from the environment and has **no usable default** — the service refuses to start with a placeholder key outside of explicit dev mode, so a demo secret can never quietly become a production secret.

**RBAC.** Four roles, strictly ordered — each includes everything below it:

| Role | May do |
|---|---|
| `viewer`  | read: paginated retrieval, single fetch, stats |
| `analyst` | + advanced search and the SSE stream |
| `writer`  | + append log entries |
| `admin`   | + debug/operational routes |

Enforcement is a route dependency, so the required role is part of the generated OpenAPI document rather than tribal knowledge. `401` means "I don't know who you are"; `403` means "I know, and no."

**Rate limiting.** A token bucket per principal, refilled continuously, sized by the principal's **tier**:

| Tier | Sustained rate | Burst | Intent |
|---|---|---|---|
| `free`       | 10 req/s  | 20  | tire-kicking and docs browsing |
| `pro`        | 100 req/s | 200 | a real dashboard or a scripted client |
| `enterprise` | 1000 req/s| 2000| bulk export and backfill |

Limits are advertised on every response (`X-RateLimit-Limit`, `-Remaining`, `-Reset`) — not just on rejection — so a well-behaved client can pace itself instead of discovering the ceiling by hitting it. Exhaustion returns `429` with `Retry-After`. SSE connections are counted against a separate per-principal **concurrent-stream** cap, since one long-lived connection and one thousand quick requests are not the same kind of load.

---

## How It Will Run

A long-lived ASGI HTTP server on port `8000`, containerized, runnable standalone (`docker run`), via Compose, or on a single-node k3d cluster. Interaction is over ordinary HTTP clients — `curl`, `httpx`, or the Swagger UI at `/docs` — with an optional browser dashboard over the same public API.

Once implemented, the sketch is:

```bash
make up                       # API: http://localhost:8000 · docs: http://localhost:8000/docs
curl -s http://localhost:8000/health

TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/token \
  -d 'username=analyst&password=…' | jq -r .access_token)

curl -s -H "Authorization: Bearer $TOKEN" \
  'http://localhost:8000/api/v1/logs?level=ERROR&limit=20'

curl -N -s -H "Authorization: Bearer $TOKEN" \
  'http://localhost:8000/api/v1/logs/stream?level=ERROR'
```

Host port is intended to be compose-level and overridable (`API_PORT=8010 make up`), since sibling projects in this repo routinely hold `:8000`.

---

## Planned Configuration

Settings will be read from **field defaults → optional `.env` → environment variables**, each env var being the upper-cased field name. A committed `.env.example` will carry placeholders only — never a real signing key.

| Setting | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Root log level |
| `API_PORT` | `8000` | Host port (compose maps → uvicorn `:8000`) |
| `JWT_SECRET` | *(none)* | HS256 signing key — **required**; startup fails on the placeholder value |
| `JWT_ALGORITHM` | `HS256` | Token signing algorithm |
| `ACCESS_TOKEN_TTL_MIN` | `30` | Token lifetime in minutes |
| `STORE_CAPACITY` | `100000` | Max entries retained in the in-memory ring |
| `SEED_ENTRIES` | `10000` | Entries generated into the store at startup |
| `DEFAULT_PAGE_SIZE` | `50` | `limit` when the client omits it |
| `MAX_PAGE_SIZE` | `500` | Ceiling that `limit` is clamped to |
| `SSE_HEARTBEAT_SEC` | `15` | Comment-frame keepalive interval |
| `SSE_QUEUE_SIZE` | `1000` | Per-subscriber buffer; overflow drops the slow consumer |
| `MAX_STREAMS_PER_PRINCIPAL` | `3` | Concurrent SSE cap per principal |
| `RATE_LIMIT_ENABLED` | `true` | Operability switch for the limiter |
| `TIER_LIMITS` | *(see above)* | Per-tier sustained rate + burst |
| `STATS_BUCKET_SEC` | `60` | Time-bucket width for the stats histogram |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins (credentials disabled with `*`) |

---

## Planned Testing

Per the repo rules, everything gets verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a hard-gated load harness.

- **Unit** — store (cursor stability under concurrent append, index correctness, ring eviction), filter-tree evaluation, JWT issue/verify (including expiry and tampering), the token-bucket refill math, and stats aggregation.
- **Integration** — the full dependency chain per route: `401` without a token, `403` with an insufficient role, `429` past the bucket, correct `X-RateLimit-*` headers, pagination envelopes that neither skip nor duplicate, and an SSE stream that delivers appends made after the connection opened.
- **E2E** (`scripts/verify_e2e.py`) — ordered black-box checks against a live container: health → token issue → authorized read → pagination walk covers the corpus exactly once → advanced search agrees with generated ground truth → SSE receives a post-connect append → stats agree with the same filter's result count → `401`/`403`/`429` all provably reachable.
- **Load** (`scripts/load_test.py`) — concurrent reads with gates on throughput, p95 latency, error count, and backend RSS, plus a sustained multi-client SSE fan-out check.

Measured numbers will be filled in here once the implementation exists. There are none yet.

---

## What I Learned

To be written once the project is built.

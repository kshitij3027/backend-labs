# Log Query API (REST)

A **versioned REST service that exposes a log store over HTTP**. One long-lived ASGI process on port `8000` serves four read paths over the same in-memory corpus — **paginated retrieval**, **filtered / advanced search**, **real-time SSE streaming**, and **aggregate statistics** — behind three cross-cutting gates: **JWT authentication**, **role-based access control**, and **per-tier rate limiting**. Every data route lives under an explicit `/api/v1` prefix, every schema is a Pydantic model, the whole surface is self-documented at `/docs` (Swagger UI) and `/redoc`, and a dependency-free browser dashboard is served by the same process at `/`.

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

## Architecture

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

  GET /  +  /static/*   ← the dashboard, served by this same process (no build step, no nginx)
```

**Request pipeline.** Auth and limiting are FastAPI **dependencies**, not ad-hoc checks inside handlers, so the contract is declared per-route and shows up in the OpenAPI schema:

1. **`request_id`** — every response carries `X-Request-ID` (echoed if the client supplied one) for log correlation.
2. **`current_principal`** — decodes the `Authorization: Bearer <jwt>` header, verifies signature + expiry, and yields a `Principal {subject, role, tier, issued_at, expires_at}`. Failure → `401` with a `WWW-Authenticate` header.
3. **`require_role(...)`** — asserts the principal's role satisfies the route's minimum. Failure → `403`. Authenticated-but-unauthorized is never conflated with unauthenticated.
4. **`rate_limit`** — a per-principal token bucket sized by the principal's tier. Every response carries `X-RateLimit-Limit` / `-Remaining` / `-Reset`; exhaustion → `429` with `Retry-After`.

The `X-Request-ID` and `X-RateLimit-*` stamping is **middleware**, not a dependency writing to an injected `response: Response` — the latter is silently discarded the moment a dependency raises, and the raising paths (`401`, `403`, `429`) are exactly where a client most needs the numbers.

**Versioning.** `/api/v1` is a hard prefix on every data route. Unversioned paths (`/health`, `/`, `/static/*`, `/docs`, `/redoc`, `/openapi.json`) are the only exceptions. A future `v2` mounts a second router beside `v1` rather than mutating it — the point of the prefix is that breaking changes get a new namespace, not a changelog entry.

### Module layout

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
├── stats.py         # on-demand aggregate computation over the store
├── api/
│   ├── v1.py        # the /api/v1 router (logs, search, stream, stats, auth, debug)
│   ├── health.py    # unversioned liveness
│   └── dashboard.py # GET / — the static dashboard, plus the no-cache StaticFiles subclass
├── static/          # index.html + app.css + app.js (the dashboard; no build step)
└── main.py          # Runtime dataclass + lifespan + middleware + `app` + OpenAPI metadata

scripts/
├── verify_e2e.py    # black-box E2E verifier — 15 ordered checks against a live container
└── load_test.py     # concurrent perf/load harness with hard gates (4 phases)
```

---

## Tech Stack

- **Language / runtime:** Python 3.11
- **API:** FastAPI 0.115 + `uvicorn[standard]`, auto-generated OpenAPI 3.1 at `/docs` and `/redoc`
- **Streaming:** `sse-starlette` — Server-Sent Events over plain HTTP (no WebSocket upgrade, so it survives ordinary proxies and `curl -N`)
- **Auth:** `PyJWT` (HS256) for tokens, `bcrypt` **called directly** for password hashing (not via passlib — passlib 1.7.4 reads `bcrypt.__about__.__version__`, which bcrypt 4.x removed)
- **Models / config:** Pydantic v2 + pydantic-settings
- **Storage:** in-process, in-memory — no database, no Redis, no queue
- **Dashboard:** three static files (HTML/CSS/vanilla JS) served by the API process, Chart.js from a pinned CDN
- **Testing:** pytest + `httpx` (+ `httpx-sse` for the stream)
- **Infra:** Docker + Docker Compose. One production service; the test / e2e / loadtest services are profile-gated and never start on a bare `docker compose up`.

---

## API Surface

All data routes are prefixed `/api/v1`. Every list response is a **paginated envelope**, never a bare array — a bare top-level array is a compatibility dead end.

| Method | Path | Role | Purpose |
|---|---|---|---|
| `GET`  | `/health`                | — (public) | Liveness; dependency-free, always `200` while alive |
| `GET`  | `/`                      | — (public) | The browser dashboard (static shell; it authenticates itself) |
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

- **Cursor** (`?cursor=<opaque>&limit=50`) — the default and the correct one for a growing log store. The cursor encodes the last-seen entry's `seq`, so concurrent appends can't cause skips or duplicates.
- **Offset** (`?offset=200&limit=50`) — supported for human/ad-hoc use and for a "jump to page N" UI, with the honest caveat that it drifts under concurrent writes. Supplying both is a `400`.

The envelope is the same either way:

```json
{
  "items": [ { "id": "…", "ts": "2026-07-27T10:31:04.512Z", "level": "ERROR",
               "service": "auth-svc", "host": "node-3", "message": "invalid token",
               "attrs": {} } ],
  "page": { "limit": 50, "returned": 50, "next_cursor": "b64:…", "has_more": true, "total": 12840 }
}
```

`limit` is clamped to a configured maximum (`MAX_PAGE_SIZE`) rather than rejected, so a client asking for 10,000 rows gets the ceiling and an `X-Page-Limit-Clamped` header saying what it asked for — not a `422`. If the cursor's anchor has already been evicted from the ring, the page carries `X-Cursor-Truncated: true` rather than silently returning fewer rows.

> ### A cursor is bound to its filter — re-send the filter with it
>
> This is the one contract most likely to bite a client, so it is stated plainly. `next_cursor` encodes the filter's fingerprint and the sort order alongside the position. You must send **the same filter parameters again** on the next page, next to `cursor`:
>
> ```
> GET /api/v1/logs?level=ERROR&limit=50                       → next_cursor=X
> GET /api/v1/logs?level=ERROR&limit=50&cursor=X              ✅ page 2
> GET /api/v1/logs?cursor=X                                   ❌ 400 — filter mismatch
> GET /api/v1/logs?level=FATAL&limit=50&cursor=X              ❌ 400 — foreign cursor
> ```
>
> A bare cursor is a `400` and so is replaying a cursor against a different filter, because in both cases the request describes a different walk. This is deliberate: a cursor from another walk is still a well-formed position, so serving it would return a page that is internally consistent and completely wrong. Refusing makes a wrong answer impossible — at the cost that a client which drops its filter on page 2 gets a `400` instead of a plausible-looking page. That is the trade, and it is the right way round.

### Filtering & advanced search

Query-string filters on `GET /api/v1/logs` cover the common case: `level`, `service`, `host`, `since`, `until`, `q` (substring over `message`). They are **ANDed**, which is the only thing a flat query string can honestly express.

Anything more expressive moves to `POST /api/v1/logs/search`, where the body carries a structured filter tree (`{"all": [...]}` / `{"any": [...]}` / `{"not": {...}}` over leaf predicates like `{"field": "level", "op": "in", "value": ["ERROR", "FATAL"]}`), plus `sort`, `limit`, and the same cursor. Using `POST` for a read is deliberate: a nested filter does not fit in a URL, and it keeps sensitive search terms out of proxy access logs.

### SSE streaming

```
GET /api/v1/logs/stream?level=ERROR&service=auth-svc
Accept: text/event-stream
```

The connection stays open; each matching append is emitted as one `log` event whose `data:` payload is the same `LogEntry` JSON that `GET /logs/{id}` returns — one schema, two delivery modes. Periodic comment-only heartbeat frames keep idle proxies from closing the connection, `Last-Event-ID` is honored on reconnect so a client resumes exactly where it dropped, and a slow consumer is **dropped rather than buffered without bound** — a stalled reader must never be able to grow the server's memory. Concurrent streams per principal are capped (`MAX_STREAMS_PER_PRINCIPAL`); one past the cap is its own `429`, distinct from the rate limiter's.

> ### `?access_token=` is accepted on this route and no other
>
> The browser's native `EventSource` API takes a URL and nothing else — there is no options bag, no header. A dashboard that bearer-authenticates every other call therefore has exactly one way to authenticate its stream, so `GET /api/v1/logs/stream` accepts `?access_token=<jwt>` as a fallback. The `Authorization` header always wins when both are present, including when it is invalid (falling back would let anyone who can append to a URL override a credential the browser attached itself).
>
> It is **rejected everywhere else**, and there is a test pinning that boundary. Most pointedly on `POST /api/v1/logs/search`, whose entire rationale is keeping search terms out of access logs — widening the token rule to every route would have put live credentials in the same logs. The cost is real even where it is allowed: a query string is written down by every hop it passes, so the stream response sets `Referrer-Policy: no-referrer` and tokens are deliberately short-lived.

### Statistics

`GET /api/v1/stats` returns counts by level and service, a time-bucketed histogram over the configured window, the top recurring error messages, and ingest throughput. It takes the same filter vocabulary through the same `LogQuery` model and aggregates over the same `store.iter_matching` the list route walks — so `stats.total == page.total` for a given filter **by construction**, not by two handlers being careful. A histogram that would exceed the point ceiling is folded to a coarser bucket width (reported in `window.bucket_sec`), never truncated: a series that silently stops early is a chart that lies about its own window.

---

## Auth, RBAC & Rate Limiting

**Authentication.** `POST /api/v1/auth/token` takes credentials and returns a short-lived HS256 JWT whose claims carry `sub`, `role`, `tier`, `iat`, and `exp`. Bootstrap users are defined in code (with bcrypt-hashed passwords); there is no user-registration surface, because user management is not what this project is about. The signing key is read from the environment and has **no usable default** — `Settings` rejects an empty, too-short (<16 chars) or placeholder key, so the process refuses to start rather than let a demo secret quietly become a production secret.

### Demo accounts

Four bootstrap accounts, declared once in `src/auth.py` (`DEV_ACCOUNTS`) and hashed **lazily** on first use at `BCRYPT_ROUNDS` — never at import, because four bcrypt hashes at cost 12 is roughly a second of CPU that every container start and every pytest collection would otherwise pay.

| Username | Password | Role | Tier |
|---|---|---|---|
| `viewer`  | `viewer-dev-pw`  | `viewer`  | `free` |
| `analyst` | `analyst-dev-pw` | `analyst` | `pro` |
| `writer`  | `writer-dev-pw`  | `writer`  | `pro` |
| `admin`   | `admin-dev-pw`   | `admin`   | `enterprise` |

These are **dev-only credentials in a public repo** — they are printed here on purpose, because they are demo fixtures, not secrets. The tier assignment is not arbitrary either: it is what makes the gates testable. `viewer` on `free` (burst 20) means the E2E verifier can provoke a `429` in a handful of requests; `admin` on `enterprise` (burst 2000) means the load harness measures the server rather than its own tier ceiling.

**RBAC.** Four roles, strictly ordered — each includes everything below it:

| Role | May do |
|---|---|
| `viewer`  | read: paginated retrieval, single fetch, stats |
| `analyst` | + advanced search and the SSE stream |
| `writer`  | + append log entries |
| `admin`   | + debug/operational routes |

Enforcement is a route dependency, so the required role is part of the generated OpenAPI document (as `x-required-role`) rather than tribal knowledge. `401` means "I don't know who you are"; `403` means "I know, and no."

**Rate limiting.** A token bucket per principal, refilled continuously, sized by the principal's **tier**:

| Tier | Sustained rate | Burst | Intent |
|---|---|---|---|
| `free`       | 10 req/s  | 20  | tire-kicking and docs browsing |
| `pro`        | 100 req/s | 200 | a real dashboard or a scripted client |
| `enterprise` | 1000 req/s| 2000| bulk export and backfill |

Limits are advertised on every response (`X-RateLimit-Limit`, `-Remaining`, `-Reset`) — not just on rejection — so a well-behaved client can pace itself instead of discovering the ceiling by hitting it. Exhaustion returns `429` with `Retry-After`. The role check runs to completion *before* the limiter, so a `403` never drains the caller's bucket. SSE connections are counted against a separate per-principal **concurrent-stream** cap, since one long-lived connection and one thousand quick requests are not the same kind of load.

---

## How to Run

A long-lived ASGI HTTP server, containerized, runnable standalone (`docker run`) or via Compose. The container always binds `:8000`; the **host** port is compose-level and overridable via `API_PORT`, which defaults to **8010** because sibling projects in this repo routinely hold `:8000`.

```bash
make up                       # API: http://localhost:8010 · docs: http://localhost:8010/docs
make ui                       # same process, also prints the dashboard URL

curl -s http://localhost:8010/health
# {"status":"healthy","version":"1.0.0","uptime_sec":12.431,"store_entries":10000}

TOKEN=$(curl -s -X POST http://localhost:8010/api/v1/auth/token \
  -d 'username=analyst&password=analyst-dev-pw' | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

# paginated read (note: page 2 must re-send `level`, see the cursor contract above)
curl -s -H "Authorization: Bearer $TOKEN" \
  'http://localhost:8010/api/v1/logs?level=ERROR&limit=20'

# structured boolean search
curl -s -X POST http://localhost:8010/api/v1/logs/search \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"filter":{"all":[{"field":"level","op":"in","value":["ERROR","FATAL"]},
                        {"not":{"field":"service","op":"eq","value":"auth-svc"}}]},"limit":20}'

# aggregate snapshot over the same filter vocabulary
curl -s -H "Authorization: Bearer $TOKEN" \
  'http://localhost:8010/api/v1/stats?level=ERROR'

# live tail (Ctrl-C to stop)
curl -N -s -H "Authorization: Bearer $TOKEN" \
  'http://localhost:8010/api/v1/logs/stream?level=ERROR'
```

Appending needs the `writer` account, and it is the easiest way to watch the tail move:

```bash
WTOKEN=$(curl -s -X POST http://localhost:8010/api/v1/auth/token \
  -d 'username=writer&password=writer-dev-pw' | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

curl -s -X POST http://localhost:8010/api/v1/logs \
  -H "Authorization: Bearer $WTOKEN" -H 'Content-Type: application/json' \
  -d '{"level":"ERROR","service":"auth-svc","host":"node-3","message":"invalid token"}'
```

Override the host port the same way for everything: `API_PORT=9000 make up`.

### Make targets

| Target | What it does |
|---|---|
| `help`      | List the targets (the default goal) |
| `build`     | Build all images (api + test) |
| `up`        | Run the API detached; print the API + docs URLs |
| `down`      | Stop and remove the stack |
| `logs`      | Tail the API logs |
| `test`      | Full pytest suite in Docker (unit + integration; rebuilds the tester image first) |
| `test-unit` | Unit tests only, in Docker |
| `test-int`  | Integration tests only, in Docker |
| `e2e`       | Black-box E2E verifier vs the live container — 15 ordered checks |
| `load`      | Perf/load gates vs the live container (throughput, latency, errors, memory) |
| `ui`        | `up` plus the dashboard URL — the dashboard is the *same* process, not a second service |
| `clean`     | `down` + remove volumes and orphans |

---

## Dashboard

`GET /` serves a single-page dashboard from the **same uvicorn process on the same port** as the API. There is no build step, no node toolchain, no nginx and no second container: everything the page needs is already published by `/api/v1`, so the dashboard is three flat files under `src/static` plus a router. Because it is served from the same origin as the API it calls, the dashboard path has no CORS in it at all.

- **Login → chips.** The page authenticates against `POST /api/v1/auth/token` and renders the decoded `sub`/`role`/`tier` as chips. The token lives in **`sessionStorage`**, not `localStorage` — it is a short-lived demo credential and it should die with the tab.
- **Table + filters + cursor paging.** A live table over `GET /api/v1/logs`, with the filter re-sent alongside `cursor` on every page (see the cursor contract above — the dashboard is a worked example of getting it right).
- **Stat cards + two charts.** Matching / ingest-rate / resident / evicted cards, a per-level card row, and two canvases — level distribution and entries per time bucket — all from `GET /api/v1/stats`, rendered with **Chart.js from a pinned CDN**. The CDN is a hard dependency for the charts *only*: `app.js` checks for `window.Chart` before touching it, so an air-gapped run loses two canvases and a one-line notice appears, with every number on the page unaffected.
- **Live tail.** Native `EventSource` against `GET /api/v1/logs/stream`, authenticated by `?access_token=` because that API cannot set headers.
- **Honest errors.** A `403` renders a readable message *without* logging you out (it is an authorization answer, not an authentication one); a `429` surfaces the rate-limit badge rather than a blank screen.

Static assets are served through a `StaticFiles` subclass that stamps `Cache-Control: no-cache` on the `200` **and** on the `304`. That is not decoration — see *What I Learned*.

---

## Configuration

Settings are read from **field defaults → optional `.env` → environment variables**, each env var being the upper-cased field name (`store_capacity` ← `STORE_CAPACITY`). The committed [`.env.example`](.env.example) carries placeholders only — never a real signing key. Compose passes every key through as `${VAR:-default}`, so the whole surface is host-overridable.

| Setting | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Root log level (`DEBUG` \| `INFO` \| `WARNING` \| `ERROR`) |
| `API_PORT` | `8010` | **Host** port only; compose maps `${API_PORT:-8010}:8000` and the container always binds `:8000`. Not 8000, because sibling projects in this repo routinely hold that port |
| `JWT_SECRET` | *(none)* | HS256 signing key — **required**; startup fails on an empty, <16-char, or placeholder value |
| `JWT_ALGORITHM` | `HS256` | Token signing algorithm (pinned as a decode allowlist, so `alg: none` is refused) |
| `ACCESS_TOKEN_TTL_MIN` | `30` | Token lifetime in minutes |
| `STORE_CAPACITY` | `100000` | Max entries retained in the in-memory ring |
| `SEED_ENTRIES` | `10000` | Entries generated into the store at startup |
| `DEFAULT_PAGE_SIZE` | `50` | `limit` when the client omits it |
| `MAX_PAGE_SIZE` | `500` | Ceiling that `limit` is clamped to |
| `SSE_HEARTBEAT_SEC` | `15` | Comment-frame keepalive interval |
| `SSE_QUEUE_SIZE` | `1000` | Per-subscriber buffer; overflow drops the slow consumer |
| `MAX_STREAMS_PER_PRINCIPAL` | `3` | Concurrent SSE cap per principal |
| `RATE_LIMIT_ENABLED` | `true` | Operability switch for the limiter |
| `TIER_LIMITS` | `free:10:20,pro:100:200,enterprise:1000:2000` | Per-tier `tier:rate:burst` |
| `STATS_BUCKET_SEC` | `60` | Time-bucket width for the stats histogram |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins (credentials disabled with `*`) |

One documented extra is deliberately not a row above: `BCRYPT_ROUNDS` (default `12`) is the work factor for the demo hashes. It exists so tests can construct `Settings(bcrypt_rounds=4)` (~2 ms per hash instead of ~250 ms) and keep the suite fast without weakening the production default. The E2E and load gates are likewise not backend settings — they are read by `scripts/verify_e2e.py` and `scripts/load_test.py`, and are listed in `.env.example`.

---

## Testing & Measured Performance

Everything is verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a hard-gated load harness, all profile-gated compose services.

```bash
make test        # 498 unit + 246 integration = 744 tests
make e2e         # 15-check black-box verifier vs the live container
make load        # 4-phase perf/load gates vs the live container
```

- **Unit** — store (cursor stability under concurrent append, index correctness, ring eviction), filter-tree evaluation and fingerprinting, JWT issue/verify (including expiry, tampering, and a forged `alg: none` token), the token-bucket refill math, SSE fan-out, and stats aggregation.
- **Integration** — the full dependency chain per route: `401` without a token, `403` with an insufficient role, `429` past the bucket, correct `X-RateLimit-*` headers on success *and* on every error, pagination envelopes that neither skip nor duplicate, and an SSE stream that delivers appends made after the connection opened.
- **E2E** (`scripts/verify_e2e.py`) — 15 ordered black-box checks against a live container: health shape + `X-Request-ID` → token issue for all four demo users → `401` surface → list envelope + rate-limit headers → full cursor walk → limit clamping → single fetch + `404` → search RBAC + ground-truth agreement → SSE post-connect append → SSE cap `429` → `Last-Event-ID` resume → stats agreement → `debug/memory` is admin-only → free-tier `429` → read p95 + RSS + docs surface.
- **Load** (`scripts/load_test.py`) — phase A concurrent `GET /logs`, phase B a fixed mixed-endpoint workload, phase C an SSE fan-out with a deliberate slow consumer, phase D the server's own RSS.

### Measured results

From the final Docker verification run — `E2E PASSED (15/15)`, `LOAD PASSED`, single uvicorn process, load concurrency 50.

| Metric | Result | Gate |
|---|---|---|
| Unit + integration tests | **744 passing** (498 unit + 246 integration) | all green |
| E2E checks | **15 / 15 passed** | all pass |
| Cursor walk integrity | **10,000 unique ids over 100 pages of 100 — 0 duplicates, 0 skips** | exact corpus coverage |
| Search agreement | `POST /logs/search` == `GET /logs` == generator oracle over n=10,000: ERROR **706**, auth-svc **1249**, compound tree **703** | exact match |
| Stats ↔ list agreement | 4 filters agree exactly: unfiltered **10004**, ERROR **706**, auth-svc **1249**, ERROR+FATAL **806** | `stats.total == page.total` |
| Over-large limit | `limit=100000` → `page.limit=500` + `X-Page-Limit-Clamped`, HTTP **200** | clamped, never a 422 |
| SSE delivery | post-connect append delivered; frame JSON **identical** to `GET /logs/{id}` | one schema, two modes |
| SSE concurrent cap | 3 held, 4th → **429** `"too many concurrent SSE streams: 'analyst' already holds 3; close one before opening another"` | cap enforced, own status |
| `Last-Event-ID` resume | replayed **exactly 2**, anchor not repeated | no gap, no duplicate |
| Free-tier rate limit | **40 of 60** viewer requests → `429`, `Retry-After: 1s`, `X-RateLimit-Remaining: 0` | 429 reachable |
| Sequential read latency | **p50 0.7 ms · p95 1.2 ms** over n=50 | p95 ≤ 250 ms |
| Backend RSS (E2E) | **79.8 MB** | ≤ 400 MB |
| Load phase A — 2000 `GET /logs` | **388.9 req/s**, **0 errors**, p50 **76.4 ms** · p95 **390.0 ms** · p99 646.5 ms | ≥ 200 rps · p95 ≤ 800 ms · 0 errors |
| Load phase B — 1000 mixed | **341.9 req/s**, **0 errors**, p50 96.9 ms · p95 403.1 ms | same three gates |
| Load phase B — per endpoint p95 | `GET /logs` **408.2** · `POST /logs/search` **436.6** · `GET /logs/{id}` **394.4** · `GET /stats` **376.0** ms | attributable, not just noticed |
| Load phase C — SSE fan-out | 8 clients × **546 frames each = 4368**; 546 appends, **0 rejected**; peak subscribers 9; peak RSS 81.1 MB; all slots released after | ≥ 5 frames/client · bounded memory · release |
| Load phase D — memory | RSS **81.2 MB**, 10,546 entries, **0 evicted** | ≤ 400 MB |

The two p95 ceilings are deliberately different numbers for different quantities. `MAX_P95_MS` (250 ms) bounds the E2E's **sequential** reads — one request in flight, so it measures service time. `LOAD_MAX_P95_MS` (800 ms) bounds the load harness at concurrency 50 in front of a **single** uvicorn worker, where Little's Law alone puts mean latency at `concurrency / throughput` ≈ 130 ms before the server has done anything wrong. Sharing one number would fail a healthy server on arithmetic.

### The gates actually bite

Every gate is host-overridable, which is how you prove it is real rather than decorative. All four of these exit non-zero, each for its intended reason:

```bash
MAX_BACKEND_MEM_MB=1 make e2e      # memory gate
MAX_P95_MS=0 make e2e              # sequential latency gate
LOAD_MIN_RPS=1000000 make load     # throughput gate
LOAD_MAX_P95_MS=1 make load        # under-load latency gate
```

### Dashboard verification

Verified in Chrome against the live container: login renders the principal chips; 50 rows of real data; both Chart.js canvases drawn at 874×360; the stat cards match the corpus exactly (**DEBUG 1908 / INFO 6041 / WARN 1245 / ERROR 706 / FATAL 100**); filtering to `ERROR` left every visible level cell `ERROR` with `stat-total == log-total`; cursor paging 50 → 100 kept the filter; the **live tail went 0 → 5 frames** when a writer appended from *outside* the browser; a `403` rendered a readable message without logging the user out; a `429` surfaced at request 22 with the badge reading `0 / 20`; zero page-origin console errors; and the token never left `sessionStorage`.

---

## What I Learned

- **A `seq`-anchored cursor is the only pagination that survives concurrent appends — and the eviction case is where it gets interesting.** Offset paging counts from the *current* head, so N appends between page 1 and page 2 shift every subsequent page by N and the client sees the same rows twice. That is inherent to offset, not a bug in an implementation. Anchoring on a monotone `seq` fixes both halves at once: new entries land above the anchor and are structurally invisible to a DESC walk, and nothing can move an existing entry's `seq`, so nothing can slide across the anchor either. The subtle part is what to do when the anchor has been *evicted* from the ring, and the answer differs by direction. ASC resumes at the oldest resident record — the same rule Kafka's `auto.offset.reset=earliest` picks — because the walk travels toward rows that still exist. DESC has nowhere to resume *to*: everything below the anchor is exactly what was evicted, so the honest page is an empty terminal one with `X-Cursor-Truncated: true`. The tempting symmetry — "resume from the oldest resident" in both directions — would snap a DESC anchor *upward* and emit rows above it, which the client has almost certainly already seen. That trades a flagged empty page for a silent duplicate, and silent duplicates are the exact failure the whole design exists to prevent.

- **`403` must never collapse into `401`, and rate-limit headers belong on the error paths most of all.** Conflating them is a real bug with two victims: a client that retries a `403` by re-authenticating loops forever, and a UI that logs you out on `403` throws away a perfectly good session because you touched an admin route. `401` means "I don't know who you are"; `403` means "I know, and no." The header lesson took a working implementation to see. The natural FastAPI shape is a dependency writing to an injected `response: Response` — and those headers are silently discarded the moment any dependency *raises*, because the response the handler was going to decorate never gets built. The raising paths are `401`, `403` and `429`, which is to say: precisely the responses where a client most needs to be told what its ceiling is. A `429` that does not say when to come back is barely better than a connection reset. Moving the stamping into middleware — which sees the response that actually leaves the app, including unhandled `500`s — is what made the header a contract instead of a happy-path courtesy. The ordering of the gates is load-bearing for a similar reason: the role check runs to completion before the limiter, so a `403` cannot drain the caller's bucket and turn an authorization mistake into a denial of service against yourself.

- **A bounded queue that drops slow consumers is the only safe SSE back-pressure policy — and it is weaker in practice than the config number suggests.** The alternatives are worse in ways that are easy to miss: an unbounded queue means any stalled reader can grow the server's memory without limit (one `curl` you forgot to Ctrl-C is now an OOM), and blocking the producer means one slow reader stalls the fan-out for everyone. Dropping is the only policy where a misbehaving client's blast radius is that client. But the honest caveat is that `SSE_QUEUE_SIZE` is not the whole buffer. Between the queue and the reader sit the kernel's socket send buffer and the client's receive buffer, and those absorb a backlog the application never sees — so a genuinely slow reader stays connected far longer than a 1000-slot queue implies. Measured: `curl --limit-rate 1` against the 1000-slot default was never dropped for the duration of the test; dropping to 50 made it fire exactly as designed. That is why the load harness asserts **bounded memory and eventual release** (RSS inside its ceiling, subscriber count never above what was opened, back to baseline after teardown) rather than gating on a `dropped` frame appearing. Gating on a thing that depends on kernel buffer sizing would be a flaky test dressed up as a guarantee.

- **Computing stats over the same filtered iterator as the list route makes agreement structural.** The obvious design is incremental counters folded on append — `O(1)` per write and no scan. It is also unfilterable: the moment `?service=auth-svc` appears you need a second code path, and now two pieces of code define "matches" and they will eventually disagree by three. Computing on demand over the *same* compiled filter and the *same* `iter_matching` the list route walks costs a pass over the corpus, and buys `stats.total == page.total` **by construction** — not by two handlers being careful, but by there being one filter compiler and one definition of matching. A dashboard can put a number and a table side by side and never have to explain a discrepancy. The E2E check that compares stats against the list route across four filters is really just proving the property is still structural; it has never been the thing that catches a bug, because there is no second implementation left to drift.

- **`except Exception` is not the ceiling of what a Python call can raise.** bcrypt 4.x is a PyO3 extension, and a malformed hash with a valid `$2b$NN$` prefix but a truncated body makes the Rust side slice past the end of its input. That is a Rust *panic*, and it crosses the FFI boundary as `pyo3_runtime.PanicException`, whose MRO is `(PanicException, BaseException, object)` — it does not derive from `Exception`, so neither `except (ValueError, TypeError)` nor `except Exception` catches it. One corrupt stored hash would have taken down the token endpoint with an error nothing in the codebase could handle. The fix worth remembering is not the `except BaseException` backstop (a panic that has already unwound leaves the extension's internal state undefined by construction, so surviving it is not the same as being fine); it is validating the hash's shape with a regex *before* a single byte reaches Rust, which makes the panic path unreachable rather than merely survivable.

- **An `ETag` without a `Cache-Control` is an invitation to serve stale JavaScript.** Starlette stamps `ETag` and `Last-Modified` on a file response and no freshness directive — and a response carrying a validator but no explicit directive licenses the browser to cache *heuristically*, commonly around 10% of the age implied by `Last-Modified`, reused with no request to the server at all. The dashboard's filenames carry no content hash, so their URLs do not change when their bytes do: that combination is exactly how a returning user silently runs last week's `app.js` against this week's API, which surfaces as an unreproducible "works for me" report. `Cache-Control: no-cache` is the fix and it is widely misread — it means "cache freely, but revalidate before every reuse", not "do not cache" (that is `no-store`, which would throw away a working cache to fix a freshness problem). The part that is easy to get half-right: the header has to be stamped on the `304` too, because a `304` that omits it re-opens the same heuristic window for every reuse after the first.

# GraphQL Log Query Platform

A **GraphQL API over a persisted log store**. One long-lived ASGI process on port `8000` serves the entire surface from a single origin: `POST /graphql` for flexible filtered queries, SQL-computed aggregate stats and log creation; `WS /graphql` for real-time streaming; `GET /health` and `GET /metrics` for the infrastructure; and `GET /` for the React dashboard. Rows live in **Postgres** behind SQLAlchemy 2.x async, results and persisted query documents live in **Redis**, and everything in front of the resolvers — **DataLoader batching**, **cache-aside**, **automatic persisted queries**, and a **depth + complexity gate that rejects expensive operations before execution** — exists because one flexible endpoint is exactly as expensive as its worst client makes it.

> **Status: commit C1 of 14 — scaffold.** The container, the Postgres/Redis stack, configuration and `/health` are real. The GraphQL schema (C3), persistence (C2), subscriptions (C6), cache (C7), cost gate (C8), APQ + metrics (C9), the e-commerce schema (C10–C12) and the React dashboard (C13) land in the commits named against each section below. Nothing in this document describes behaviour that does not exist yet without saying which commit it arrives in.

---

## What It Does

A single endpoint answering questions that would otherwise be three round trips — and a set of guards that make "the client picks the shape of the response" survivable:

| Capability | Operation | What it solves | Lands |
|---|---|---|---|
| **Filtered log query** | `Query.logs(filters)` | Service, level, time range and message substring, AND-composed; omitted filters ignored; every path capped by `limit` | C3 |
| **Aggregate stats** | `Query.logStats(startTime, endTime)` | `totalLogs` / `errorCount` / `services` computed in SQL with `GROUP BY`, never by pulling rows into Python | C4 |
| **Log creation** | `Mutation.createLog(logData)` | Persists an entry, returns the created object, and publishes it to every matching subscriber | C4 / C6 |
| **Real-time streaming** | `Subscription.logStream(service, level)` | New entries over WebSocket, filtered **server-side before yielding**, one bounded queue per subscriber | C6 |
| **Trace correlation** | `LogEntry.relatedLogs` | Everything sharing a `traceId` — and an empty list, not an error, when there is no trace id | C5 |
| **Cross-entity query** | `Query.orders(filters)` + nested `user` / `payments` / `logs` | One query returning what REST would need 3+ calls for, with filters composing across dimensions | C10 / C11 |
| **Order status stream** | `Subscription.orderStatusStream(orderId, status, userId)` | Status transitions as they happen, under a 100 ms end-to-end delivery budget | C12 |

And the four things standing between that surface and a denial of service:

| Guard | Mechanism | Lands |
|---|---|---|
| **N+1 elimination** | Per-**operation** DataLoaders — N field resolutions become one SQL round trip, proven by a statement counter, not a comment | C5 |
| **Result caching** | Redis cache-aside keyed by `sha256` of the sorted-JSON filter set, 30 s TTL; a hit reconstructs fully typed objects with **zero** SQL | C7 |
| **Cost gating** | A custom `ValidationRule` scoring the AST *before* execution — over-budget operations are refused with the computed cost and the limit in `extensions` | C8 |
| **Persisted queries** | Clients send a `sha256` instead of a document; the server recomputes the hash on registration and refuses a mismatch | C9 |

---

## Architecture

One `python:3.11-slim` container running uvicorn on `:8000`, plus Postgres 16 and Redis 7. The React SPA is built in a `node:20-alpine` stage and copied into the *same* Python image — one origin, no CORS on the dashboard path, no nginx sidecar.

```
        ┌──────────────── one origin, container :8000 ───────────────┐
POST /graphql ─┤ HTTP transport ┐                                    │
WS   /graphql ─┤ WS transport  ─┼─ parse → validate → COST GATE → execute
GET  /health  ─┤ plain ASGI                                          │
GET  /metrics ─┤ Prometheus text exposition                          │
GET  / , /assets/* ← React SPA (StaticFiles, same origin)            │
        └────────────────────────┬───────────────────────────────────┘
                                 │
      ┌──────────────┬───────────┴────────┬──────────────────┐
      ▼              ▼                    ▼                  ▼
  Query.logs     Query.logStats   Mutation.createLog   Subscription
  Query.orders   (SQL GROUP BY,   → INSERT → publish   .logStream
  nested user/    never row-pull)                      .orderStatusStream
  payments             │                    │                ▲
      │                │                    │                │
      └── DataLoader ──┴─ Redis cache ──────┤          bounded queue
          (per-operation)  (sha256(filter),  │          per subscriber
                            TTL 30s)         │                ▲
                             │               ▼                │
                     ┌───────┴───────┐  ┌─────────────────────┴──┐
                     │ Redis 7       │  │ Broker: in-proc fan-out │
                     │ cache + APQ   │◄─┤ + Redis pub/sub bridge  │
                     │ + pub/sub     │  │ (survives N workers)    │
                     └───────────────┘  └────────────────────────┘
                             │
                     ┌───────┴────────────────────────────────┐
                     │ Postgres 16 — SQLAlchemy 2.0 async     │
                     │ log_entries / order_events /           │
                     │ user_events / payment_events           │
                     └────────────────────────────────────────┘
```

**Three decisions worth stating up front.**

1. **Loaders and sessions are per-*operation*, not per-connection.** Strawberry resolves `context_getter` **once per WebSocket connection**, not once per operation. A DataLoader or `AsyncSession` created there would live for the entire life of the socket — a stale-cache and connection-leak bug that looks fine in an HTTP test and misbehaves only over a long subscription. So a `PerOperationResources` `SchemaExtension` owns the lifecycle in `on_operation`: it mints a fresh loader registry and, for query/mutation operations only, one `AsyncSession` shared by every loader batch in that operation. **Subscription operations get no long-lived session** — the resolver opens a short-lived one per yielded item.

2. **The cost gate runs in validation, before execution.** Rejecting inside a resolver is too late: by then the expensive part has started. A custom `graphql.validation.ValidationRule` walks the operation AST, giving each field a static weight and multiplying list fields by their requested `first`/`limit` (assuming `DEFAULT_QUERY_LIMIT` when omitted, so an unbounded list cannot score zero), resolving fragments as it goes. Stacked with `QueryDepthLimiter`, `MaxTokensLimiter` and `MaxAliasesLimiter`, which close the amplification routes complexity alone does not.

3. **The broker publishes locally *and* to Redis.** In-process fan-out serves subscribers on this worker; a `PUBLISH` on the same event, plus a background `psubscribe` reader, lets subscriptions survive `uvicorn --workers N`. A worker ignores its own echo via a per-process publisher id, and Redis being unreachable degrades to in-process delivery rather than crashing — the same rule the cache follows.

### Module layout

```
graphql-log-query/
├── Dockerfile              # multi-stage from C13: node:20-alpine build → python:3.11-slim runtime
├── Dockerfile.test         # same base + tests/ + scripts/, runs as root, CMD ["pytest"]
├── docker-compose.yml      # api + postgres + redis; profile-gated test / e2e / loadtest
├── Makefile                # help build up down logs test test-unit test-int e2e load ui clean
├── pytest.ini              # testpaths=tests, addopts=-q --tb=short, asyncio_mode=auto
├── docker/postgres-init/   # creates the separate gqllogs_test database on first init
├── .dockerignore .env.example .gitignore README.md requirements.txt schema.graphql
├── src/
│   ├── config.py           # pydantic-settings Settings + @lru_cache get_settings()
│   ├── main.py             # lifespan, GraphQLRouter mount, /health, /metrics, SPA mount
│   ├── metrics.py          # CollectorRegistry: query time, field time, active subs, cache
│   ├── broker.py           # bounded per-subscriber queues + Redis pub/sub bridge
│   ├── cache.py            # sha256(sorted-JSON) keys, TTL, single-flight, never-raises
│   ├── generators.py       # deterministic seed corpus (startup seed + test oracle)
│   ├── api/health.py       # GET /health, GET /metrics
│   ├── db/
│   │   ├── base.py         # DeclarativeBase
│   │   ├── models.py       # LogEntryORM, OrderEventORM, UserEventORM, PaymentEventORM
│   │   ├── session.py      # create_async_engine, async_sessionmaker, init_db retry loop
│   │   └── repository.py   # filter→SELECT builder, GROUP BY aggregates, batch loaders
│   ├── graphql/
│   │   ├── types.py inputs.py enums.py     # LogEntry, LogConnection, LogFilterInput…
│   │   ├── query.py mutation.py subscription.py
│   │   ├── ecommerce.py    # LogEvent interface + Order/User/Payment types & resolvers
│   │   ├── loaders.py      # DataLoader factories, LoaderRegistry
│   │   ├── context.py      # Context(BaseContext) + PerOperationResources extension
│   │   ├── cost.py         # depth + complexity ValidationRule
│   │   ├── apq.py          # automatic persisted queries SchemaExtension
│   │   ├── errors.py       # extensions.code taxonomy, MaskErrors wiring
│   │   └── schema.py       # assembled strawberry.Schema + SDL export
│   └── web/                # ← React build output lands here at image build time
├── web/                    # React SPA source (Vite); web/dist gitignored
│   ├── package.json vite.config.js index.html
│   └── src/ App.jsx apollo.js components/ hooks/ queries/
├── scripts/ __init__.py verify_e2e.py load_test.py export_sdl.py
└── tests/ conftest.py unit/ integration/
```

At **C1**, what exists is `src/config.py`, `src/main.py`, `src/api/health.py`, `scripts/__init__.py` and the test skeleton; the rest is the map the following commits fill in.

---

## Tech Stack

- **Language / runtime:** Python 3.11 (Node 20 for the frontend build stage only)
- **GraphQL:** `strawberry-graphql[fastapi]` — code-first schema, one `GraphQLRouter` serving both the HTTP POST transport and `graphql-transport-ws` (plus legacy `graphql-ws`) on the same path
- **API / ASGI:** FastAPI 0.115 + `uvicorn[standard]` (the `websockets` extra is not optional here — subscriptions are a real WebSocket upgrade)
- **Persistence:** SQLAlchemy 2.x with the asyncio extra + **asyncpg** against Postgres 16. Not psycopg2: every resolver is a coroutine, and a blocking driver would park the event loop for the length of each query
- **Cache / pub-sub:** Redis 7 via `redis-py` (`redis.asyncio`, in-tree — no separate aioredis) — result cache, persisted query documents, and the cross-worker subscription bridge
- **Cost analysis:** a custom `graphql-core` `ValidationRule` (Strawberry ships depth/token/alias limiters but **no** complexity extension), installed via `AddValidationRules`
- **Config:** Pydantic v2 + pydantic-settings
- **Observability:** `prometheus-client` with an explicit `CollectorRegistry`, text exposition on `/metrics`
- **Frontend:** React 18 + Vite, Apollo Client with a `split` link (WS for subscriptions, HTTP for everything else), `graphql-ws`
- **Testing:** pytest + `pytest-asyncio` (`asyncio_mode = auto`) + `httpx` + raw `websockets` for the subscription E2E
- **Infra:** Docker + Docker Compose. Three long-lived services (`api`, `postgres`, `redis`); the `test` / `e2e` / `loadtest` services are profile-gated and never start on a bare `docker compose up`

---

## GraphQL Surface

One endpoint, three operation types. `POST /graphql` carries queries and mutations; `GET /graphql` serves GraphiQL when `GRAPHQL_PLAYGROUND_ENABLED` is on; the WebSocket upgrade on the same path carries subscriptions. The full SDL is committed to [`schema.graphql`](schema.graphql) from C3, and a test fails when the committed copy drifts from the live schema.

| Type | Operation | Arguments | Returns | Lands |
|---|---|---|---|---|
| **Query** | `logs` | `filters: LogFilterInput` (`service`, `level`, `startTime`, `endTime`, `searchText`, `limit`), plus cursor args `first` / `after` | `LogConnection` (edges + `pageInfo`) | C3 |
| **Query** | `log` | `id: ID!` | `LogEntry` (null when absent) | C3 |
| **Query** | `logStats` | `startTime`, `endTime` | `LogStats` — `totalLogs`, `errorCount`, `services` | C4 |
| **Query** | `orders` | `OrderFilterInput` (status + time window + user attribute, composed) | `[OrderEvent!]!`, each traversable to `user`, `payments`, `logs` | C10 / C11 |
| **Mutation** | `createLog` | `logData: LogInput!` | `LogEntry!` — persisted, returned, and published to subscribers | C4 |
| **Subscription** | `logStream` | `service: String`, `level: LogLevel` | `LogEntry!` — filtered **server-side** before yielding | C6 |
| **Subscription** | `orderStatusStream` | `orderId: ID`, `status: OrderStatus`, `userId: ID` | `OrderEvent!` | C12 |

Field-level notes worth knowing before you write a query:

- **`level` is a `LogLevel` enum**, never a free string. An invalid level is a validation error against the schema, not a query that quietly matches nothing.
- **`LogEntry.relatedLogs`** returns every entry sharing this one's `traceId`, and an **empty list** when `traceId` is null — a null trace id is an ordinary state, not an error.
- **`limit` is clamped, not rejected.** Asking for 10,000 returns `MAX_QUERY_LIMIT` rows. Every list path applies the cap, including nested ones, so a nested traversal cannot escape it.
- **A `LogEvent` interface** (from C10) carries the fields every event type shares — `timestamp`, `service`, `level`, correlation id — and is implemented by `LogEntry`, `OrderEvent`, `UserEvent` and `PaymentEvent`, so a client can query across all four in one selection.
- **Errors are GraphQL errors.** Every failure carries a typed `extensions.code` (`VALIDATION_ERROR`, `NOT_FOUND`, `COST_LIMIT_EXCEEDED`, `PERSISTED_QUERY_NOT_FOUND`, `INTERNAL_ERROR`); `MaskErrors` keeps stack traces off the wire, and no operation ever produces a bare HTTP 500.

<!-- filled in at C3–C12: worked query/mutation/subscription examples with real request and response payloads, once each operation exists to produce them -->

---

## How to Run

Docker Compose brings up the whole stack — API + Postgres + Redis — with one command. The container always binds `:8000`; the **host** port is compose-level and overridable via `API_PORT`, which defaults to **8020** because sibling projects in this repo already hold `:8000` and `:8010`.

```bash
make up                       # postgres + redis, then the api once both are healthy
                              # API:     http://localhost:8020
                              # GraphQL: http://localhost:8020/graphql   (GET → GraphiQL)

curl -s http://localhost:8020/health
# {"status":"healthy"}
```

From C3, the spec's own verification commands work against the live container:

```bash
# a plain query
curl -s -X POST http://localhost:8020/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ logs { id service level message } }"}'

# variable-driven filtered query
curl -s -X POST http://localhost:8020/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "query($f: LogFilterInput) { logs(filters: $f) { id service level } }",
       "variables": {"f": {"level": "ERROR", "service": "auth-svc", "limit": 20}}}'

# aggregate summary
curl -s -X POST http://localhost:8020/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ logStats { totalLogs errorCount services } }"}'

# create a log entry (also published to every matching subscriber)
curl -s -X POST http://localhost:8020/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "mutation { createLog(logData: {service: \"auth-svc\", level: ERROR, message: \"invalid token\"}) { id service } }"}'
```

Override the host port the same way for everything: `API_PORT=9000 make up`.

### Make targets

| Target | What it does |
|---|---|
| `help`      | List the targets (the default goal) |
| `build`     | Build both images (api + test), naming each service so the profile-gated one is not skipped |
| `up`        | Run the stack detached; print the API + GraphiQL URLs |
| `down`      | Stop and remove the stack |
| `logs`      | Tail the API logs |
| `test`      | Full pytest suite in Docker (unit + integration; prunes volumes, then rebuilds the tester image) |
| `test-unit` | Unit tests only, in Docker |
| `test-int`  | Integration tests only, in Docker |
| `e2e`       | Black-box E2E verifier vs the live stack (C12) |
| `load`      | Perf/load gates vs the live stack — throughput, latency, errors, memory (C14) |
| `ui`        | `up` plus the dashboard URL — the dashboard is the *same* process, not a second service |
| `clean`     | `down` + remove volumes and orphans |

Every test-ish target captures the exit status, tears the stack down, and *then* re-raises it — so a failing test still fails the target and still leaves a clean machine behind.

---

## Configuration

Settings are read from **field defaults → optional `.env` → environment variables**, each env var being the upper-cased field name (`max_query_depth` ← `MAX_QUERY_DEPTH`). The committed [`.env.example`](.env.example) documents every key; `docker-compose.yml` passes every one through as `${VAR:-default}`, so the whole surface is host-overridable and the compose file never has to change as features land.

| Setting | Default | Meaning |
|---|---|---|
| `HOST` | `0.0.0.0` | Informational inside the container — the image CMD hard-codes `--host 0.0.0.0` |
| `PORT` | `8000` | Informational inside the container — the image CMD hard-codes `--port 8000` |
| `LOG_LEVEL` | `INFO` | Root log level (`DEBUG` \| `INFO` \| `WARNING` \| `ERROR`); an unrecognised value degrades to `INFO` |
| `DATABASE_URL` | `postgresql+asyncpg://gqllogs:gqllogs@postgres:5432/gqllogs` | SQLAlchemy 2.x async URL. `postgres` is the compose service name, not a host |
| `DB_POOL_SIZE` | `10` | Persistent connections in the pool |
| `DB_MAX_OVERFLOW` | `5` | Extra connections allowed above the pool under burst |
| `DB_INIT_RETRIES` | `10` | Boot-time schema-create attempts before giving up |
| `DB_INIT_RETRY_DELAY_SECONDS` | `2.0` | Delay between those attempts, seconds |
| `REDIS_URL` | `redis://redis:6379/0` | One instance, three jobs: result cache, persisted queries, subscription pub/sub |
| `SEED_ENTRIES` | `2000` | Log rows generated at startup when the store is empty (`0` leaves it empty) |
| `SEED_ORDERS` | `200` | Correlated e-commerce orders generated alongside them |
| `RANDOM_SEED` | `20260725` | RNG seed — fixed so the corpus is reproducible and the E2E verifier can grade against it |
| `DEFAULT_QUERY_LIMIT` | `100` | `limit` when the client omits it; also the multiplier the cost gate assumes for an unbounded list |
| `MAX_QUERY_LIMIT` | `500` | Hard ceiling `limit` is clamped to on every path. Must be ≥ `DEFAULT_QUERY_LIMIT` or startup fails |
| `CACHE_ENABLED` | `true` | Operability switch for the Redis result cache |
| `CACHE_TTL_SECONDS` | `30` | Result-cache TTL — deliberately short; a log query is a point-in-time answer |
| `AGG_CACHE_TTL_SECONDS` | `60` | Separate, longer TTL for aggregations: costlier to compute, far less sensitive to one new row |
| `MAX_QUERY_DEPTH` | `10` | Max operation nesting. Must be ≥ 1 — a budget below 1 rejects every possible operation |
| `MAX_QUERY_COMPLEXITY` | `1000` | Complexity budget, scored from the AST before any resolver runs |
| `MAX_QUERY_TOKENS` | `2000` | Document-size ceiling — closes the "enormous but shallow" document depth does not catch |
| `MAX_QUERY_ALIASES` | `30` | Alias ceiling — closes one field requested ten thousand times under ten thousand names |
| `PERSISTED_QUERIES_ENABLED` | `true` | Accept `extensions.persistedQuery` hash-only requests |
| `PERSISTED_QUERY_TTL_SECONDS` | `3600` | How long a registered query document is retained in Redis |
| `DATALOADER_BATCH_WINDOW_MS` | `5` | Batch window; `0` batches within one event-loop tick |
| `SUBSCRIPTION_QUEUE_MAXSIZE` | `500` | Per-subscriber bounded queue; overflow drops the slow consumer. **Must be ≥ 1** — `asyncio.Queue` reads `0` as *unbounded* |
| `MAX_SUBSCRIPTIONS_PER_CONNECTION` | `10` | Cap on operations multiplexed over one WebSocket |
| `SUBSCRIPTION_CHANNEL` | `graphql-log-query:events` | Redis pub/sub channel bridging workers |
| `GRAPHQL_PLAYGROUND_ENABLED` | `true` | Serve GraphiQL on `GET /graphql` (POST is unaffected) |
| `METRICS_ENABLED` | `true` | Expose `GET /metrics` (Prometheus text exposition) |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins; credentials are disabled with `*` |

Two names deliberately absent from that table. **`API_PORT`** (default `8020`) is a *compose* variable — the host side of `${API_PORT:-8020}:8000` — and is not an application setting at all. The **verification gates** (`MAX_P95_MS`, `LOAD_MIN_RPS`, `E2E_SUB_LATENCY_MS`, …) are read by `scripts/verify_e2e.py` and `scripts/load_test.py` in a separate container, never by the backend; they are documented in `.env.example`.

---

## Testing

Everything is verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a hard-gated load harness, all profile-gated compose services. Nothing is run on the host.

```bash
make test        # unit + integration, in the test profile container
make e2e         # black-box verifier vs the live stack          (C12)
make load        # perf/load gates vs the live stack             (C14)
```

The `test` service points at a **separate `gqllogs_test` database** (created by `docker/postgres-init/` on first init) with `SEED_ENTRIES=0`, so the suite starts from an empty store and builds its own deterministic corpus. A `make test` can never destroy the corpus of a stack you have running beside it, and no expected count is inherited from rows the test did not write.

- **Unit** — configuration (every declared default matches the published table, every validator rejects its bad input with a message that explains why), the health contract, and from C3 onward: schema shape, filter composition, cursor encoding, the cost scorer against hand-built ASTs, and the broker's queue mechanics.
- **Integration** — the real application construction path *through the lifespan*, against live Postgres and Redis by service name: query/filter correctness, mutation behaviour, N+1 elimination proven with a SQLAlchemy `before_cursor_execute` statement counter, a cache hit issuing **zero** SQL, complexity rejection landing *before any resolver runs* (a resolver spy that stays at zero), and subscription delivery.
- **E2E** (`scripts/verify_e2e.py`, C12) — ordered black-box checks against a genuinely separate container over HTTP and a real `graphql-transport-ws` socket, exit 1 on the first failure.
- **Load** (`scripts/load_test.py`, C14) — concurrent query phases, a mixed query/mutation/nested workload, a subscription fan-out with a deliberate slow consumer, and the backend's own RSS.

### Measured results

<!-- filled in at C14: transcribed from the harness's own RESULT lines — test counts, E2E check tally, sequential and under-load p95, throughput, cache hit ratio, subscription delivery latency, backend RSS. No numbers are written here until they have been measured. -->

### The gates actually bite

<!-- filled in at C14: the must-fail proofs (`MAX_P95_MS=0 make e2e`, `LOAD_MIN_RPS=1000000 make load`) with their exit statuses -->

### Dashboard verification

<!-- filled in at C13: Chrome UI walkthrough — filter refetch, live stream prepend + buffer cap, optimistic update, order status transition, console and network evidence -->

---

## What I Learned

<!-- filled in at C14: the full set, written from what the build actually taught rather than from what was expected going in -->

The two that were already true before a line of schema was written:

- **`asyncio.Queue(maxsize=0)` means *unbounded*, which makes the safest-looking configuration the most dangerous one.** Subscription back-pressure is the one place in this project where a stalled client can grow the server's memory, and the bound is a single integer. The trap is that somebody tightening that bound reaches for the smallest number they can type — and `0` does not mean "hold nothing", it means "hold everything". The type system cannot express the difference: `0` is a perfectly good `int`. So `SUBSCRIPTION_QUEUE_MAXSIZE` gets a validator that refuses it at startup with a message that says *why*, because the alternative is a process that starts cleanly, passes every test, and OOMs the first time a browser tab is left open on a slow connection. The general lesson is that a validator earns its place when the value it rejects is one a *reasonable* person would type on purpose — not when it merely guards against nonsense.

- **A liveness probe that checks its dependencies is not a liveness probe.** `/health` here touches neither Postgres nor Redis, and that is a deliberate refusal rather than an unfinished implementation. The container `HEALTHCHECK`, compose's `condition: service_healthy`, and the `e2e`/`loadtest` services' `depends_on` all hang off this one route — so if it reported unhealthy while Postgres reconnected, Docker would restart a process that is working perfectly, and a transient database blip would become a container restart loop *and* a flapping gate that the harnesses wait on. Readiness of the data layer is a startup concern, handled by a retry loop in the lifespan; liveness answers exactly one question, "is this process listening?", and answering more than that costs availability. The structural version of the rule: the handler takes no arguments at all, so there is nothing a later commit can quietly give it to depend on.

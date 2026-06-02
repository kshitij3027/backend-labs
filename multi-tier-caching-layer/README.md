# Multi-Tier Caching Layer

A layered **read-through cache** (in-memory → Redis → pre-computed DB results → slow backend)
that sits in front of an expensive log-query backend, **learns query patterns**, and
**proactively warms hot data** — cutting query latency from hundreds of milliseconds to
microseconds.

**Status: complete.** Three cache tiers + a heuristic pattern learner + a background warmer
are wired behind a FastAPI service, with a live Chart.js/WebSocket dashboard, and verified in
Docker against real Redis and PostgreSQL (unit + integration + cross-container E2E + a load
test). Measured ~120x speedup on repeat queries and ~85% hit rate under load.

---

## The Problem

Querying raw logs is expensive. A typical log-query backend scans large amounts of data per
request, so even simple, repeated queries can take **hundreds of milliseconds**. In practice, a
small fraction of queries (recent time ranges, popular filters, dashboards that refresh on a
timer) account for most of the traffic. Serving those repeatedly from the slow backend is
wasteful.

This project puts a **tiered cache** in front of that backend. Frequently requested results
are served from progressively faster layers, and a background learner predicts which data is
about to be "hot" and warms it **before** the request arrives.

---

## Architecture

```
                     ┌──────────────────┐
   client ─────────► │   FastAPI app    │  /query, /cache/*, /patterns   ◄──── live dashboard (WS)
                     └────────┬─────────┘
                              │ read-through lookup
        ┌─────────────────────┼──────────────────────────────────────┐
        ▼                     ▼                                        ▼
  ┌────────────┐        ┌───────────┐                          ┌────────────────┐
  │ L1: memory │  miss  │ L2: Redis │  miss                    │ L3: pre-computed│
  │ (LRU + TTL)│ ─────► │ (sibling  │ ───────────────────────► │   DB results    │
  │ in-process │        │ container)│                          │ (materialized)  │
  └────────────┘        └───────────┘                          └────────┬───────┘
                                                                         │ miss
                                                                         ▼
                                                                ┌────────────────┐
                                                                │  log-query     │
                                                                │  backend (slow)│
                                                                └────────────────┘

   Query Pattern Learner ── observes ──► live query stream (frequency · recency · cost)
   Proactive Warmer       ── populates ──► L1 / L2 with predicted-hot keys, refreshes near-expiry
```

A request walks the tiers in order. On a hit, the result is returned immediately and
**back-filled upward** into the faster tiers it missed. On a full miss it falls through to the
log-query backend, then the result is populated upward. A **single-flight** guard collapses
concurrent identical misses into one backend call, so a cold popular key can never start a
cache stampede.

### Cache Tiers

| Tier | Backing store | Scope | Typical latency | Role |
|------|---------------|-------|-----------------|------|
| **L1** | In-process LRU + TTL (`cachetools`) | Per app instance | µs | Hottest, smallest working set |
| **L2** | Redis (sibling container, `redis.asyncio`) | Shared across instances | sub-ms | Larger shared hot set; zstd-compressed blobs |
| **L3** | Pre-computed / materialized DB results (Postgres) | Durable | ms | Aggregations computed ahead of time |
| **Source of truth** | Log-query backend (synthetic slow path) | — | ~150 ms | Slow path, only on full miss |

### Intelligence Layer

- **Query Pattern Learner** (`src/patterns.py`) — observes the live query stream and scores
  entries by **frequency**, **recency**, and **backend cost** (a frecency-with-cost heuristic)
  to maintain a ranking of "hot" keys, plus temporal (hour-of-day / day-of-week) and per-source
  histograms.
- **Proactive Warmer** (`src/warmer.py`) — a background task that periodically replays the
  top-N recommendations through the cache manager, pre-populating L1/L2 with predicted-hot
  entries so a cache miss never reaches a real user for popular queries.

---

## Tech Stack

- **Language:** Python 3.12
- **API framework:** FastAPI 0.115 + Uvicorn (long-lived ASGI service)
- **L1 cache:** `cachetools` (in-process LRU/TTL, guarded by an explicit lock for thread safety)
- **L2 cache:** Redis via `redis.asyncio` (sibling container; per-call timeouts + fail-soft)
- **L3 / backend store:** PostgreSQL via `asyncpg` (+ SQLAlchemy) — holds raw logs and
  pre-computed aggregate tables
- **Compression:** `zstandard` (zstd) for L2 time-series blobs
- **Config / validation:** Pydantic v2 + `pydantic-settings`
- **Dashboard:** vanilla HTML + vendored Chart.js + a `/ws/metrics` WebSocket (`websockets`)
- **Testing:** pytest + pytest-asyncio + httpx (+ `websockets` for the E2E WS check)
- **Deployment:** Docker Compose (app + Redis + Postgres as siblings)

---

## How to Run

Everything runs through the `Makefile` (which wraps `docker compose`). The Postgres
`db-init` step **seeds ~200,000 synthetic `raw_logs` rows automatically** on first start, so
queries hit real data with no manual setup.

```bash
# from this project folder
make build        # build the app + tester images
make up           # start the app (+ Redis + Postgres) at http://localhost:8000
```

Then open the live dashboard and probe health:

```bash
open http://localhost:8000/          # live Chart.js / WebSocket dashboard
curl http://localhost:8000/health    # -> {"status":"healthy"}
```

Run the **same query twice** to watch it flip from a slow backend/L3 miss to an L1 hit. The
`meta.tier` field shows which tier served the result and `meta.elapsed_ms` the latency:

```bash
# 1st call — cold: served from L3 / backend (~150-180 ms)
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"error_rate","params":{"source":"api","start":0,"end":2000000000,"bucket":"hour"}}'
# -> {"result":..., "meta":{"tier":"backend","elapsed_ms":167.4, ...}}

# 2nd call — warm: served from L1 (<1 ms)
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"error_rate","params":{"source":"api","start":0,"end":2000000000,"bucket":"hour"}}'
# -> {"result":..., "meta":{"tier":"l1","elapsed_ms":0.4, ...}}

# Inspect cache effectiveness (overall + per-tier hit rates, memory, timing)
curl -s http://localhost:8000/cache/stats

# Invalidate matching entries across all tiers (by glob pattern or tags)
curl -s -X POST http://localhost:8000/cache/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"pattern":"q:*"}'

# See the learned query patterns + warming recommendations
curl -s http://localhost:8000/patterns
```

Verification targets (all run **inside Docker** — never on the host):

```bash
make test         # full suite: unit + integration + e2e (real Redis + Postgres)
make e2e          # cross-container E2E verifier against the live stack
make load         # containerized load test (throughput + cached-p90 gates)
make clean        # stop the stack and remove volumes
make logs         # tail app logs
make down         # stop and remove the stack (keeps volumes)
```

(`make test-unit` / `make test-int` / `make test-e2e` run the suites individually.)

---

## API

Base URL: `http://localhost:8000`.

### REST

| Method | Path | Request body | Response |
|--------|------|--------------|----------|
| `GET`  | `/` | — | Live monitoring dashboard (HTML + Chart.js). |
| `GET`  | `/health` | — | `{"status":"healthy"}`. |
| `POST` | `/query` | `{"query":"<error_rate\|requests_over_time\|avg_latency\|top_sources>", "params":{"source":..,"start":<epoch>,"end":<epoch>,"bucket":"hour"}}` | `{"result":..., "meta":{"tier":"l1\|l2\|l3\|backend","elapsed_ms":..,"key":..,"degraded":..}}`. Unknown query → `400`; malformed body → `422`. |
| `GET`  | `/cache/stats` | — | `{"performance":{overall_hit_rate,total_requests,hits,misses}, "tiers":{l1,l2,l3}, "memory":{l1_mb,cap_mb,total_mb}, "timing_ms":..., "degraded":..., "alert":...}`. |
| `GET`  | `/cache/hot` | — | Ranked hot keys (frecency-with-cost score), top 20. |
| `POST` | `/cache/warm` | `{"queries":[{"query":..,"params":..}], "top_n":N}` | `{"warmed":N}`. Empty `queries` → one recommendation-driven sweep. |
| `POST` | `/cache/invalidate` | `{"pattern":"q:*"}` **or** `{"tags":["source:api"]}` | `{"l1":n,"l2":n,"l3":n}` per-tier removal tally. Requires at least one of `pattern`/`tags` (else `422`). |
| `GET`  | `/patterns` | — | `{"temporal":{hour_of_day,day_of_week}, "per_source":..., "total_observations":..., "recommendations":[...]}`. |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws/metrics` | Pushes an immediate snapshot on connect, then a `{"type":"tick", "stats":..., "series":..., "recommendations":[...], "degraded":..}` payload every `WS_PUSH_INTERVAL_SECONDS`. Drives the dashboard. |

---

## Configuration (env vars)

All fields are overridable via env var (case-insensitive) or a `.env` file (see
`.env.example`, which lists **every** setting with its default); defaults live in
`src/settings.py`.

| Variable | Default | Notes |
|----------|---------|-------|
| `L1_MAX_SIZE` | `1000` | Max L1 entries before LRU eviction. |
| `L1_TTL` | `300` | L1 entry TTL, seconds. |
| `REDIS_URL` | *(empty)* | Explicit L2 Redis URL; if empty, derived as `redis://REDIS_HOST:REDIS_PORT/0`. |
| `REDIS_HOST` | `redis` | L2 Redis host (used when `REDIS_URL` is empty). |
| `REDIS_PORT` | `6379` | L2 Redis port (used when `REDIS_URL` is empty). |
| `L2_TTL_SECONDS` | `600` | L2 (Redis) entry TTL, seconds. |
| `L2_TIMEOUT` | `2.0` | Per-call Redis timeout, seconds (enables fail-soft degradation). |
| `L2_COMPRESS` | `true` | Compress L2 time-series blobs with zstd. |
| `DATABASE_URL` | `postgresql://cache:cache@postgres:5432/cache` | L3 / backend Postgres DSN. |
| `BACKEND_DELAY_MS` | `150` | Artificial slow-backend latency for the demo, ms. |
| `TIME_BUCKET_SECONDS` | `300` | Timestamp bucketing granularity for cache-key normalization. |
| `WARMER_INTERVAL_SECONDS` | `5.0` | How often the proactive warmer sweeps. |
| `WARMER_TOP_N` | `20` | How many hot recommendations the warmer replays per sweep. |
| `CACHE_MEM_CAP_MB` | `200` | Total cache memory cap across tiers (reported in `/cache/stats`). |
| `DASHBOARD_POINTS` | `60` | Trailing data points retained for the dashboard series. |
| `WS_PUSH_INTERVAL_SECONDS` | `2.0` | Dashboard WebSocket broadcast cadence, seconds. |
| `API_PORT` | `8000` | HTTP listen port. |
| `LOG_LEVEL` | `INFO` | Log level. |

(Additional knobs — `L1_MEMORY_MB`, `L2_MAX_MB`, the `PATTERN_*` weights/half-life,
`DEGRADATION_HIT_RATE_THRESHOLD`, `SEED_ROWS`/`SEED_RANDOM_SEED`, `API_HOST`, `DASH_PORT` —
are also configurable; see `src/settings.py` / `.env.example`.)

---

## Results / success criteria

Verified in Docker (`make test`, `make e2e`, `make load`):

| Success criterion | Result |
|-------------------|--------|
| Repeated queries served from a faster tier | Repeat-query hit: cold L3/backend ~150–180 ms → warm **L1 < 1 ms** (`meta.tier` flips `backend`→`l1`). |
| High hit rate / large latency win under load | Load test ~**790 req/s**, hit rate ~**85%**, cached **p90 ≈ 3 ms**, ~**120x** speedup vs. the slow backend. |
| Graceful degradation when L2 is down | Stopping the Redis container keeps `/query` returning **200** and `/health` **healthy**; responses carry `degraded:true` and a `/cache/stats` alert (L2 fail-soft, traffic falls through to L3/backend). |
| Proactive warming of predicted-hot data | Background warmer replays top-N recommendations each sweep; `POST /cache/warm` warms on demand (`{"warmed":N}`). |
| Live observability | Dashboard updates live over the `/ws/metrics` WebSocket (per-tier hit rates, memory, timing, recommendations). |

---

## What I Learned

- **Read-through hierarchy with upward backfill** beats a flat cache: a hit at L2/L3 should
  populate the faster tiers it skipped, so the *next* request lands one tier higher. Getting
  the populate-upward direction right is what turns a 150 ms query into a sub-millisecond one.
- **`cachetools` is not thread-safe on its own** — its LRU/TTL containers need an explicit lock
  around mutating operations, or concurrent requests corrupt the structure.
- **Redis must fail soft** for graceful degradation: wrap every call in a per-call timeout and
  catch all exceptions so an L2 outage degrades to L3/backend (still `200`) instead of taking
  the whole service down. The `degraded` flag and a stats alert make that visible.
- **Semantic cache-key normalization** is the heart of the hit rate: canonicalize params
  (sort keys, normalize types), **bucket timestamps** to a fixed granularity so near-identical
  time ranges collide on the same key, then hash with **SHA-256** for a stable, compact key.
- **Single-flight** prevents a cache stampede: collapse concurrent identical misses into one
  in-flight backend call and fan the result out, so a cold popular key never triggers a
  thundering herd.
- **zstd is a strong fit for time-series blobs** in L2 — repetitive aggregate JSON compresses
  hard, cutting Redis memory and network bytes for a tiny CPU cost.
- A **frecency heuristic** (frequency × recency × backend cost) is a cheap, explainable way to
  predict hot keys for warming — it rivals an ML ranker here without the training/serving
  overhead.
- **Testing cache tiers properly means real dependencies**: spinning up actual Redis + Postgres
  in Docker (not mocks) is the only way to trust degradation, TTLs, and the cross-container E2E
  data flow.

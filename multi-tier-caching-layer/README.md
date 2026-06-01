# Multi-Tier Caching Layer

A layered **read-through cache** (in-memory → Redis → pre-computed DB results) that sits in
front of a slow log-query backend, **learns query patterns**, and **proactively warms hot
data** — cutting query latency from seconds to milliseconds.

---

## The Problem

Querying raw logs is expensive. A typical log-query backend scans large amounts of data per
request, so even simple, repeated queries can take **seconds**. In practice, a small fraction
of queries (recent time ranges, popular filters, dashboards that refresh on a timer) account
for most of the traffic. Serving those repeatedly from the slow backend is wasteful.

This project puts a **tiered cache** in front of that backend. Frequently requested results
are served from progressively faster layers, and a background learner predicts which data is
about to be "hot" and warms it **before** the request arrives.

---

## Architecture

```
                     ┌──────────────────┐
   client ─────────► │   FastAPI app    │  /query, /cache/*   ◄──── optional dashboard (HTTP)
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

### Cache Tiers

| Tier | Backing store | Scope | Typical latency | Role |
|------|---------------|-------|-----------------|------|
| **L1** | In-process LRU + TTL (`cachetools`) | Per app instance | µs | Hottest, smallest working set |
| **L2** | Redis (sibling container) | Shared across instances | sub-ms | Larger shared hot set |
| **L3** | Pre-computed / materialized DB results | Durable | ms | Aggregations computed ahead of time |
| **Source of truth** | Log-query backend | — | seconds | Slow path, only on full miss |

A request walks the tiers in order. On a hit, the result is returned immediately (and
optionally back-filled into faster tiers). On a full miss it falls through to the log-query
backend, then the result is populated upward.

### Intelligence Layer

- **Query Pattern Learner** — observes the live query stream and scores entries by
  **frequency**, **recency**, and **backend cost** to maintain a ranking of "hot" keys.
- **Proactive Warmer** — a background task that pre-populates L1/L2 with predicted-hot
  entries and refreshes hot keys that are close to expiry, so a cache miss never reaches a
  real user for popular queries.

---

## Tech Stack

- **Language:** Python 3.12
- **API framework:** FastAPI + Uvicorn (long-lived ASGI service)
- **L1 cache:** `cachetools` (in-process LRU/TTL)
- **L2 cache:** Redis (async client, runs as a sibling container)
- **L3 / backend store:** PostgreSQL via `asyncpg` (+ SQLAlchemy) — holds raw logs and
  pre-computed result tables
- **Config / validation:** Pydantic + pydantic-settings
- **Testing:** pytest + pytest-asyncio + httpx
- **Deployment:** Docker Compose (app + Redis + DB as siblings)

---

## Planned API

> Endpoint shapes are indicative and will be finalized during implementation.

| Method & path | Purpose |
|---------------|---------|
| `POST /query` | Run a log query through the cache hierarchy. Returns the result plus metadata showing **which tier served it** (L1/L2/L3/backend) and timing. |
| `GET /cache/stats` | Per-tier hit/miss ratios and latency percentiles. |
| `GET /cache/hot` | Current learned hot-key ranking. |
| `POST /cache/warm` | Manually trigger warming for a set of queries/keys. |
| `POST /cache/invalidate` | Evict / invalidate entries (single key or pattern). |
| `GET /health` | Liveness / readiness probe. |

---

## How to Run

> **Project status:** Scaffold only. This folder currently contains the `README.md`,
> `requirements.txt`, and `.gitignore`. Application code, the `Dockerfile`, and
> `docker-compose.yml` will be added in the implementation phase (pending approval).

Once implemented, the intended workflow is:

```bash
# from this project folder
docker compose up --build        # starts FastAPI app + Redis (+ Postgres) as siblings

# run a query through the cache layers
curl -X POST localhost:8000/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "...", "time_range": "..."}'

# inspect cache effectiveness
curl localhost:8000/cache/stats
```

### Configuration (planned env vars)

| Variable | Description |
|----------|-------------|
| `REDIS_URL` | Connection URL for the L2 Redis tier |
| `DATABASE_URL` | Connection URL for the L3 / log-query backend |
| `L1_MAX_SIZE` / `L1_TTL_SECONDS` | In-memory tier sizing and expiry |
| `L2_TTL_SECONDS` | Redis tier expiry |
| `WARMER_INTERVAL_SECONDS` | How often the proactive warmer runs |

A `.env.example` with placeholder values will be added alongside the implementation.

---

## What I Learned

<!-- Filled in as the project evolves. Topics to capture:
     - Read-through vs. cache-aside trade-offs across tiers
     - Designing cache keys for log queries (normalization, time-bucketing)
     - Measuring per-tier hit ratios and latency percentiles
     - Predicting hot data: frequency + recency + cost scoring
     - Avoiding thundering-herd / cache-stampede on expiry -->

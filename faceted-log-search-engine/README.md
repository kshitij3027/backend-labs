# Faceted Log Search Engine

A multi-dimensional faceted search system that lets users filter structured logs across several attributes (service, level, region, response time, time-of-day) simultaneously, with real-time facet counts updated on every query.

---

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** FastAPI (REST API + Jinja2-served dashboard)
- **Cache:** Redis 7 (cache-aside on full search responses)
- **Primary Store:** SQLite with WAL mode and STORED generated columns
- **Frontend:** Vanilla JS dashboard (no build step, served by FastAPI)
- **Containerization:** Docker & Docker Compose
- **Testing:** pytest, pytest-asyncio, httpx (74 tests, all green in Docker)

---

## What This Project Does

Traditional log search returns a flat list. **Faceted search** lets a user progressively narrow results by clicking facet values — e.g., start with *all logs*, then filter to `service=auth`, then `level=ERROR`, then `region=us-east-1`, while the UI continuously shows how many matching logs fall into each remaining facet bucket.

Every filter combination is answered in real time, and each response carries the full facet breakdown so the UI can re-render counts alongside results. Sibling facet values inside a selected dimension stay visible with their counts (the "excluded-self" pattern) so the user can always broaden the selection.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                           │
│                                                              │
│  ┌──────────────────────┐    ┌────────────────────────────┐ │
│  │    FastAPI Backend    │    │           Redis            │ │
│  │                       │◄──►│  - Hot facet counts         │ │
│  │  ┌─────────────────┐ │    │  - Query result cache       │ │
│  │  │  Ingest API      │ │    │  - Top-N value sets         │ │
│  │  └─────────────────┘ │    └────────────────────────────┘ │
│  │  ┌─────────────────┐ │                                    │
│  │  │  Search API      │ │    ┌────────────────────────────┐ │
│  │  │  (facet filters) │ │◄──►│          SQLite            │ │
│  │  └─────────────────┘ │    │  - Persistent log store     │ │
│  │  ┌─────────────────┐ │    │  - Indexed facet columns    │ │
│  │  │  Facet Counter   │ │    │  - Range queries (latency, │ │
│  │  │  (aggregations)  │ │    │    timestamp)              │ │
│  │  └─────────────────┘ │    └────────────────────────────┘ │
│  │  ┌─────────────────┐ │                                    │
│  │  │  Static UI Host  │ │  GET /  (dashboard)               │
│  │  └─────────────────┘ │                                    │
│  └──────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Facet Dimensions

| Facet | Type | Example Values |
|-------|------|----------------|
| `service` | categorical | `auth`, `payments`, `api-gateway`, `cache` |
| `level` | categorical | `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` |
| `region` | categorical | `us-east-1`, `us-west-2`, `eu-west-1`, `ap-south-1` |
| `latency_bucket` | numeric range | `0-100ms`, `100-500ms`, `500ms-2s`, `2s+` |
| `hour_bucket` | temporal bucket | integer `0`-`23` (hour of day, UTC) |

Any combination of filters can be applied simultaneously. The response always contains:
- **Matching log entries** (keyset-paginated)
- **Facet counts** for every dimension, reflecting the current filter set with excluded-self semantics

---

## How It Runs

```
git clone <repo>
cd faceted-log-search-engine

cp .env.example .env

make build           # build app + test images
make up              # start app + redis (waits on /health)
make ui              # opens http://localhost:8000 in default browser
make demo            # walk a realistic facet drill-down
make load            # async p95/qps harness against the running stack
make test            # full pytest suite inside Docker
make e2e             # start stack, run e2e tests, tear down
make logs            # tail app container logs
make down            # stop the stack
make clean           # down -v + remove local images
```

All commands go through Docker — no Python environment needed on the host. The app listens on port 8000; Redis listens on 6379 inside the compose network.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Interactive dashboard (HTML + vanilla JS) |
| GET | `/static/*` | Frontend JS/CSS assets |
| GET | `/health` | Liveness probe: `{status, db, redis, redis_url}` |
| POST | `/api/logs` | Ingest a single log entry or a JSON list |
| POST | `/api/logs/generate?count=N&seed=S` | Seed `N` synthetic logs (optional deterministic seed) |
| POST | `/api/search` | Faceted search — `SearchRequest` body, returns rows + facets |
| GET | `/api/facets` | Facet counts only (no rows). Filters as comma-separated query params. |
| GET | `/api/stats` | Total logs, per-dim cardinality, cache counters, redis reachable |

### Example: POST /api/search

Request body:

```json
{
  "query": "timeout",
  "filters": {
    "service": ["payments"],
    "level": ["ERROR", "WARN"],
    "region": ["us-east-1"]
  },
  "limit": 10
}
```

Response (shape, trimmed):

```json
{
  "logs": [
    {
      "id": "b6e4a8...",
      "ts": 1744900812,
      "service": "payments",
      "level": "ERROR",
      "region": "us-east-1",
      "response_time_ms": 1834.2,
      "source_ip": "10.0.3.14",
      "request_id": "req-9f2c",
      "message": "payments upstream timeout after 2000ms",
      "metadata": {"trace_id": "..."},
      "latency_bucket": "500ms-2s",
      "hour_bucket": 14
    }
  ],
  "total_count": null,
  "has_more": true,
  "next_cursor": 1744900801,
  "facets": [
    {
      "name": "service",
      "display_name": "Service",
      "values": [
        {"value": "payments", "count": 142, "selected": true},
        {"value": "auth", "count": 89, "selected": false},
        {"value": "api-gateway", "count": 76, "selected": false}
      ],
      "has_more_values": false
    },
    {"name": "level", "display_name": "Level", "values": [/* ... */]},
    {"name": "region", "display_name": "Region", "values": [/* ... */]},
    {"name": "latency_bucket", "display_name": "Response time", "values": [/* ... */]},
    {"name": "hour_bucket", "display_name": "Hour of day", "values": [/* ... */]}
  ],
  "query_time_ms": 8.412,
  "applied_filters": {
    "service": ["payments"],
    "level": ["ERROR", "WARN"],
    "region": ["us-east-1"]
  },
  "cached": false
}
```

Notes:
- `total_count` is `null` by default — we fetch `limit + 1` rows and set `has_more` instead of running a separate `COUNT(*)` over a filtered set.
- `next_cursor` is the `ts` of the last row; pass it back as `cursor` for keyset pagination.
- Under the excluded-self rule, the `service` facet still includes `auth`, `api-gateway`, etc. with their real counts even when `service=["payments"]` is selected.

### Example: GET /api/stats

```json
{
  "total_logs": 52000,
  "facet_cardinality": {
    "service": 5,
    "level": 5,
    "region": 4,
    "latency_bucket": 4,
    "hour_bucket": 24
  },
  "cache": {
    "hits": 1847,
    "misses": 612,
    "errors": 0,
    "hit_rate": 0.7511
  },
  "redis_reachable": true
}
```

---

## Sample Output

Trimmed output from `make demo` showing the excluded-self behaviour after a single-facet selection:

```
============================================================
=== Stage 4 / Q2: service=payments (excluded-self) ===
============================================================
  returned_logs          5
  query_time_ms          11.842
  applied_filters        {'service': ['payments']}
  service facet (excluded-self should show all 5 services):
    - payments     count=701 <selected>
    - auth         count=494
    - api-gateway  count=401
    - cache        count=205
    - orders       count=199
  level facet (all levels, scoped to service=payments):
    - INFO   count=491
    - WARN   count=108
    - ERROR  count=69
    - DEBUG  count=28
    - FATAL  count=5
```

Note that the `service` facet still reports counts for `auth`, `api-gateway`, `cache`, and `orders` even though we're filtering *to* `service=payments` — clicking any of them will broaden the selection rather than blanking the list.

---

## Performance

Numbers from `scripts/load_test.py` and the demo harness, all measured inside Docker on the full stack:

| Metric | Value |
|---|---|
| Cached search (p50 / p95) | 7 ms / <30 ms |
| Uncached facet UNION ALL on 50k rows | ~50-60 ms |
| Sustained throughput | 273 qps |
| Cache hit rate (typical drill-down) | 60-80% |
| Tests passing in Docker | 74 / 74 |

**Honest caveat.** Under an adversarial 100-way concurrency load with maximally diverse queries (every request has different random filters, so the Redis cache is cold every time), p95 climbs to ~1100 ms. In that regime cold misses queue behind the 16-connection SQLite read pool and each miss runs the full UNION ALL. Real-world drill-down patterns repeat filter combinations, so the cache-aside layer absorbs most of the load and p95 stays well under 100 ms. A materialized facet-aggregate table (rolling counts per filter signature) would close that gap; we left it out of scope.

---

## What I Learned

- **Excluded-self UNION ALL** is the cleanest way to compute faceted counts in one SQL round-trip: for each dimension, emit a `SELECT '<dim>', <dim>, COUNT(*) ... WHERE <full-filter-EXCEPT-self> GROUP BY <dim>` and UNION ALL the five subqueries. That way clicking `level=ERROR` doesn't collapse the other level counts to zero.
- **STORED generated columns** (for `latency_bucket` and `hour_bucket`) beat `VIRTUAL` ones when you want to index them — SQLite can only index STORED values, so you pay the compute cost once on write in exchange for a plain B-tree lookup on read.
- **Keyset pagination** (`WHERE ts < :cursor ORDER BY ts DESC LIMIT N+1`) kicks OFFSET hard. OFFSET forces SQLite to scan-and-skip; keyset lets it seek. And fetching `N+1` lets you set `has_more` without a separate `COUNT(*)` over a filtered set, which was the single biggest latency win we found.
- **SQLite pragmas are low-hanging fruit**: WAL journaling, `synchronous=NORMAL`, `cache_size=-64000` (64 MB page cache), `mmap_size=256MB`, `temp_store=MEMORY` stacked to a measurable win with no code changes.
- **Composite index order matters**: equality columns first, range columns last. `idx_ts_service_level (ts,service,level)` read wrong — we wanted `(service, level, ts)` for the dominant filter shape, and `(region, ts)` separately.
- **aiosqlite serializes on a single connection** — every `await conn.execute(...)` queues behind one background thread. Under 100-way concurrency this turned the backend into a single-threaded queue. Fix: an `AsyncSqlitePool` with 8-16 read connections plus a dedicated write connection.
- **Cache-aside with `filter_hash` + short TTL (30s)** beats eager invalidation for append-only logs. We key on `sha1(json.dumps(filters, sort_keys=True))` and let stale entries expire rather than chasing invalidations on every ingest.
- **Vanilla JS UX details pay off**: `AbortController` cancels in-flight fetches when the user clicks a new facet before the previous response lands; and keeping selected-but-zero facet entries visible-but-disabled (instead of hiding them) means the user never loses the "click to deselect" affordance.

---

## Project Structure

```
faceted-log-search-engine/
├── README.md
├── plan.md
├── project_requirements.md
├── requirements.txt
├── pytest.ini
├── .env.example
├── .gitignore
├── Dockerfile                   # app image
├── Dockerfile.test              # test runner image
├── docker-compose.yml           # app + redis + test service
├── Makefile                     # build/up/down/test/e2e/demo/load/ui/logs/clean
├── start.sh                     # compose up -d + /health poll
├── stop.sh                      # compose down -v
├── src/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app + lifespan + /health
│   ├── config.py                # pydantic-settings BaseSettings
│   ├── models.py                # LogEntry, SearchRequest, SearchResponse, ...
│   ├── api/
│   │   ├── __init__.py
│   │   ├── logs.py              # POST /api/logs, POST /api/logs/generate
│   │   ├── search.py            # POST /api/search, GET /api/facets
│   │   ├── stats.py             # GET /api/stats
│   │   └── ui.py                # GET / (dashboard)
│   ├── search/
│   │   ├── __init__.py
│   │   ├── query_builder.py     # FACET_DIMS + excluded-self WHERE + UNION ALL SQL
│   │   ├── facet_counter.py     # run SQL, shape SearchResponse, timing
│   │   └── generator.py         # synthetic log generator (weighted distributions)
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── sqlite_store.py      # AsyncSqlitePool, pragmas, migrations, indexes
│   │   └── redis_cache.py       # cache-aside + graceful fallback + counters
│   ├── templates/
│   │   └── index.html           # dashboard (Jinja-served)
│   └── static/
│       ├── app.js               # state + debounced fetch + AbortController + <mark>
│       └── app.css              # minimal grid + facet/mark styles
├── scripts/
│   ├── __init__.py
│   ├── demo.py                  # end-to-end walkthrough over the running stack
│   └── load_test.py             # async httpx p95/qps harness
└── tests/
    ├── __init__.py
    ├── conftest.py              # async client, seeded DB, isolated tmp DB fixtures
    ├── test_storage.py          # WAL + pragmas + schema + generated columns + indexes
    ├── test_generator.py        # distribution checks over 1000+ generated rows
    ├── test_api_logs.py         # ingest + generate endpoints
    ├── test_search.py           # query_builder + facet_counter unit tests
    ├── test_api_search.py       # /api/search HTTP-level behaviour
    ├── test_cache.py            # Redis cache-aside + graceful fallback
    ├── test_api_stats.py        # /api/stats shape + numbers
    ├── test_health.py           # /health probe variants
    ├── test_ui_routes.py        # GET / returns HTML, /static/* returns assets
    └── test_e2e.py              # full-stack integration (generate -> search -> verify)
```

---

## Development Notes

- **All tests run inside Docker.** The test image is separate (`Dockerfile.test`) and joins the compose network via the `test` profile. Host-side `pytest` is not supported and is not part of the workflow.
- **Five facet dimensions** are registered in `src/search/query_builder.py` as `FACET_DIMS = ("service", "level", "region", "latency_bucket", "hour_bucket")`. This tuple is the single source of truth for the facet response order and the UNION ALL fan-out.
- **Adding a new facet dimension** requires four coordinated changes:
  1. Add the column (and an index) to the `logs` table migration in `src/storage/sqlite_store.py`.
  2. Add the dimension name to `FACET_DIMS` in `src/search/query_builder.py`.
  3. Add its human label to `FACET_DISPLAY_NAMES` in `src/models.py`.
  4. Add it to the frontend's initial `state.filters` object in `src/static/app.js` so the dashboard starts with an empty selection for the new dimension.
- **Environment variables** are defined in `.env.example` and read via `src/config.py` (pydantic-settings). The ones worth knowing: `REDIS_URL`, `DB_PATH`, `FACET_CACHE_TTL` (default 30 s), `FACET_VALUES_TTL` (default 300 s), `MAX_FACET_VALUES` (default 8; frontend top-N cap).

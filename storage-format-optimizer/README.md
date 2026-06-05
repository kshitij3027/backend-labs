# Storage Format Optimizer

An **adaptive storage engine** for logs that ingests entries, **learns from query
access patterns**, and **automatically migrates partitions** between row, columnar, and
hybrid formats — minimizing both query latency and on-disk size for whatever shape of
traffic it actually sees.

**Status: complete.** Three pluggable storage backends (row / columnar / hybrid) sit behind
an atomic per-tenant manifest, fed by a real-time pattern tracker, a rule-based format
selector, and a background **copy-on-write** migration engine that rewrites live partitions
without disrupting reads. Adaptive per-column compression learning, self-pruning min/max
indexing, hot/warm/cold tiering, and per-tenant optimization views are wired behind a FastAPI
service with a live Chart.js/WebSocket dashboard, and verified in Docker (unit + integration +
cross-container E2E + a load test). Measured ~9,800 ingested entries/s and query-service
p90 ≈ 7 ms.

---

## The Problem

No single physical layout is best for all access patterns. Logs are written once and then read
in wildly different ways: recent data is appended and read back whole; older data is scanned
analytically over a handful of columns. A row store is great for the former and terrible for
the latter; a columnar store is the reverse. Picking one layout up front means losing on half
your traffic.

This project stores each partition in the layout that matches **how it is actually being read**,
and **changes that layout over time** as the access pattern shifts — recent, write-heavy,
point-looked-up data trends toward row; aging, scan-heavy data trends toward columnar; mixed
data lands on hybrid.

---

## The Core Idea — Row vs. Columnar vs. Hybrid

- **Row format** (`ROW`) — best for **write-heavy** workloads, **point lookups**, and
  **full-record reads**. Stored as **LZ4-framed JSONL**: records are contiguous, so appending
  or fetching a whole entry is cheap. The natural home for **hot / recent** data.
- **Columnar format** (`COLUMNAR`) — best for **analytical scans** and **aggregations over a few
  columns**, and it **compresses far better** because each column holds homogeneous values.
  Stored as **Parquet** (via PyArrow) with per-column codecs. Suits **cold / scan-heavy** data.
- **Hybrid format** (`HYBRID`) — a deliberate mix: **recent rows** are kept in a row buffer for
  cheap appends/lookups while **sealed older rows** are written to Parquet. Fits **warm** data
  that still takes occasional point lookups but is increasingly scanned analytically.

The **format selector** observes the live query stream — how often each partition is touched,
how recently, and the *shape* of queries (point lookup vs. wide scan, which columns) — and the
**migration engine** rewrites partitions toward the format that minimizes **query latency +
storage size** for that observed pattern.

---

## Architecture

```
                         ┌─────────────────────┐
   client ─────────────► │   FastAPI server    │  /api/ingest, /api/query, /api/stats
                         └──────────┬──────────┘  /ws (live dashboard)   ◄──── dashboard (WS)
                                    │
        ┌───────────────┬──────────┼───────────────┬───────────────────┐
        ▼               ▼          ▼               ▼                   ▼
 ┌────────────┐  ┌────────────┐  ┌─────────┐  ┌────────────┐   ┌────────────────┐
 │ Ingest     │  │ Query      │  │ Pattern │  │ Manifest   │   │ Background     │
 │ engine     │  │ engine     │  │ tracker │  │ (per-tenant│   │ migration      │
 │ (flatten + │  │ (classify+ │  │ (freq · │  │  atomic    │   │ engine         │
 │  partition)│  │  read +agg)│  │ recency·│  │  source of │   │ (COW rewrite + │
 └─────┬──────┘  └─────┬──────┘  │  shape) │  │  truth)    │   │  atomic swap)  │
       │               │         └────┬────┘  └─────┬──────┘   └───────┬────────┘
       └───────┬───────┴──────────────┴─────────────┘                  │
               ▼                                                        │
   ┌─────────────────────────────────────┐   ┌──────────────┐   ┌──────────────┐
   │ Pluggable storage backends          │   │ Format       │   │ Tier manager │
   │  ROW = LZ4 JSONL                     │◄──┤ selector     │◄──┤ hot/warm/cold│
   │  COLUMNAR = Parquet (per-col codecs) │   │ (rule-based) │   └──────────────┘
   │  HYBRID = recent rows + sealed Parquet│   └──────────────┘
   └──────────────────┬──────────────────┘   ┌──────────────┐   ┌──────────────┐
                      │                       │ Compression  │   │ Index        │
                      │                       │ chooser      │   │ manager      │
                      │                       │ (learned     │   │ (min/max,    │
                      │                       │  codecs, B)  │   │  self-prune,C)│
                      ▼                       └──────────────┘   └──────────────┘
                  metrics ──────────────► live dashboard via WebSocket + vendored Chart.js
```

A request lands through the **ingest engine**, which flattens entries, buckets them into
time-based partitions, writes each partition through its **current format's backend**, and
records the write into the **manifest** (the per-tenant source of truth), the **pattern tracker**,
and **metrics**. The **query engine** classifies each query, skips non-matching partitions via
the min/max index, reads each surviving partition through its own backend, and either unions
the rows or computes the requested aggregations. In the background, the **migration engine**
re-evaluates every partition on a timer and, when the selector recommends a different layout,
rewrites it **copy-on-write** and atomically swaps the manifest pointer — so reads in flight
never see a half-written partition.

### Components

- **Pluggable storage backends** (`src/storage/`) — a common `StorageBackend` interface
  (`base.py`) with three implementations: `RowBackend` (LZ4-framed JSONL), `ColumnarBackend`
  (Parquet via PyArrow, with per-column codecs and column projection + predicate pushdown via
  `pyarrow.dataset`), and `HybridBackend` (a recent-rows buffer plus sealed Parquet).
- **Atomic per-tenant manifest** (`src/manifest.py`) — the durable **source of truth** for which
  partitions exist, their format/tier/paths/counters/codecs/index. Writes go through a
  temp-file + atomic-rename so a crash never leaves a torn manifest.
- **Real-time pattern tracker** (`src/pattern_tracker.py`) — records per-partition access
  (frequency, recency, point-lookup vs. scan, which columns) to score how each partition is used.
- **Query classifier** (`src/classifier.py`) — labels each query `analytical`, `full_record`,
  or `mixed` from its projection width.
- **Rule-based format selector** (`src/format_selector.py`) — recommends a format (with a
  human-readable reason + confidence) from access stats, age, row count, and tier, and gates
  whether a migration is worth doing.
- **Background migration engine** (`src/migration_engine.py`) — the one long-lived background
  task. Re-evaluates partitions on a timer, rewrites the ones whose ideal format changed
  **copy-on-write**, swaps the manifest atomically (no query disruption), and reclaims orphaned
  files. Engineered to be unkillable by data errors.
- **Adaptive compression learning** (`src/compression.py`, **Feature B**) — chooses per-column
  Parquet codecs, learning the best size/latency trade-off from a sample of real data rather
  than using a fixed codec.
- **Intelligent self-pruning indexing** (`src/index_manager.py`, **Feature C**) — builds min/max
  partition indexes for frequently-filtered, selective columns, enabling partition skipping, and
  **drops** indexes that stop earning their keep.
- **Hot/warm/cold tiering** (`src/tier_manager.py`, **Feature D**) — classifies partitions by
  recency + read rate, steering both codec choice (cold → stronger codec) and format selection.
- **Per-tenant optimization** (`src/api/routes_stats.py`, **Feature A**) — `GET /api/stats/{tenant}`
  explains *every* partition: its current layout next to what the selector recommends and why.
- **Metrics + WebSocket dashboard** (`src/metrics.py`, `src/websocket.py`, `dashboard/`) — a
  bounded metrics aggregator feeds a WebSocket that pushes live ticks to a vendored-Chart.js page.

---

## Tech Stack

- **Language:** Python 3.12
- **API framework:** FastAPI + Uvicorn (long-lived ASGI service)
- **Columnar storage:** Apache Parquet via **PyArrow** (`pyarrow.dataset` for projection +
  predicate pushdown)
- **Row storage:** append-only **JSONL** framed with **LZ4** (stdlib + `lz4`)
- **Config / validation:** **Pydantic v2** + **pydantic-settings**
- **Dashboard:** vanilla HTML + **vendored Chart.js** (served as a static asset — no Python
  charting dependency) + a `/ws` WebSocket
- **Testing:** **pytest** + **httpx** (FastAPI `TestClient` and an E2E client)
- **Deployment:** Docker / Docker Compose

---

## How to Run

Everything runs through the `Makefile` (which wraps `docker compose`). The data and log
directories are created on startup; there is no external dependency to seed.

```bash
# from this project folder
make build        # build the app + tester images
make up           # start the app (detached) at http://localhost:8000
```

If host port 8000 is already taken, pick another published port:

```bash
API_PORT=8011 make up        # then use http://localhost:8011
```

Open the live dashboard and probe health:

```bash
open http://localhost:8000/          # live Chart.js / WebSocket dashboard
curl http://localhost:8000/health    # -> {"status":"healthy"}
```

Populate the dashboard with a realistic spread of demo data (a few hundred rows across 2–3
tenants, plus a mix of full-record and analytical queries, so the format distribution drifts as
the migration loop runs):

```bash
python scripts/seed_demo.py          # seeds a RUNNING server (defaults to localhost:8000)
```

### Sample requests

```bash
# Ingest a batch for tenant "acme"
curl -s -X POST http://localhost:8000/api/ingest \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","entries":[
        {"ts":1700000000,"fields":{"region":"us","status":200,"latency_ms":12}},
        {"ts":1700000001,"fields":{"region":"eu","status":500,"latency_ms":48}}
      ]}'
# -> {"ingested":2,"partitions_touched":["p_472222"],"tenant":"acme"}

# Full-record query (no projection -> all columns)
curl -s -X POST http://localhost:8000/api/query \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","filters":[{"column":"status","op":"eq","value":500}]}'
# -> {"rows":[{...}],"aggregates":null,"meta":{"query_class":"full_record",...}}

# Projection query (analytical -> a few columns, exercises columnar column-skipping)
curl -s -X POST http://localhost:8000/api/query \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","columns":["region","latency_ms"]}'
# -> {"rows":[{"region":"us","latency_ms":12},...],"aggregates":null,"meta":{...}}

# Aggregation query (avg latency grouped by region)
curl -s -X POST http://localhost:8000/api/query \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","aggregations":[{"op":"avg","column":"latency_ms"}],"group_by":["region"]}'
# -> {"rows":null,"aggregates":{...},"meta":{"query_class":"analytical",...}}

# System-wide stats (format distribution, migrations, per-format latency, storage)
curl -s http://localhost:8000/api/stats

# One tenant's per-partition decisions (current format vs. recommendation + reason)
curl -s http://localhost:8000/api/stats/acme
```

Verification targets (all run **inside Docker** — never on the host):

```bash
make test         # full suite: unit + integration + e2e
make test-unit    # only tests/unit
make test-int     # only tests/integration
make test-e2e     # only tests/e2e
make e2e          # cross-container E2E verifier against the live stack
make load         # containerized load test (ingest throughput + query p90 gates)
make logs         # tail app logs
make down         # stop and remove the stack
make clean        # down + remove volumes
```

---

## API

Base URL: `http://localhost:8000`.

### REST

| Method | Path | Request body | Response |
|--------|------|--------------|----------|
| `GET`  | `/` | — | Live monitoring dashboard (HTML + vendored Chart.js). |
| `GET`  | `/health` | — | `{"status":"healthy"}` (liveness probe). |
| `POST` | `/api/ingest` | `{"tenant":"<t>","entries":[{"ts":<epoch?>,"fields":{...}}, ...]}` (≥1 entry) | `{"ingested":<int>,"partitions_touched":[<pid>...],"tenant":"<t>"}`. Empty/malformed body → `422`. |
| `POST` | `/api/query` | `{"tenant":"<t>","columns":[...]?,"filters":[{"column":..,"op":"eq\|ne\|gt\|gte\|lt\|lte\|in","value":..}],"aggregations":[{"op":"count\|sum\|avg\|min\|max","column":..?}],"group_by":[...],"limit":<int>?}` | `{"rows":[...] \| null,"aggregates":{...} \| null,"meta":{"query_class":"analytical\|full_record\|mixed","partitions_read":..,"partitions_skipped":..,"formats_used":{<format>:n},"rows_scanned":..,"rowgroups_skipped":..,"elapsed_ms":..}}`. Exactly one of `rows`/`aggregates` is set. Malformed body → `422`. |
| `GET`  | `/api/stats` | — | `{"storage":{total_bytes,uncompressed_estimate_bytes,compression_ratio,by_format}, "formats":{distribution,partitions_total}, "performance":{per-format p50/p90/throughput/count + analytical speedup}, "migrations":{completed,failed,in_flight,recent}, "ingest":{entries_per_sec,total_entries}, "tenants":[...], "selection_optimality":<0..1>}`. |
| `GET`  | `/api/stats/{tenant}` | — | `{"tenant":"<t>","format_distribution":{<format>:n},"tier_distribution":{<tier>:n},"partitions":[{partition_id,format,tier,row_count,size_bytes,recommended_format,reason,confidence,indexed_columns}, ...],"index_columns_total":..,"storage_bytes":..,"compression_ratio":..}`. Unknown tenant → zeroed `200`, never `404`. |
| `GET`  | `/api/partitions?tenant=<t>` | — | Raw per-partition manifest records for one tenant (full on-disk record: format, tier, paths, counters, codecs, index, access, last_migration). `tenant` defaults to `default`; unknown tenant → empty list. |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws` | Sends an immediate snapshot on connect, then a `{"type":"tick","stats":..,"series":..,"tenants":{<tenant>:{<format>:n}},"migrations":[...],"indexes":{"columns_indexed":n},"tiers":{<tier>:n}}` payload every `WS_PUSH_INTERVAL_SECONDS`. Drives the dashboard. |

---

## Configuration (env vars)

All fields are overridable via env var (case-insensitive) or a `.env` file. `.env.example` lists
**every** setting with its default; the source of truth is `src/settings.py`.

| Variable | Default | Purpose |
|----------|---------|---------|
| **storage paths** | | |
| `DATA_DIR` | `./data` | Root dir for per-tenant partition data. |
| `LOG_DIR` | `./logs` | Root dir for application logs. |
| **partitioning** | | |
| `PARTITION_BUCKET_SECONDS` | `3600` | Time-bucket width per partition. |
| `HYBRID_SEAL_AGE_SECONDS` | `1800` | Age after which HYBRID recent rows seal to Parquet. |
| **query classification** | | |
| `ANALYTICAL_MAX_COLUMNS` | `3` | ≤ this many projected columns → `analytical`. |
| `FULL_RECORD_MIN_COLUMNS` | `10` | ≥ this many projected columns → `full_record`. |
| **format selector** | | |
| `SELECT_WRITE_RATIO_ROW` | `0.3` | Write fraction above which ROW wins. |
| `SELECT_POINT_LOOKUP_ROW` | `0.5` | Point-lookup fraction favouring ROW. |
| `SELECT_SCAN_RATIO_COLUMNAR` | `0.6` | Scan fraction favouring COLUMNAR. |
| `SELECT_FEW_COLUMNS_FRACTION` | `0.4` | Column-touch fraction favouring COLUMNAR. |
| `SELECT_MIN_CONFIDENCE` | `0.6` | Below this confidence → keep current format. |
| `SELECT_MIN_ROWS` | `256` | Below this row count → keep current format. |
| **tiers** | | |
| `TIER_HOT_MAX_AGE_SECONDS` | `3600` | Max age to still qualify as hot. |
| `TIER_COLD_MIN_AGE_SECONDS` | `86400` | Min age to qualify as cold. |
| `TIER_HOT_MIN_READS_PER_MIN` | `1.0` | Read rate to stay hot. |
| **migration engine** | | |
| `MIGRATION_INTERVAL_SECONDS` | `5.0` | Background loop tick interval. |
| `MIGRATION_MAX_PER_TICK` | `4` | Max partitions migrated per tick. |
| `MIGRATION_COOLDOWN_SECONDS` | `60.0` | Per-partition re-migration cooldown. |
| **compression (Feature B)** | | |
| `ROW_CODEC` | `lz4` | Codec for ROW JSONL frames. |
| `COLUMNAR_DEFAULT_CODEC` | `SNAPPY` | Default Parquet codec. |
| `COLUMNAR_COLD_CODEC` | `ZSTD` | Parquet codec for cold partitions. |
| `COMPRESSION_LEARN_ENABLED` | `true` | Enable learned per-column codec selection. |
| `COMPRESSION_LEARN_SAMPLE_ROWS` | `2000` | Rows sampled when learning a codec. |
| `COMPRESSION_LEARN_SIZE_WEIGHT` | `1.0` | Weight on compressed size in the learner. |
| `COMPRESSION_LEARN_LATENCY_WEIGHT` | `0.2` | Weight on (de)compress latency in the learner. |
| **indexing (Feature C)** | | |
| `INDEX_MIN_FILTER_HITS` | `5` | Filter hits before building an index. |
| `INDEX_MIN_SELECTIVITY` | `0.2` | Min selectivity to justify an index. |
| `INDEX_DROP_BENEFIT_WINDOW` | `200` | Window of queries for the benefit calc. |
| `INDEX_DROP_MIN_BENEFIT` | `0.01` | Min benefit before an index is dropped. |
| **metrics / dashboard** | | |
| `METRICS_HISTORY_POINTS` | `60` | Retained time-series points. |
| `WS_PUSH_INTERVAL_SECONDS` | `2.0` | WebSocket broadcast interval. |
| **API / server** | | |
| `API_HOST` | `0.0.0.0` | HTTP bind host. |
| `API_PORT` | `8000` | HTTP listen port. |
| `LOG_LEVEL` | `INFO` | Log level. |

---

## Results / success criteria

Verified in Docker (`make test`, `make e2e`, `make load`):

| Success criterion | Result |
|-------------------|--------|
| Ingest throughput | Load test sustains **> 1,000 entries/s** (measured **~9,800/s**). |
| Query latency | Query-service **p90 < 100 ms** (measured **~7 ms**) on the load profile. |
| Migration without disruption | Partitions are rewritten **copy-on-write** with an atomic manifest swap; the E2E verifier confirms **rows are preserved** across a format change while the server keeps serving. |
| Format-selection optimality | `selection_optimality` reports the fraction of partitions already in the format the selector would choose; the seed/E2E flow drives cold scan-heavy partitions to COLUMNAR and recent ones to ROW/HYBRID. |
| Storage compression | Parquet per-column codecs (SNAPPY default, ZSTD for cold) shrink columnar partitions; `/api/stats` reports the `compression_ratio` from the manifest. |
| Live observability | Dashboard updates live over the `/ws` WebSocket (format distribution, migrations, per-tier counts, per-format latency, indexed columns). |

**Honest caveats:**

- The **analytical 3× speedup** (columnar vs. row for narrow scans) is **data-dependent** — it is
  *reported* by the `performance.analytical_speedup` metric, **not gated** by the load test, and
  varies with the column mix and selectivity of the workload.
- Under heavy **16-way concurrency**, query latency **degrades**: the service uses **synchronous
  file I/O in a single Uvicorn worker**, a deliberate simplicity trade-off for this learning
  project (no async object store, no worker pool). Throughput and single/low-concurrency latency
  are strong; tail latency under high fan-out is the cost of that simplicity.

---

## Project Structure

```
storage-format-optimizer/
├── README.md
├── requirements.txt
├── .env.example                # every Settings field with its default
├── Dockerfile / Dockerfile.test
├── docker-compose.yml
├── Makefile                    # build/up/down/logs/test*/e2e/load/clean
├── pytest.ini
├── src/
│   ├── main.py                 # FastAPI app: lifespan wiring, /, /health, /ws
│   ├── settings.py             # Pydantic-settings config (source of truth)
│   ├── models.py               # domain enums + request models
│   ├── paths.py                # data-dir path helpers + partition identity
│   ├── manifest.py             # atomic per-tenant manifest (source of truth)
│   ├── pattern_tracker.py      # real-time access-pattern tracking
│   ├── classifier.py           # analytical / full_record / mixed
│   ├── format_selector.py      # rule-based format recommendation + migration gate
│   ├── tier_manager.py         # hot / warm / cold (Feature D)
│   ├── compression.py          # learned per-column codec chooser (Feature B)
│   ├── index_manager.py        # self-pruning min/max indexing (Feature C)
│   ├── ingest_engine.py        # flatten + partition + write
│   ├── query_engine.py         # classify + read + aggregate
│   ├── migration_engine.py     # background COW migration + atomic swap
│   ├── metrics.py              # bounded metrics aggregator
│   ├── websocket.py            # dashboard connection manager
│   ├── storage/
│   │   ├── base.py             # StorageBackend interface
│   │   ├── row_backend.py      # ROW = LZ4 JSONL
│   │   ├── columnar_backend.py # COLUMNAR = Parquet (per-col codecs, pushdown)
│   │   └── hybrid_backend.py   # HYBRID = recent rows + sealed Parquet
│   └── api/
│       ├── dependencies.py     # app.state accessors
│       ├── schemas.py          # response models
│       ├── routes_ingest.py    # POST /api/ingest
│       ├── routes_query.py     # POST /api/query
│       └── routes_stats.py     # GET /api/stats, /api/stats/{tenant}, /api/partitions
├── dashboard/
│   ├── templates/index.html
│   └── static/                 # vendored chart.min.js + dashboard.css/js
├── scripts/
│   ├── seed_demo.py            # populate a running server for the dashboard
│   ├── load_test.py            # throughput + query p90 gates
│   └── verify_e2e.py           # cross-container E2E verifier
└── tests/
    ├── unit/                   # backends, selector, classifier, manifest, ...
    ├── integration/            # API, engines, migration, indexing, dashboard, WS
    └── e2e/                    # full storage-flow verification
```

---

## What I Learned

- **Row vs. columnar vs. hybrid is a real, measurable trade-off**: contiguous rows win for
  appends and whole-record reads; columns win for narrow analytical scans and compression.
  Hybrid (recent rows + sealed Parquet) is a pragmatic middle for warm data.
- **Parquet earns its keep through column projection + predicate pushdown** via
  `pyarrow.dataset` — only the requested columns and matching row groups are decoded, which is
  where the analytical speedup comes from.
- **Copy-on-write migration with an atomic manifest swap** gives **zero-disruption** reads: write
  the new layout beside the old, then flip a single manifest pointer with an atomic rename, so a
  query in flight never sees a half-written partition.
- **An explicit manifest as the source of truth** (separate from observational metrics) keeps the
  system honest: format distribution, storage totals, and optimality are all derived from what is
  actually on disk.
- **Per-column codec selection can be *learned*** from a data sample (size vs. (de)compress
  latency), beating a single fixed codec for heterogeneous columns.
- **Index-driven partition skipping should self-prune**: build a min/max index only for
  frequently-filtered, selective columns, and drop it once it stops earning benefit — an index
  that never skips anything is pure overhead.
- **Frecency-based tiering** (recency + read rate) is a cheap, explainable signal for steering
  both codec strength (cold → stronger) and format choice.
- **Serving a live WebSocket dashboard** from a bounded metrics aggregator keeps observability
  cheap: append one time-series point per tick and broadcast a compact JSON snapshot to every
  connected client.

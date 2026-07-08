# Correlation Analysis System

A real-time engine that ingests logs from **five simulated e-commerce sources** and automatically detects **five kinds of correlation** between events — temporal, session, user, error-cascade, and metric — filters the statistical ones through **Benjamini-Hochberg false-discovery-rate control**, learns recurring patterns, raises operator alerts, and streams everything to a live **React + Recharts dashboard**. It runs as three long-lived Docker services (FastAPI backend, nginx-served SPA, Redis) and is verified end-to-end by a black-box harness and a hard-gated load test.

---

## What It Does

The system stands in for a production observability stack where logs from many services need to be *related to each other*, not just collected. A deterministic generator simulates a checkout platform — web (nginx), database (postgresql), API/microservice, payment service, and inventory service — emitting coherent multi-hop **checkout journeys** (all hops sharing one `correlation_id` + `user_id`), background noise with no ids, and rotating **incident scenarios** where symptoms genuinely co-move (a saturating DB pool really does push web 5xx up). Each raw line is parsed back into a standardized `LogEvent` (timestamp, source, service, level, message, correlation/user id, error code, numeric metrics).

A single-process pipeline then folds that stream into per-second metric rings and a sliding event window, and every 2 seconds runs **five detectors**: temporal proximity across streams, session (one finished journey), user (one person across journeys), error cascade (ordered failure chains across services), and metric (statistical relationships between numeric series). Metric findings pass through one BH-FDR significance pass per cycle so re-scanning ~19 series pairs every tick doesn't flood the dashboard with coincidences. Findings are assessed against **learned pattern baselines** (a recurrence confidence boost, 2σ anomaly flags, new-pattern flags), fed to an **alert** rule set, mirrored best-effort to **Redis**, and accumulated in memory.

Everything the operator sees is served from those in-memory accumulators — the REST API performs **zero Redis operations on any request path**, so the API and dashboard keep answering right through a Redis outage. The dashboard polls one fat `/api/v1/dashboard` payload every 5 seconds and fans it out to stat cards, a timeline chart, a strength×confidence scatter, a 5×5 source heatmap, an alerts feed, and two sortable/filterable tables.

---

## Architecture

One asyncio process runs the whole pipeline; the API reads its accumulators between ticks on the same event loop, so there is **no locking anywhere**. Redis is an *enhancement*, never a hard dependency — every store operation degrades to a no-op if Redis is unreachable.

```
  LogGenerator ──► parsers ──► LogCollector ──► MetricAggregator (per-second
  5 sources +      raw line     parse + buffer   numpy ring buffers, 120 slots)
  3 scenarios      → LogEvent    (deque, 5000)          │
       │                                                │ window events (30 s)
       │  1 s ingest tick                               ▼
       │                                       CorrelationEngine ── 2 s detection tick
       │                                       ├─ temporal   ┐
       ▼                                       ├─ session    │
  JourneyRecord                                ├─ user       ├─► PatternLearner (boost/
  (E2E ground truth)                           ├─ cascade    │   anomaly/new) ─► AlertManager
                                               └─ metric ────┘        │
                                          (BH-FDR significance pass)   │
                                                    │                  │
                    best-effort mirror ┌────────────┴──────────────────┴─────────┐
                    (never blocks) ───►│  Redis: events · correlations · stats ·  │
                                       │  patterns · alerts (list + pub/sub)      │
                                       └──────────────────────────────────────────┘
                                                    │
                        in-memory accumulators (recent deque, lifetime counters,
                        10-min timeline, 5×5 source EMA matrix, alert history)
                                                    │
                                    FastAPI REST API (:8000, in-memory reads only)
                                                    │  nginx /api reverse proxy
                                                    ▼
                          React + Recharts dashboard (:3000, 5 s poll of /dashboard)
```

**Single-loop design.** The pipeline task ticks every `generation_interval_seconds` (1 s): generate → parse → buffer → aggregate. Detection runs synchronously inside that loop every `detection_interval_seconds` (2 s) over the events parsed since the last pass plus the sliding window. Batch detection in one loop beats per-event coroutines at 100+ events/sec — the hot paths (`add_event`, `parse_line`) do only scalar writes into pre-allocated numpy buffers and bounded deques, no numpy allocation or pydantic revalidation per event.

**Services.**

| Service  | Port   | Role                                                                    |
|----------|--------|-------------------------------------------------------------------------|
| backend  | `8000` | uvicorn/FastAPI — hosts the pipeline (asyncio task) + the REST API       |
| frontend | `3000` | nginx serving the React SPA, reverse-proxying `/api` → `backend:8000`    |
| redis    | `6379` | ephemeral mirror of events/correlations/stats/patterns + alert pub/sub   |

---

## Correlation Types & Scoring

Each detector emits at most one finding per logical relationship per cycle (a TTL dedupe cache suppresses re-emission), with `strength` and `confidence` both clamped to `[0, 1]`. Formulas are taken verbatim from the detector modules.

| Type            | Links                                                    | Strength                                        | Confidence                                              |
|-----------------|----------------------------------------------------------|-------------------------------------------------|---------------------------------------------------------|
| `temporal`      | Two different source streams co-occurring in time         | `1 − dt/window` (proximity decay, window = 30 s) | `clamp(support/10)` into `[0.1, 0.9]` (per-cycle support)|
| `session_based` | The hops of one finished checkout journey (shared id)     | `coverage` = distinct sources / 5               | `0.7 + 0.3·coverage`                                    |
| `user_based`    | The multiple journeys of one user                         | `coverage` = distinct sources / 5               | `0.5 + 0.3·coverage` (lower floor: weaker evidence)     |
| `error_cascade` | An ordered chain of errors hopping across services        | `0.5·(1 − dt/window) + 0.5·min(1, sources/3)`   | `0.4` base `+0.3` shared corr_id `+0.2` shared user_id `+0.1` known root-cause direction |
| `metric_based`  | Two numeric per-second series                             | `|r|`, or Jaccard `J`, or normalized MI          | see below                                               |

**Metric confidence** depends on the method:
- Pearson / Spearman / lagged cross-correlation (all BH-tested): `(1 − p_adj) · min(1, n/30)`
- Jaccard error co-presence: `min(1, union/15)`
- Mutual information: `0.5 + 0.5·min(1, (n − 10)/50)`

**Statistical rigor (metric detector).** Per cycle it tests curated Pearson/Spearman pairs and the three target incident pairs by **lagged cross-correlation** (lead-lag: "the pool saturates, web errors follow seconds later"), plus **Jaccard** error-presence overlap and **normalized mutual information** (nonlinear dependence Pearson is blind to). Whichever of Pearson (linear) vs Spearman (monotone) fits better per pair is kept. Every Pearson/Spearman/TLCC p-value in the cycle goes through **one Benjamini-Hochberg pass** at `fdr_q` (0.05) — the multiple-testing false-positive filter — and survivors must also clear `|r| ≥ 0.4`.

**Freshness guard.** Cascade and user findings only emit while their newest underlying event is within `FRESHNESS_SECONDS` (4 s) of the cycle clock. This bounds per-row detection latency to ≈ 4 s + one tick, comfortably inside the 5 s real-time contract, and stops a finding from re-emitting against stale events lingering in the 30 s window after its dedupe TTL lapses.

**Pattern learning.** Before persistence each finding is assessed against its learned baseline `(type, endpoint_a, endpoint_b)`: a recurrence confidence **boost** `min(0.15, 0.03·ln(1 + prior_count))`, an **anomaly** flag when an established pattern (count ≥ 5) deviates > 2σ from its learned mean strength, and a **new-pattern** flag on the first sighting of an already-strong (≥ 0.8) relationship. Baselines are kept in memory, hydrated once from Redis on startup, and mirrored back fire-and-forget.

---

## Log Sources & Incident Scenarios

Five sources, each with its own wire format (parsed back to a common `LogEvent`):

| Source        | Service            | Format                                                        |
|---------------|--------------------|---------------------------------------------------------------|
| `web`         | `nginx`            | nginx combined + optional `corr=/user=/latency_ms=` trailer   |
| `database`    | `postgresql`       | `LOG/ERROR/FATAL` lines with a `/* corr=… user=… pool=a/b */` comment |
| `api_service` | `api-service`      | one JSON object per line                                      |
| `payment`     | `payment-service`  | logfmt `k=v` pairs                                            |
| `inventory`   | `inventory-service`| `[iso-ts] INVENTORY <op> k=v …` bracket format                |

The generator rotates through three **incident scenarios** (each active for 20 s out of a 45 s slot, on a deterministic clock), during which symptoms genuinely co-move so the target correlations are real rather than coincidental:

| Scenario              | What happens                                                             | Target correlation it makes detectable                     |
|-----------------------|--------------------------------------------------------------------------|------------------------------------------------------------|
| `db_pool_saturation`  | DB pool pegged at 20/20, query duration ×5, API latency ×3, some web 5xx  | `web.error_rate` ↔ `db.pool_utilization`                   |
| `payment_slowdown`    | Payment latency ×8, some timeouts, ~35% cart abandonment                 | `payment.latency_ms_avg` ↔ `user.abandonment_count`        |
| `inventory_timeouts`  | Inventory reserve timeouts + latency ×10, checkout failures (API 500 + web 500) | `inventory.timeout_count` ↔ `checkout.failure_count`  |

Alongside these three metric pairs, the DB scenario produces a genuine `database → web/api_service` **error cascade** — both are asserted by the E2E harness.

---

## How to Run

Everything runs in Docker — no local Python or Node needed, only Docker with Compose v2.

```bash
# Full stack incl. the dashboard (redis + backend + frontend), detached
make ui                 # Dashboard: http://localhost:3000 · API: http://localhost:8000

# equivalent helper scripts (build, wait for /health, print URLs)
./start.sh
./stop.sh

# backend + redis only (no dashboard)
make up                 # API: http://localhost:8000  (GET /health)

# or drive compose directly
docker-compose up       # redis + backend (frontend is started explicitly)
```

**Overriding ports.** The three host ports are compose-level and overridable via env vars on any target — e.g. if `8000 / 3000 / 6379` are taken:

```bash
BACKEND_PORT=8010 FRONTEND_PORT=3001 REDIS_PORT=6380 make ui
```

---

## Make Targets

| Target       | What it does                                                                    |
|--------------|---------------------------------------------------------------------------------|
| `build`      | Build all images (backend + test)                                               |
| `up`         | Run the backend detached (redis comes up as a healthy dependency)               |
| `down`       | Stop and remove the stack                                                        |
| `logs`       | Tail the backend logs                                                            |
| `ui`         | Run redis + backend + React dashboard detached, print the URLs                  |
| `test`       | Full pytest suite in Docker (unit + integration; rebuilds the tester image first)|
| `test-unit`  | Unit tests only, in Docker                                                       |
| `test-int`   | Integration tests only, in Docker (reach Redis by service name)                 |
| `e2e`        | Black-box E2E verifier vs the live stack — 14 ordered checks                    |
| `load`       | Perf/load gates vs the live stack (ingest, stats latency, concurrent RPS, memory)|
| `clean`      | `down` + remove volumes and orphans                                             |

`make e2e` and `make load` are **hard-gated** — the first failed check/breached gate exits non-zero and fails the target.

---

## REST API

Every endpoint serves from in-memory accumulators (no Redis on the request path) and degrades to an empty/zeroed shape rather than a 500 when a runtime piece is missing.

| Method | Path                                    | Purpose                                                            |
|--------|-----------------------------------------|--------------------------------------------------------------------|
| `GET`  | `/health`                               | Liveness — always `200` while alive; degradation reported in body  |
| `GET`  | `/api/v1/logs/recent?count=N`           | Newest parsed events, newest first (`count` clamped to `[1, 500]`) |
| `GET`  | `/api/v1/correlations?limit=N&min_strength=X` | Newest correlations (`limit` clamped `[1, 1000]`)            |
| `GET`  | `/api/v1/correlations/stats`            | The spec-verbatim 4-key stats contract                             |
| `GET`  | `/api/v1/correlations/types/{type}?limit=N` | Newest correlations of one type (bad type → `422`)             |
| `GET`  | `/api/v1/dashboard`                     | Everything the dashboard needs in one poll (9 sections)            |
| `GET`  | `/api/v1/debug/ground-truth?max_age=S`  | Generator journey ground truth — **E2E verification aid only**     |

**`GET /health`**

```json
{
  "status": "healthy",
  "service": "correlation-analysis",
  "version": "0.1.0",
  "uptime_seconds": 142.7,
  "memory_mb": 149.9,
  "components": {
    "redis": true,
    "pipeline_running": true,
    "events_processed": 17284,
    "events_per_sec": 118.6,
    "parse_errors": 0
  }
}
```

`status` and `service` are **spec-verbatim contract values** the tests assert exactly; richer operational data lives only under `components` and `/api/v1/dashboard`.

**`GET /api/v1/correlations/stats`** — exactly four keys, never more (the E2E verifier asserts the key set verbatim):

```json
{ "total": 4521, "types": { "temporal": 512, "session_based": 3110, "user_based": 402, "error_cascade": 168, "metric_based": 329 }, "avg_strength": 0.7134, "recent_count": 61 }
```

**`GET /api/v1/logs/recent?count=2`**

```json
{
  "events": [
    {
      "id": "9f2c…",
      "timestamp": 1752088812.512,
      "source": "database",
      "service": "postgresql",
      "level": "ERROR",
      "message": "connection pool exhausted",
      "correlation_id": null,
      "user_id": null,
      "error_code": "DB_POOL_EXHAUSTED",
      "metrics": { "pool_in_use": 20.0, "pool_size": 20.0 },
      "raw": "2026-07-09 10:00:12.512 PDT [8123] FATAL:  connection pool exhausted /* pool=20/20 */"
    }
  ]
}
```

**`GET /api/v1/correlations?limit=1`**

```json
{
  "count": 1,
  "correlations": [
    {
      "id": "a1b2c3d4e5f6",
      "detected_at": 1752088813.004,
      "correlation_type": "error_cascade",
      "event_a": { "id": "…", "source": "database", "service": "postgresql", "message": "connection pool exhausted", "timestamp": 1752088812.5, "correlation_id": null },
      "event_b": { "id": "…", "source": "web", "service": "nginx", "message": "POST /api/checkout/complete -> 503", "timestamp": 1752088812.9, "correlation_id": null },
      "strength": 0.83,
      "confidence": 0.5,
      "details": { "chain": [ … ], "chain_length": 4, "distinct_services": 2, "span_seconds": 0.4, "root_error": "DB_POOL_EXHAUSTED" }
    }
  ]
}
```

**`GET /api/v1/correlations/types/session_based?limit=50`** → `{ "correlation_type": "session_based", "count": 50, "correlations": [ … ] }`.

**`GET /api/v1/dashboard`** — one payload with exactly these nine sections:

| Section               | Contents                                                                 |
|-----------------------|--------------------------------------------------------------------------|
| `generated_at`        | Server epoch seconds when the payload was built                          |
| `status`              | `healthy`, `redis` (last observed, no probe), `pipeline_running`, `active_scenario` |
| `stats`               | The 4 spec stat keys **plus** `events_processed`, `events_per_sec`, `parse_errors`, `uptime_seconds`, `memory_mb`, `alerts_total` |
| `timeline`            | Up to 60 buckets of 10 s each: `t`, `count`, `avg_strength`, `by_type`    |
| `scatter`             | Newest ≤ 200 points: `strength`, `confidence`, `type`, `detected_at`     |
| `matrix`              | `sources` (canonical 5) + symmetric 5×5 `cells` of source-pair strength   |
| `recent_correlations` | Newest 20 full correlation objects                                       |
| `recent_logs`         | Newest 20 log events                                                      |
| `alerts`              | Newest 20 fired alerts                                                    |

---

## Dashboard

A React 18 + Vite SPA using **Recharts**, served by nginx and polling `GET /api/v1/dashboard` every **5 seconds** through nginx's `/api` reverse proxy (relative URLs only — no CORS, no hardcoded backend host). One polling hook (`useDashboard`) fans the payload out to:

- **Stat cards** — totals, avg strength, recent (60 s), events/sec, events processed, memory, alerts total, active scenario, Redis/pipeline status, and a per-type breakdown.
- **Timeline chart** — correlation activity over the last ~10 minutes.
- **Scatter plot** — strength × confidence, colored by type.
- **5×5 source heatmap** — hand-rolled CSS grid, symmetric with a zero diagonal.
- **Alerts feed** — newest fired alerts with severity.
- **Correlations table** — client-side sortable columns + correlation-type filter chips.
- **Logs table** — sortable + source/level filters, with ERROR rows tinted.

**Graceful degradation** is the whole point of the polling hook: on a failed poll it keeps the last good snapshot and raises `error`/`stale`, so a banner explains the outage while every panel keeps showing the last data instead of blanking out. Polling pauses while the browser tab is hidden and does an immediate catch-up fetch on return. nginx re-resolves the `backend` service name **per request** via Docker DNS, so the proxy stays correct if the backend container is recreated or scaled.

---

## Redis Key Schema

Redis mirrors state best-effort (a dead Redis logs one warning per outage and degrades to memory-only). All keys carry the `corr:` prefix.

| Key                                 | Type    | Contents                                                                 |
|-------------------------------------|---------|--------------------------------------------------------------------------|
| `corr:events:recent`                | LIST    | Newest-first `LogEvent` JSON, capped at 1000                             |
| `corr:correlations:recent`          | LIST    | Newest-first `Correlation` JSON, capped at 2000                          |
| `corr:correlations:by_type:{type}`  | LIST    | Newest-first per-type `Correlation` JSON, capped at 500 each             |
| `corr:stats`                        | HASH    | `total`, per-type `type:{t}` counts, `strength_sum`                      |
| `corr:stats:minute:{minute}`        | HASH    | Per-minute `total` + `type:{t}` counts, 1 h TTL (durability; API never reads it) |
| `corr:pattern:{type}:{a}:{b}`       | HASH    | `count`, `strength_sum`, `strength_sqsum`, `first_seen`, `last_seen`     |
| `corr:pattern:index`                | ZSET    | Member = pattern hash key, score = observation count (hot patterns first)|
| `corr:alerts:recent`                | LIST    | Newest-first `Alert` JSON, capped at 200                                 |
| `corr:alerts:channel`               | PUB/SUB | Every fresh `Alert` JSON, fanned out live to subscribers                 |

---

## Configuration

Backend settings (`src/config.py`) are read from field defaults → optional `.env` → environment variables. Each env var name is the **upper-cased field name** (e.g. `WINDOW_SECONDS` ← `window_seconds`).

| Setting                        | Default                       | Meaning                                                            |
|--------------------------------|-------------------------------|--------------------------------------------------------------------|
| `redis_url`                    | `redis://localhost:6379/0`    | Redis connection URL (compose overrides to `redis://redis:6379/0`) |
| `log_level`                    | `INFO`                        | Log level                                                          |
| `window_seconds`              | `30`                          | Sliding correlation window (detectors consider the last N seconds) |
| `generation_interval_seconds`  | `1.0`                         | Pipeline ingest tick: generate/parse/buffer/aggregate every N s    |
| `detection_interval_seconds`   | `2.0`                         | Detection tick: run the correlation detectors every N s            |
| `events_per_second`            | `135`                         | Target synthetic log volume (realized ≈ 0.9× → ~119 eps sustained) |
| `event_buffer_size`            | `5000`                        | Max parsed events kept in the in-memory deque                      |
| `pipeline_enabled`             | `true`                        | Master switch for the background pipeline task (`false` in tests)  |
| `min_samples`                  | `10`                          | Min paired samples before a metric correlation is attempted        |
| `fdr_q`                        | `0.05`                        | Benjamini-Hochberg false-discovery-rate level per detection cycle  |
| `cascade_window_seconds`       | `10`                          | Max spacing between ordered errors that still chains one cascade   |
| `dedup_ttl_seconds`            | `30`                          | Emitted-correlation dedupe cache TTL                               |
| `alert_strength_threshold`     | `0.8`                         | Min strength before the generic alert rule fires                   |
| `alert_confidence_threshold`   | `0.6`                         | Min confidence before the generic alert rule fires                 |
| `alert_cooldown_seconds`       | `60`                          | Per-(rule, pair) alert cooldown                                    |
| `scenario_period_seconds`      | `45`                          | A new incident scenario starts every N s (rotating)               |
| `scenario_duration_seconds`    | `20`                          | Each incident scenario stays active for N s                        |

**Host ports** (`BACKEND_PORT` 8000, `FRONTEND_PORT` 3000, `REDIS_PORT` 6379) are compose-level host mappings, not backend settings. See [`.env.example`](.env.example) for the full committed template.

---

## Testing

Everything is verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a load harness, all profile-gated compose services.

```bash
make test        # 217 unit + integration tests
make e2e         # 14-check black-box verifier vs the live stack
make load        # four hard-gated perf phases vs the live stack
```

- **Unit tests** cover every module — parsers, aggregation, collector, all five detectors, significance math, patterns, alerts, and each API endpoint. The four spec-required correlation-engine areas — **engine initialization**, **temporal detection**, **error-cascade detection**, and **correlation stats** — are all covered (`test_engine_init.py`, `test_temporal.py`, `test_cascade.py`, `test_stats_api.py`).
- **Integration tests** exercise the collector/engine/alerts against a real Redis over the compose network.
- **E2E** (`scripts/verify_e2e.py`) walks 14 ordered checks over the whole loop: health contract, logs from all 5 sources, ingest throughput, all 5 correlation types, stats contract + growth, type-filter purity + `422`, detection-latency p95, session accuracy vs ground truth, the 3 target metric pairs + a DB cascade, alerts (incl. a critical), dashboard shape, pattern re-detection, and backend memory.
- **Load** (`scripts/load_test.py`) gates sustained ingest, sequential stats latency, 100-way concurrent mixed GETs (RPS + error rate), and server-reported memory. Every gate is host-overridable, so e.g. `MIN_EVENTS_PER_SEC=100000 make load` proves the gate bites.

### Measured Performance

From the final Docker verification run (all gates passed):

| Metric                                   | Result                                        | Gate            |
|------------------------------------------|-----------------------------------------------|-----------------|
| Unit + integration tests                 | **217 passing**                               | all green       |
| E2E assertions                           | **14 / 14 passed**                            | all pass        |
| Ingest throughput (E2E)                  | **124.4 events/s**                            | ≥ 100           |
| Detection latency, event pairs (n = 547) | **p95 4.33 s**, max 4.51 s                     | p95 ≤ 5 s       |
| Session accuracy (272 ground-truth journeys) | **recall 1.000**, precision 1.000          | recall ≥ 0.95   |
| Target correlations                      | **3 / 3** metric pairs at `p_adj < 0.05` + DB→web/api cascade | 3/3 + cascade |
| Alerts                                   | **warning + critical firing**                 | ≥ 1 critical    |
| Backend memory (E2E)                     | **149.9 MB**                                  | < 200 MB        |
| Ingest throughput (load)                 | **117.8 events/s**                            | ≥ 100           |
| Stats endpoint, 50 sequential            | **avg 0.9 ms**, p95 1.4 ms, max 2.6 ms         | avg < 50 ms, max < 100 ms |
| Concurrency (500 reqs @ 100 concurrent)  | **1198.5 RPS**, 0.00 error rate, p50 67.2 ms / p95 150.9 ms / max 155.2 ms | ≥ 50 RPS, 0 errors |
| Backend memory (load)                    | **129.0 MB**                                  | < 200 MB        |

---

## What I Learned

- **Benjamini-Hochberg FDR is the right tool for a detector that re-tests many hypotheses.** The metric detector scans ~19 series pairs every 2 seconds — thousands of hypothesis tests an hour — so emitting on raw `p < 0.05` would bury real signals under coincidences. One BH step-up pass per cycle (sort the p-values, keep everything at or below the largest `k` with `p_(k) ≤ (k/m)·q`) bounds the *expected fraction* of false discoveries among emissions instead of the per-test error rate. I compute BH-adjusted q-values too and fold `(1 − p_adj)` straight into confidence, so significance and reported certainty stay consistent.

- **Per-second numpy ring buffers + histogram MI kept the whole thing under 200 MB without pandas or sklearn.** Metrics live in fixed 120-slot float64 rings (slot = `second % 120`); `add_event` does only scalar writes, and reads copy small windows out. Mutual information is just `np.histogram2d` normalized by the smaller marginal entropy — enough to catch nonlinear dependence Pearson misses, with none of the memory footprint of a dataframe/ML stack. Measured backend RSS stayed at ~130–150 MB against a 200 MB ceiling.

- **Synchronous batch detection in one asyncio loop beat per-event coroutines.** At 100+ events/sec, spawning a coroutine per event drowns in scheduling overhead. Instead one task ticks ingest every second and runs all five detectors synchronously every 2 seconds over the accumulated batch + sliding window. No locking (the API reads the same accumulators between ticks on the same loop), and each detector and each tick is individually guarded so one bug logs and is skipped rather than killing the pipeline.

- **The subtlest bug was detection latency, and the fix was anchoring emissions to the *newest* event.** Cascade and user findings dedupe on a 30 s TTL, but the sliding window is also ~30 s — so after a TTL lapsed, a finding could re-emit against events that were already ~30 s old, stamping `detected_at = now` and reporting a detection latency of tens of seconds (the dedupe TTL plus a tick), far past the 5 s budget. A same-source error storm made it worse by keeping a cascade "fresh" while its newest *cross-source* error aged out. The fix: a `FRESHNESS_SECONDS` (4 s) guard on the emitted event (the cascade **leaf**, the user's newest event), so a finding only (re-)emits while genuinely current activity backs it. p95 latency dropped to ~4.3 s.

- **Pattern learning is a cheap recurrence baseline, not a model.** Each `(type, endpoint_a, endpoint_b)` keeps count + strength sum + sum-of-squares, which is enough for a running mean and σ. Recurrence adds a logarithmic confidence boost (capped at 0.15); an established pattern deviating > 2σ raises an anomaly flag that alerts even when absolute strength is modest. Baselines hydrate from Redis once on startup and mirror back fire-and-forget, so learning survives restarts but a dead Redis just degrades to session-local learning — never an exception.

- **Graceful degradation had to be designed in, not bolted on.** No API handler touches Redis — every read serves from in-memory accumulators — so the API and dashboard keep answering through a Redis outage, and `/health` returns `200` while alive with degradation reported in the body (so the container healthcheck tracks liveness, not dependencies). On the frontend, nginx re-resolves the `backend` name per request via Docker DNS, and the SPA keeps its last good snapshot on a failed poll and flags it stale rather than blanking the screen.

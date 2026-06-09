# Bloom Filter Log Membership

A **memory-efficient probabilistic membership service** that answers one question —
**"have we seen this log entry before?"** — with **sub-millisecond lookups**, a
**tunable false-positive rate**, and **zero false negatives**. A long-lived **FastAPI**
service (`:8001`) owns four independent scalable Bloom filters — **`error_logs`**,
**`access_logs`**, **`security_logs`**, and **`sessions`** — rotates them on a daily
schedule, persists them to disk with crash-safe CRC-validated snapshots, fronts a
SQLite "expensive storage" tier as a two-tier lookup pipeline, and feeds a **separate
real-time dashboard process** (`:8002`) that pushes live metrics to browsers over
WebSocket.

**Tech stack:** Python 3.12, FastAPI + Uvicorn, `mmh3` (MurmurHash3), `bitarray`,
pydantic-settings, SQLite (WAL), vanilla JS + vendored Chart.js 4.4.1, Docker Compose,
pytest + httpx.

---

## The Problem

Log pipelines constantly re-encounter the same entries: a producer retries a batch, two
collectors tail the same file, an at-least-once queue redelivers, a crash replays a
segment. Downstream you need a fast, cheap answer to **"is this entry new, or already
processed?"** — for deduplication, idempotent ingestion, or skipping already-indexed
records.

The obvious answer — a hash set of every key ever seen — is exact but stores **every
element**: memory grows linearly and unboundedly, and it must live in RAM to stay fast.
When all you need is *"definitely new"* vs. *"probably seen"*, paying to store every key
is wasteful. A **Bloom filter** trades a small, controllable error for a small, predictable
memory budget: it answers membership in **O(k)** time using only a **bit array**, never
storing the elements themselves.

---

## How a Bloom Filter Answers It

A Bloom filter is a bit array of `m` bits (all `0`) and `k` hash-derived probe positions
per key:

- **`add(x)`** — hash `x` to `k` positions in `[0, m)` and set those bits to `1`.
- **`might_contain(x)`** — re-derive the same `k` positions:
  - **any bit is `0`** → `x` was **definitely never added**. Bits are only ever set,
    never cleared, so no insert sequence could have left it `0` — a "no" is a **proof**.
  - **all bits are `1`** → `x` is **"probably present"** (it was added, *or* other keys
    coincidentally set those bits — a **false positive**, bounded and tunable).

That asymmetry — **zero false negatives, bounded false positives** — is the entire
design, and it is exactly what the API's two confidence strings
(`"definitely_not_exist"` / `"probably_exists"`) encode.

### Sizing (memory vs. accuracy)

For `n` expected items at target false-positive probability `p`:

```
m = ceil( -(n · ln p) / (ln 2)^2 )      # bits in the array
k = round( (m / n) · ln 2 )             # hash probes per key
```

Bits-per-element depend only on `p` — independent of key size:

| Target FP rate `p` | Bits per element | `k` (probes) |
|--------------------|------------------|--------------|
| 10%   | ~4.8 bits  | 3  |
| 1%    | ~9.6 bits  | 7  |
| 0.1%  | ~14.4 bits | 10 |

At 1% FP, a 64-byte log key that costs 512 bits in a hash set costs **~10 bits** here.
The implementation rounds `m` up to a multiple of 8 so the `bitarray` bytes roundtrip
exactly through persistence.

### One hash call, k indices (Kirsch–Mitzenmacher)

Instead of `k` independent hash functions, one seeded `mmh3.hash128` digest is split
into two 64-bit halves `h1`, `h2` (`h2` forced odd so the stride never degenerates),
and all probes are derived as `index_i = (h1 + i·h2) mod m`. Kirsch & Mitzenmacher
(2006) proved this loses nothing asymptotically in FP rate — so every add/query costs
exactly **one** hash call plus `k` modular reads.

---

## What's Actually Built

Two processes plus durable state on a bind-mounted `./data`:

```
        POST /logs/* /pipeline/* /sessions/* /demo/*        browser (live charts, forms)
clients ───────────────────────────────►┐                          ▲
                                        │                          │ GET /  +  WS /ws ticks
                          ┌─────────────┴─────────┐   ┌────────────┴──────────────┐
                          │   API service  :8001  │   │   Dashboard service :8002 │
                          │   owns ALL filters    │◄──┤   polls /stats,           │
                          │  ┌─────────────────┐  │   │   /pipeline/stats,        │
                          │  │ error_logs    SBF│  │   │   /sessions/stats over   │
                          │  │ access_logs   SBF│  │   │   HTTP every 5s, pushes  │
                          │  │ security_logs SBF│  │   │   one tick to every WS   │
                          │  │ sessions      SBF│  │   │   client; POST /proxy/*  │
                          │  └─────────────────┘  │   │   relays page forms to    │
                          │  each: current +      │   │   the API (no CORS)       │
                          │  previous generation  │   └───────────────────────────┘
                          └──────┬─────────┬──────┘
                       snapshots │         │ ground truth (two-tier pipeline)
                                 ▼         ▼
                      ┌───────────────┐  ┌───────────────┐
                      │ data/*.bloom  │  │ data/logs.db  │
                      │ SBF1/BLM1 +   │  │ SQLite (WAL)  │
                      │ CRC32, atomic │  │               │
                      └───────────────┘  └───────────────┘
```

The dashboard **never imports the filter code** — it reaches the API over HTTP only, so
a browser refresh storm or a slow WebSocket client can never steal event-loop time from
the hot add/query path, and there is no second copy of filter state to diverge.

### Scalable filters (adaptive sizing)

Each filter is a **Scalable Bloom Filter** (Almeida et al., 2007): a series of plain
Bloom filter *slices*. Inserts land in the newest slice; when it reaches capacity a new
slice is appended with **2× the capacity** (`SBF_GROWTH_FACTOR`) and a **tighter error
budget** (`SBF_TIGHTENING_RATIO` 0.85). Queries OR across slices. Slice `i` gets:

```
capacity_i = initial_capacity · 2^i
fp_i       = target · (1 − 0.85) · 0.85^i      # geometric series → sums to exactly `target`
```

The `(1 − r)` down-payment on slice 0 is what makes "compound FP ≤ target" a theorem
rather than a hope (granting slice 0 the full target would compound to ~6.7× it). The
price: slice 0 is sized for a tighter `p` than the advertised target —

| Filter | Slice-0 capacity | Compound FP target | Slice-0 FP budget | Slice-0 size |
|---|---|---|---|---|
| `error_logs`    | 1,000,000 | 0.01  | 0.0015  | 1,691,709 B ≈ 1.61 MiB |
| `access_logs`   | 5,000,000 | 0.05  | 0.0075  | 6,364,895 B ≈ 6.07 MiB |
| `security_logs` | 100,000   | 0.001 | 0.00015 | 229,078 B ≈ 224 KiB    |
| `sessions`      | 1,000,000 | 0.01  | 0.0015  | 1,691,709 B ≈ 1.61 MiB |

### Rotation generations (time-based expiry)

Bloom filters cannot delete, so freshness comes from **rotation**: each filter holds a
`current` and an optional `previous` generation. A background task rotates any
generation older than `ROTATION_MAX_AGE_SECONDS` (default daily): `current` is demoted
to `previous`, a fresh empty filter takes over. Queries check current **then** previous,
so keys stay answerable across exactly one rotation boundary; after **two** rotations
the oldest generation's keys read `definitely_not_exist` again — the deliberate trade
for bounded memory and a self-resetting FP rate (see Caveats).

### Two-tier pipeline (bloom in front of expensive storage)

`/pipeline/*` and `/sessions/*` put the filters in front of a SQLite ground-truth tier:

- **ingest** writes both tiers in one call (SQLite row first, filter bits second) — the
  filter auto-updates as logs arrive.
- **lookup** asks the filter first. A **negative short-circuits storage entirely** (the
  payoff — most dedup lookups are misses); a positive is **verified** against SQLite,
  and a disproved positive is counted as an **observed false positive**.
- **fallback**: every lookup first reads the current generation's live fill-based FP
  estimate. Above `FP_FALLBACK_THRESHOLD` (0.05) the filter is bypassed (a saturated
  filter rarely says "no", so it saves nothing) and, with `FP_ROTATE_ON_BREACH`, the
  breach triggers exactly **one** rotation to restore health — re-armed only after the
  estimate drops back under the threshold, so a sustained breach can't cause a rotation
  storm.

### Crash-safe snapshots

A background task snapshots every filter each `SNAPSHOT_INTERVAL_SECONDS` (plus once at
shutdown); startup reloads whatever validates. Writes are atomic — serialize under the
filter lock, then `tmp file → fsync → rename` outside it — so a crash leaves either the
old or the new complete snapshot, never a torn one. Corrupt/missing/mismatched files
mean a fresh filter plus a logged warning, never a crash loop.

```
BLM1 (one fixed-size filter, little-endian)     SBF1 (a scalable series)
─────────────────────────────────────────────   ──────────────────────────────────────────
magic "BLM1" | version u16                      magic "SBF1" | version u16
m u64 | k u16 | n u64 | p f64                   n0 u64 | target_fp f64 | growth u16
seed u64 | count u64                            tightening f64 | seed u64 | slice_count u16
raw bits (m/8 bytes, bitarray.tobytes)          slice_count × (length u32 + full BLM1 blob)
CRC32 trailer over everything above             CRC32 trailer over everything above
```

The header stores the exact geometry (`m`, `k`, seed) the bits were written with —
restoring never re-runs the sizing math, and the CRC turns silent corruption into a
detected rejection.

---

## API Reference

Membership API — base URL `http://localhost:8001`:

| Method | Path | Body / params | Response |
|--------|------|---------------|----------|
| `POST` | `/logs/add` | `{"log_type": "error_logs", "log_key": "req-123"}` | `{"status": "added", "processing_time_ms": 0.012}` |
| `POST` | `/logs/query` | same body | `{"might_exist": true, "confidence": "probably_exists", "processing_time_ms": 0.008}` — or `false` / `"definitely_not_exist"` |
| `GET`  | `/stats` | — | per-filter gauges + ops + totals (tree below) |
| `GET`  | `/health` | — | `{"status": "healthy"}` |
| `POST` | `/demo/populate` | `?count=10000` | `{"status": "completed", "records_added": 10000}` (round-robined across the 3 log types) |
| `POST` | `/demo/performance-test` | `?lookups=2000&dataset_size=20000` | bloom-vs-linear benchmark: `bloom_avg_ms`, `linear_avg_ms`, `speedup_vs_linear`, `bloom_memory_bytes`, `memory_ratio`, `false_positives_observed`, … |
| `POST` | `/pipeline/ingest` | `{"log_type": ..., "log_key": ...}` | `{"status": "stored", "bloom_updated": true, "duplicate": false, "processing_time_ms": ...}` |
| `POST` | `/pipeline/lookup` | same body | `{"found": ..., "might_exist": ..., "source": "bloom_negative" \| "storage", "storage_checked": ..., "false_positive": ..., "fallback_active": ..., "processing_time_ms": ...}` |
| `GET`  | `/pipeline/stats` | — | per filter: `storage_rows`, `lookups`, `bloom_negatives`, `storage_skipped_pct`, `storage_hits`, `false_positives`, `observed_fp_rate`, `fallback_active`, `fallback_lookups`, `rotations_triggered` + `_totals` |
| `POST` | `/sessions/ingest` | `{"session_id": "sess-abc"}` | `{"status": "stored", "duplicate": false, "processing_time_ms": ...}` |
| `POST` | `/sessions/query` | same body | `{"session_id": ..., "might_exist": ..., "found": ..., "source": ..., "storage_checked": ..., "confidence": "probably_exists" \| "definitely_not_exist", "processing_time_ms": ...}` |
| `GET`  | `/sessions/stats` | — | `{"filter": {...}, "memory_under_2mb": true, "pipeline": {...}, "ops": {...}}` |
| `POST` | `/sessions/performance-test` | `?sessions=5000&lookups=2000` | with/without-bloom timings, `speedup`, `storage_calls_avoided_pct`, `non_existent_correctly_identified_pct`, `filter_memory_mb`, … |

`log_type` is a strict enum (`error_logs` / `access_logs` / `security_logs`) — anything
else is a 422. A bloom negative on `/logs/query` is a **guarantee** within the
two-generation retention window; a positive is *probably*, with `estimated_fp_rate` in
`/stats` quantifying the doubt.

```
GET /stats
├─ service, uptime_seconds
├─ filters.<name>                      # error_logs / access_logs / security_logs / sessions
│  ├─ elements_added, capacity, slice_count, rotations, previous_count
│  ├─ memory_bytes, memory_mb, fill_ratio, estimated_fp_rate, target_fp_rate
│  ├─ adds_total, queries_total, positives, negatives
│  ├─ observed_false_positives, observed_fp_rate
│  ├─ avg_add_ms, p99_add_ms, avg_query_ms, p50_query_ms, p99_query_ms
│  └─ created_at, generation_age_seconds
└─ totals: elements_added, adds_total, queries_total, memory_bytes, memory_mb
```

Dashboard — base URL `http://localhost:8002`:

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/` | Single-page dashboard: 4 per-filter stat cards, add/query forms, session query box, FP-over-time + memory charts, 5s auto-refresh |
| `WS`   | `/ws` | Live feed — immediate tick on connect, then one per refresh interval: `{"type": "tick", "ts": ..., "refresh_ms": ..., "api": <GET /stats>, "pipeline": <GET /pipeline/stats>, "sessions": <GET /sessions/stats>, "error": null}` (payloads are all-or-nothing; an API outage nulls them and fills `error`) |
| `POST` | `/proxy/add` `/proxy/query` `/proxy/session-query` | Thin relays to the API's `/logs/add`, `/logs/query`, `/sessions/query` — the browser only ever talks to `:8002`, so no CORS anywhere |
| `GET`  | `/health`, `/static/*` | Liveness probe; page assets incl. the vendored Chart.js (zero CDN calls at runtime) |

---

## How to Run (Docker-first)

```bash
cd bloom-filter-log-membership

make build      # build app + tester images
make up         # API on http://localhost:8001, dashboard on http://localhost:8002

# probe the membership API
curl -X POST http://localhost:8001/logs/add \
     -H 'Content-Type: application/json' \
     -d '{"log_type": "error_logs", "log_key": "req-123"}'
curl -X POST http://localhost:8001/logs/query \
     -H 'Content-Type: application/json' \
     -d '{"log_type": "error_logs", "log_key": "req-123"}'    # → probably_exists
curl http://localhost:8001/stats

# two-tier pipeline and sessions
curl -X POST http://localhost:8001/pipeline/lookup \
     -H 'Content-Type: application/json' \
     -d '{"log_type": "access_logs", "log_key": "never-seen"}'   # → storage_checked: false
curl -X POST http://localhost:8001/sessions/ingest \
     -H 'Content-Type: application/json' -d '{"session_id": "sess-42"}'

make test            # full pytest suite in Docker (unit + integration + e2e)
make test-unit       # tests/unit only        (test-int / test-e2e likewise)
make e2e             # cross-container E2E verifier against the live stack
make load            # containerized load test (ops/s, qps, latency, memory gates)

make logs            # tail the API        (logs-dashboard for the dashboard)
make down            # stop the stack      (clean also removes volumes + dangling images)
```

The compose file gates non-serving containers behind **profiles**: `test` (pytest
runner), `e2e` (verifier that waits for both healthchecks and drives the stack by
service name), and `loadtest`. `make e2e` / `make load` tear the stack down afterwards
and propagate the container's exit code. Filter snapshots and the SQLite file live on
the `./data` bind mount, so membership survives `docker compose restart`.

---

## Configuration

Every setting is an env var (or `.env` entry — see `.env.example`), parsed by
pydantic-settings:

| Variable | Default | Purpose |
|----------|---------|---------|
| `API_HOST` / `API_PORT` | `0.0.0.0` / `8001` | Membership API bind. |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | `0.0.0.0` / `8002` | Dashboard process bind. |
| `API_BASE_URL` | `http://localhost:8001` | Where the dashboard reaches the API (compose: `http://app:8001`). |
| `DASHBOARD_REFRESH_MS` | `5000` | Poll-and-broadcast tick cadence. |
| `DATA_DIR` | `./data` | Directory for `*.bloom` snapshots (compose: `/app/data`). |
| `ERROR_LOGS_CAPACITY` / `ERROR_LOGS_FP_RATE` | `1000000` / `0.01` | `error_logs` slice-0 capacity / compound FP target. |
| `ACCESS_LOGS_CAPACITY` / `ACCESS_LOGS_FP_RATE` | `5000000` / `0.05` | `access_logs` sizing. |
| `SECURITY_LOGS_CAPACITY` / `SECURITY_LOGS_FP_RATE` | `100000` / `0.001` | `security_logs` sizing. |
| `SESSIONS_CAPACITY` / `SESSIONS_FP_RATE` | `1000000` / `0.01` | `sessions` sizing (1M daily IDs; 0.01 keeps slice 0 ≈ 1.61 MiB, under the 2 MB criterion). |
| `SBF_GROWTH_FACTOR` | `2` | Capacity multiplier per appended slice. |
| `SBF_TIGHTENING_RATIO` | `0.85` | FP-budget ratio per appended slice. |
| `SNAPSHOT_INTERVAL_SECONDS` | `30` | Background `save_all()` cadence. |
| `ROTATION_MAX_AGE_SECONDS` | `86400` | Rotate a generation at this age (`0` disables). |
| `ROTATION_CHECK_INTERVAL_SECONDS` | `60` | How often the rotation task checks ages. |
| `SQLITE_PATH` | `./data/logs.db` | The "expensive storage" ground-truth tier. |
| `FP_FALLBACK_THRESHOLD` | `0.05` | Live-FP estimate above which lookups bypass the bloom tier. |
| `FP_ROTATE_ON_BREACH` | `true` | Grant one health-restoring rotation per breach episode. |
| `LOG_LEVEL` | `INFO` | Stdlib logging level. |

---

## Measured Results

All numbers from the containerized suites (`make test` / `make e2e` / `make load`) on
an Apple-silicon laptop running Docker — **183 tests green**, E2E verifier **16/16
checks PASS**.

| Success criterion | Target | Measured |
|---|---|---|
| Avg query latency | < 1 ms | **0.0019 ms** (in-process managed path) ✓ |
| Throughput | ≥ 10,000 ops/s | **390k–404k** mixed ops/s — 39× headroom ✓ |
| False negatives | 0 | **0**, by construction and observed ✓ |
| False positives | within per-filter targets (< 5%) | compound estimates ≤ target on every filter ✓ |
| Sessions filter memory (1M IDs) | < 2 MB | **1,691,709 B ≈ 1.61 MiB** ✓ |
| Filter memory vs full-key storage | < 5% | **1.99%–3.58%** per filter ✓ |
| Speedup vs linear search | 100×+ | **106–110×** ✓ |

- **Bloom vs linear search** (`/demo/performance-test`, 20k dataset, 2k lookups):
  bloom averages **~0.0009 ms/lookup**, **106–110×** faster than a linear list scan,
  with `memory_ratio` **0.0187** — the bit array costs 1.9% of storing the full
  64-byte keys.
- **Load** (`make load`): the in-process managed path (lock + metrics + two-generation
  query) sustains **390k–404k mixed add/query ops/s** at **0.0019 ms** avg query; the
  raw scalable filter alone does 538k–542k ops/s. The HTTP phase measured
  **~1,430 qps at p50 4.8 ms with 0 errors** (16 async workers) — explicitly
  **client-bound**: the Python load generator saturates ~1 CPU core while the
  single-worker server idles around 6% CPU, so the server itself has ≥10× headroom
  beyond that figure.
- **Memory ratios vs 64 B/key storage** (from `/stats` after seeding): `error_logs`
  0.0264, `access_logs` 0.0199, `security_logs` 0.0358, `sessions` 0.0264.
- **Sessions** (`/sessions/performance-test`): **100% of non-existent sessions
  correctly identified** (a bloom negative is a proof; the rare false positive is
  corrected by storage verification), and **50% of storage calls avoided** at the
  benchmark's 50/50 present/absent mix — miss-heavy real traffic pushes that toward
  100%. Honest note: against this *warm, in-process* SQLite (~µs point SELECTs) the
  bloom tier does **not** win on raw per-call latency — measured speedup 0.6–0.9×,
  reported as-is. Its win here is **structural** (calls avoided outright) and scales
  with what storage actually costs in production: remote, disk-bound, or contended
  stores with ms-scale round trips, not a local SQLite.

---

## Caveats (by design)

- **Single uvicorn worker, always.** All filter state lives in-process; `workers > 1`
  would give each worker a divergent copy of every filter and shard membership across
  processes. The Dockerfile and compose run exactly one worker — scale reads by
  putting queries behind the pipeline, not by adding workers.
- **No deletion.** Clearing one key's bits could flip a bit shared with another key and
  silently create a false negative. Expiry is rotation's job: bounded memory, daily
  freshness, self-resetting FP rate.
- **`bloom_negative` means "not admitted within the last two generations"** — not
  "never in history". After two rotations, an old key's bits are gone even though its
  row may still sit in SQLite; the SQLite tier remains ground truth for all time, and
  the filters answer the *recent*-membership question dedup actually needs.
- **FP estimates are fill-based** (`(bits_set/m)^k` compounded across slices) —
  estimates, not measurements. The *observed* FP rate (bloom positives that storage
  disproved) is tracked separately in `/pipeline/stats` and `/stats`.

---

## What I Learned

- How the `m`/`k` sizing formulas turn a target error rate into a **fixed bits-per-element
  cost** (independent of key size), and how **Kirsch–Mitzenmacher double hashing**
  gets `k` quality probe positions from a single `mmh3.hash128` call (split, force the
  stride odd, stride mod m).
- The scalable-filter **error-budget subtlety**: slice budgets must be a geometric
  series summing to the target (`target·(1−r)·r^i`). Granting slice 0 the full target
  and "tightening from there" compounds to ~6.7× the advertised rate at r = 0.85 — the
  difference between a guarantee and a hope is the `(1−r)` down-payment.
- **Crash-safe persistence for binary blobs**: serialize under the lock, write
  tmp + fsync + rename outside it, CRC32 the whole frame, and treat any invalid
  snapshot as "start fresh with a warning" — a service that crash-loops on a bad file
  can never heal itself.
- **Two-process observability**: the dashboard reads the API over HTTP only and fans
  ticks out over WebSocket, so charts and refresh storms can never steal event-loop
  time from the µs-scale hot path — and `async def`-without-await beats threadpool
  dispatch for µs operations.
- **Honest benchmarking, twice over**: a bloom filter does *not* beat a warm in-process
  SQLite on per-call latency — its real win is the storage calls it eliminates, which
  scales with true storage cost; and a load test that reports ~1.4k qps while the
  server idles at 6% CPU is measuring the **client**, not the server — say so in the
  report instead of shipping the smaller number as a capability claim.

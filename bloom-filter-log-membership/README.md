# Bloom Filter Log Membership

A **memory-efficient probabilistic data structure** that answers one question —
**"have we seen this log entry before?"** — with **sub-millisecond lookups**, a
**tunable false-positive rate**, and **zero false negatives**. It runs as a
long-lived **FastAPI** service exposing `add` / `query` endpoints, paired with a
**separate real-time web dashboard** process, and **persists its state to disk** so the
membership set survives restarts.

> **Status: scaffold (not yet implemented).**
> This commit contains only `README.md`, `requirements.txt`, and `.gitignore` — it is the
> project skeleton and design spec. There is **no application code, no Dockerfile, and no
> tests yet**. Everything below under *Planned …* describes the intended design and will be
> built in follow-up commits **once approved**. Sections are written in the present tense as
> a design contract, not as a claim that the code already exists.

---

## The Problem

Log pipelines constantly re-encounter the same entries: a producer retries a batch, two
collectors tail the same file, an at-least-once queue redelivers, or a crash replays a
segment. Downstream you often need a fast, cheap answer to **"is this log entry new, or have
we already processed it?"** — for deduplication, idempotent ingestion, or skipping
already-indexed records.

The obvious answer is a hash set of every ID we've seen. It's exact, but it stores (a hash
of) **every element**, so its memory grows linearly and unboundedly with the data — tens to
hundreds of MB for tens of millions of entries, and it must live in RAM to stay fast. When
you only need to know *"definitely new"* vs. *"probably seen"*, paying to store every key is
wasteful.

A **Bloom filter** trades a small, controllable error for a large, fixed memory budget: it
answers membership in **constant time** using only a **bit array**, never storing the
elements themselves.

---

## The Core Idea — How a Bloom Filter Answers the Question

A Bloom filter is a bit array of `m` bits (all `0` initially) and `k` independent hash
functions. For each item (here: a log entry's identity — a line, a content hash, or an ID):

- **`add(x)`** — hash `x` with the `k` functions to get `k` positions in `[0, m)`, and set
  those `k` bits to `1`.
- **`query(x)`** — hash `x` the same way and look at those `k` bits:
  - if **any** bit is `0` → `x` was **definitely never added** → **"new"**.
  - if **all** bits are `1` → `x` is **"possibly seen"** (it was added, *or* its bits were
    coincidentally set by other items — a **false positive**).

This asymmetry is the whole point:

- ✅ **Zero false negatives.** A *"new"* answer is **always correct** — the structure never
  forgets something it has seen. (True as long as the filter only grows; standard Bloom
  filters do not support deletion — see *Caveats*.)
- ⚠️ **False positives are possible but bounded and tunable.** A *"possibly seen"* answer
  might be wrong. The rate is a design parameter you trade against memory.

### Sizing (memory vs. accuracy)

For an expected `n` items and a target false-positive probability `p`, the optimal parameters
are:

```
m = ceil( -(n · ln p) / (ln 2)^2 )      # bits in the array
k = round( (m / n) · ln 2 )             # number of hash functions
```

This makes the cost concrete and constant-per-element, independent of how big each log line
is:

| Target false-positive rate `p` | Bits per element | `k` (hashes) |
|--------------------------------|------------------|--------------|
| 10%   | ~4.8 bits  | 3 |
| 1%    | ~9.6 bits  | 7 |
| 0.1%  | ~14.4 bits | 10 |

At **1% FP**, **10 million** log entries need only **~9.6 Mbit ≈ 12 MB** of RAM — *regardless
of whether each entry is 40 bytes or 4 KB* — versus the hundreds of MB an exact hash set of
the same keys would consume.

### Why lookups are sub-millisecond

A lookup is just `k` hash computations plus `k` random-access bit reads — **O(k)**, constant,
with no allocation and no element comparison. `k` is single digits, so each `add` / `query`
is on the order of microseconds. We use **MurmurHash3** (via `mmh3`) and the
**Kirsch–Mitzenmacher double-hashing** trick: compute a single 128-bit digest, split it into
two 64-bit halves `h1`, `h2`, and derive all `k` indices as `(h1 + i·h2) mod m` for
`i = 0..k-1` — `k` good indices from **one** hash call.

---

## How It Runs — Two Processes + Disk

Per the brief, the system is split into two long-lived processes plus a durable on-disk
snapshot:

1. **API service** — a long-lived **FastAPI / Uvicorn** app that owns the Bloom filter in
   memory and exposes `add` / `query` / `stats` / `health`. It snapshots state to disk
   periodically and on shutdown, and **reloads it on startup** so membership survives
   restarts.
2. **Real-time dashboard** — a **separate process** (its own Uvicorn server) that serves a
   live web page and streams metrics — fill ratio, estimated false-positive rate, item count,
   add/query throughput — over a WebSocket. It reads from the API service (polling `/stats`
   and/or subscribing to the API's metrics `/ws`) and relays updates to the browser. Keeping
   it as its own process means dashboard load never competes with the hot `add` / `query`
   path.

```
            add / query / stats                          browser (live charts)
  clients ───────────────────────────►┐                         ▲
                                       │                         │ HTML + WebSocket
                             ┌─────────┴───────────┐   ┌─────────┴────────────┐
                             │   API service       │   │   Dashboard service   │
                             │   (FastAPI/Uvicorn) │   │   (FastAPI/Uvicorn)   │
                             │                     │   │                       │
                             │   POST /add         │   │   GET  /   (page)     │
                             │   POST /query       │   │   WS   /ws (push)     │
                             │   GET  /stats       │◄──┤   reads API /stats    │
                             │   GET  /health      │   │   (or API /ws), then  │
                             │   WS   /ws (metrics)│   │   relays to browsers  │
                             └─────────┬───────────┘   └───────────────────────┘
                                       │  BloomFilter = bit array (m bits) + k hashes
                                       ▼
                             ┌─────────────────────┐
                             │   snapshot on disk   │   data/*.bloom  (bit array + params)
                             │   load on startup    │   atomic write via *.bloom.tmp + rename
                             └─────────────────────┘
```

---

## Planned API

> *Intended* contract for the API service. Base URL `http://localhost:8000`. Subject to
> refinement during implementation.

| Method | Path | Body / params | Response |
|--------|------|---------------|----------|
| `POST` | `/add`    | `{"key": "<log entry / id>"}` (or `{"keys": [...]}` for a batch) | `{"added": <int>, "count": <n>, "fill_ratio": <0..1>}` |
| `POST` | `/query`  | `{"key": "<log entry / id>"}` (or `{"keys": [...]}`) | `{"key": "...", "seen": <bool>, "certainty": "definitely_new" \| "possibly_seen"}` |
| `GET`  | `/stats`  | — | `{"m_bits", "k_hashes", "count", "bits_set", "fill_ratio", "estimated_fp_rate", "target_fp_rate", "capacity", "memory_bytes", "throughput": {...}}` |
| `GET`  | `/health` | — | `{"status": "healthy"}` (liveness probe) |
| `WS`   | `/ws`     | — | Periodic metrics ticks consumed by the dashboard process |

**Membership semantics:** `seen: false` is a **guarantee** the key is new (zero false
negatives); `seen: true` means *possibly* seen, with `estimated_fp_rate` quantifying the
chance it's a false positive.

---

## Tech Stack

- **Language:** Python 3.12
- **API framework:** FastAPI + Uvicorn (long-lived ASGI service)
- **Hashing:** **MurmurHash3** via `mmh3` (fast, non-cryptographic) with Kirsch–Mitzenmacher
  double hashing
- **Bit storage:** `bitarray` (compact bit array + fast binary `tofile()` / `fromfile()`)
- **Config / validation:** **Pydantic v2** + **pydantic-settings**
- **Dashboard:** a separate Uvicorn process serving vanilla HTML + a WebSocket (charts via a
  vendored JS lib — no Python charting dependency)
- **Persistence:** binary snapshot of the bit array + filter parameters, written atomically
  (temp file + rename) and reloaded on startup
- **Testing:** `pytest` + `httpx` (FastAPI `TestClient` and an E2E client)

---

## Persistence Model (planned)

The filter's durable state is just the **bit array** plus a small **header** of parameters
(`m`, `k`, target FP rate, item count, hash seed, format version). On a timer and at shutdown
the service writes this snapshot to `data/*.bloom` **atomically** — write to `*.bloom.tmp`,
`fsync`, then `rename` over the real file — so a crash mid-write can never corrupt the live
snapshot. On startup the service loads the snapshot if present (validating the header), or
initializes an empty filter from configuration otherwise.

---

## Planned Configuration (env vars)

> Will be driven by `pydantic-settings`; a `.env.example` listing every setting with its
> default will ship with the implementation.

| Variable | Example default | Purpose |
|----------|-----------------|---------|
| `EXPECTED_ITEMS`     | `1_000_000` | `n` used to size the filter (`m`, `k`). |
| `TARGET_FP_RATE`     | `0.01`      | Target false-positive probability `p`. |
| `DATA_DIR`           | `./data`    | Where the `*.bloom` snapshot lives. |
| `SNAPSHOT_INTERVAL_SECONDS` | `30` | How often to persist state to disk. |
| `API_HOST` / `API_PORT`     | `0.0.0.0` / `8000` | API service bind. |
| `DASHBOARD_PORT`     | `8050`      | Dashboard process bind. |
| `API_BASE_URL`       | `http://localhost:8000` | Where the dashboard reaches the API. |
| `WS_PUSH_INTERVAL_SECONDS`  | `2.0` | Dashboard metrics broadcast interval. |
| `LOG_LEVEL`          | `INFO`      | Log level. |

---

## Planned Project Structure

> None of these source files exist yet — this is the target layout.

```
bloom-filter-log-membership/
├── README.md            # this file
├── requirements.txt     # dependencies (present)
├── .gitignore           # ignores (present)
├── .env.example         # every Settings field with its default        (planned)
├── src/
│   ├── bloom.py         # the BloomFilter: sizing, add/query, hashing   (planned)
│   ├── persistence.py   # atomic snapshot write + load-on-startup       (planned)
│   ├── settings.py      # pydantic-settings config                      (planned)
│   ├── metrics.py       # bounded throughput / fill-ratio aggregator    (planned)
│   ├── api.py           # FastAPI app: /add /query /stats /health /ws   (planned)
│   └── dashboard.py     # separate dashboard process (page + WebSocket) (planned)
└── tests/
    ├── unit/            # sizing math, no false negatives, FP-rate bound (planned)
    ├── integration/     # API + persistence reload                      (planned)
    └── e2e/             # add → restart → query survives                (planned)
```

---

## How to Run

> ⚠️ **Planned — nothing is runnable yet.** Shown here as the intended workflow once the code
> lands. Per the brief there are **no Docker files in this commit**; a Dockerfile /
> docker-compose setup will be added later following the repo convention.

```bash
# (after implementation) install deps
pip install -r requirements.txt

# terminal 1 — long-lived API service
uvicorn src.api:app --host 0.0.0.0 --port 8000

# terminal 2 — separate real-time dashboard process
uvicorn src.dashboard:app --host 0.0.0.0 --port 8050
# then open http://localhost:8050

# probe the API
curl -X POST http://localhost:8000/add   -H 'Content-Type: application/json' -d '{"key":"log-line-42"}'
curl -X POST http://localhost:8000/query -H 'Content-Type: application/json' -d '{"key":"log-line-42"}'
curl http://localhost:8000/stats
```

---

## Caveats (by design)

- **No deletion / no expiry in a standard Bloom filter.** Clearing a bit for one item could
  flip a shared bit and silently create a false negative for another — which would break the
  core *zero-false-negatives* guarantee. If sliding-window "have we seen this *recently*"
  semantics are needed later, the path is a **counting Bloom filter** (per-slot counters,
  more memory) or a **rotating / segmented** design — explicitly out of scope for the first
  cut.
- **False positives are inherent.** The structure answers *"probably seen"*, never *"seen for
  certain"*. The observed rate rises as the filter fills past its design `n`; `/stats`
  reports the live `estimated_fp_rate` so this stays visible.
- **In-memory hot path.** The bit array lives in RAM for speed; disk is only the durable
  snapshot, not the query path.

---

## What I Want to Learn

- How a Bloom filter turns a tunable error budget into a **fixed, predictable memory cost**,
  and how the `m`/`k` sizing formulas fall out of minimizing the false-positive rate.
- The **Kirsch–Mitzenmacher** result that `k` quality hash indices can be derived from **two**
  hashes with no measurable loss in false-positive rate — one `mmh3` call per operation.
- Writing **crash-safe persistence** for a large binary blob (atomic temp-file + rename) and
  reloading it on startup with header validation.
- Structuring a system as **two cooperating processes** — a hot API path and a separate
  real-time dashboard — so observability never steals cycles from `add` / `query`.
- Being honest about a probabilistic structure's guarantees: **what it promises (no false
  negatives)** versus **what it only approximates (membership)**.

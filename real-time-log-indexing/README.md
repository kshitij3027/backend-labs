# Real-Time Log Indexing

A streaming log indexing system that makes logs searchable within **milliseconds of arrival**. Logs flow through a Redis stream into a background processor that writes into a hybrid **in-memory + disk-backed inverted index**, all exposed through a FastAPI web dashboard on `http://localhost:8080/`.

---

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** FastAPI (REST API + Jinja2-served dashboard)
- **Stream Transport:** Redis Streams (producer → consumer group → background worker)
- **Index:**
  - In-memory inverted index (hot tier, recent logs, posting lists in RAM)
  - Disk-backed inverted index (warm tier, flushed segments, memory-mapped)
- **Frontend:** Vanilla JS dashboard served by FastAPI (no build step)
- **Containerization:** Docker & Docker Compose
- **Testing:** pytest, pytest-asyncio, httpx

---

## What This Project Does

Most log search systems trade off between **indexing latency** (how fast a new log becomes queryable) and **query throughput**. This project targets sub-millisecond indexing latency by:

1. **Streaming ingest** — logs are pushed onto a Redis stream as soon as they are produced.
2. **Background indexer** — a long-running asyncio worker consumes the stream, tokenizes each log, and updates the in-memory inverted index in place. A log is searchable the moment the worker advances past it, typically **<10 ms after arrival**.
3. **Tiered index** — the in-memory tier holds the last N minutes of logs for hot search. On a configurable interval, finalized segments are flushed to disk as immutable sorted posting lists (LSM-ish). Searches fan out across both tiers and merge results.
4. **Dashboard** — a browser UI on `http://localhost:8080/` lets the user generate sample logs, watch them flow through the pipeline in real time, and run search queries with term highlighting.

---

## How It Runs

Long-lived server with an HTTP API + web dashboard. A single FastAPI process hosts:

- The REST API (ingest, search, stats, control endpoints)
- The Jinja-rendered dashboard at `GET /`
- A **background stream processor** (asyncio task started in the FastAPI lifespan) that consumes from Redis and updates the index

The user interacts via the browser. Sample log generation and search are triggered through dashboard buttons that call the API — there is no separate CLI to run.

```
┌─────────────────────────────────────────────────────────────────┐
│                       Docker Compose                             │
│                                                                  │
│  ┌───────────────────────────────┐    ┌───────────────────────┐ │
│  │        FastAPI Process         │    │        Redis          │ │
│  │                                │    │                       │ │
│  │  ┌──────────────────────────┐ │    │  Streams:              │ │
│  │  │  HTTP API + Dashboard    │ │◄──►│   logs:ingest          │ │
│  │  │  (port 8080)             │ │    │  Consumer group:       │ │
│  │  └──────────────────────────┘ │    │   indexer              │ │
│  │                                │    │                       │ │
│  │  ┌──────────────────────────┐ │    └───────────────────────┘ │
│  │  │  Background Indexer      │ │                               │
│  │  │  (asyncio worker)        │ │    ┌───────────────────────┐ │
│  │  └────────────┬─────────────┘ │    │   Disk (mounted vol)  │ │
│  │               │                │    │                       │ │
│  │               ▼                │    │  segments/            │ │
│  │  ┌──────────────────────────┐ │◄──►│    seg-0001.postings  │ │
│  │  │  Hybrid Inverted Index   │ │    │    seg-0002.postings  │ │
│  │  │  - in-memory (hot)       │ │    │    ...                │ │
│  │  │  - disk segments (warm)  │ │    └───────────────────────┘ │
│  │  └──────────────────────────┘ │                               │
│  └───────────────────────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Planned API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Interactive dashboard (HTML + vanilla JS) |
| GET | `/static/*` | Frontend JS/CSS assets |
| GET | `/health` | Liveness probe: index state, Redis reachable, lag |
| POST | `/api/logs` | Push a single log (or list) onto the ingest stream |
| POST | `/api/logs/generate?count=N&rate=R` | Generate `N` synthetic logs at `R` logs/sec onto the stream |
| GET | `/api/search?q=...` | Full-text search across hot + warm tiers with highlighting |
| GET | `/api/stats` | Doc counts per tier, vocabulary size, ingest lag, flush cadence |
| POST | `/api/flush` | Force-flush the current in-memory segment to disk |

---

## Planned Dashboard Features

- **Live ingest panel** — "Generate 1 000 logs" / "Stream at 500 logs/sec" buttons that drive the `/api/logs/generate` endpoint.
- **Search bar** — debounced queries against `/api/search`, matched terms wrapped in `<mark>`.
- **Live stats** — doc count, in-memory segment size, number of disk segments, consumer-group lag, tail of the pipeline latency histogram — refreshed every second.
- **Segment inspector** — lists disk segments with size, doc range, and term count; lets the user force-flush the current in-memory segment.

---

## What I Expect to Learn

- How to decouple **ingest latency** from **persistence latency** using a streaming transport (Redis Streams) + background worker so the HTTP path never blocks on an fsync.
- How to structure a **tiered inverted index** — an append-only in-memory hot tier plus immutable flushed disk segments, with a merging search path across the two.
- How to run a **long-lived background task** alongside FastAPI via the lifespan context so the API and the indexer share one process and one event loop.
- How to expose **live system internals** (lag, segment state, flush cadence) through the dashboard so the behaviour of the pipeline is observable without reading logs.

---

## Project Structure

```
real-time-log-indexing/
├── Dockerfile                        # multi-stage runtime image (uvicorn app)
├── Dockerfile.test                   # slim pytest image for the test profile
├── docker-compose.yml                # redis + app + test profile + volumes
├── Makefile                          # build/up/down/clean/test/e2e/load/demo/ui
├── start.sh / stop.sh / cleanup.sh   # operational scripts
├── requirements.txt
├── pytest.ini
├── README.md
├── src/
│   ├── config.py                     # pydantic-settings Settings singleton
│   ├── models.py                     # LogEntry, SearchRequest/Response, Stats, ...
│   ├── logging_setup.py              # structured JSON stdout logger
│   ├── sample_data.py                # synthetic log templates + generator
│   ├── main.py                       # FastAPI app + lifespan wiring
│   ├── index/
│   │   ├── tokenizer.py              # LogTokenizer (IP/email/URL compound tokens)
│   │   ├── segment.py                # in-memory Segment (three posting maps)
│   │   ├── persistence.py            # atomic JSONL+gzip segment read/write
│   │   ├── inverted_index.py         # orchestrator (current + flushed + disk)
│   │   └── merger.py                 # background segment merge loop
│   ├── stream/
│   │   └── redis_consumer.py         # XREADGROUP with batching + backoff
│   └── api/
│       ├── routes.py                 # /api/search, /api/generate-sample, ...
│       ├── websocket.py              # ConnectionManager + broadcast loops
│       └── dashboard.py              # Jinja "/" route
├── templates/dashboard.html          # dashboard skeleton (stat cards, search, feed)
├── static/app.js + app.css           # dashboard JS (WS + poll fallback) and styles
├── scripts/
│   ├── load_test.py                  # success-criteria gate (p95, throughput, lag)
│   └── demo.py                       # narrated API walkthrough
└── tests/                            # unit + integration + test_e2e.py
```

---

## Run

```bash
# Build and bring up (redis + app); waits for /health to report ok
make build
make up

# Open the dashboard in a browser
make ui                # or: open http://localhost:8080/

# Push 5 000 sample logs, then search
curl -X POST http://localhost:8080/api/generate-sample \
     -H 'Content-Type: application/json' \
     -d '{"count": 5000}'
curl "http://localhost:8080/api/search?q=error&limit=10"

# Tear everything down
make down
make clean             # also prunes volumes + images
```

---

## Test

Every target below runs **inside Docker**. Nothing executes on the host Python interpreter.

```bash
make test              # unit + integration suite (excludes test_e2e.py)
make e2e               # live compose stack + tests/test_e2e.py
make load              # latency + throughput assertions
make demo              # narrated HTTP walkthrough
make logs              # tail the app container logs
```

- `make test` builds the `test` image if needed and runs pytest against the in-process ASGI app via `httpx.ASGITransport` — no sockets, no Redis required for most tests (consumer tests use `fakeredis`).
- `make e2e` brings up the full compose stack (real Redis + real FastAPI app + volume) and runs `tests/test_e2e.py` against the live service, then tears everything down on both success and failure.
- `make load` runs `scripts/load_test.py` against the live app and exits non-zero if any success criterion below regresses.

---

## Performance targets (verified via `make load`)

| Metric            | Target         | How it's measured                                        |
|-------------------|----------------|----------------------------------------------------------|
| Search p95        | < 50 ms        | 20 qps background load while ingesting 6 000 docs        |
| Indexing latency  | < 100 ms       | Drain delay after last XADD divided by batch size (<=)   |
| Throughput        | >= 1 000 logs/s | `/api/generate-sample` pipelined XADDs at 1 500 l/s target |
| Survives restart  | Yes             | Segments flushed to `/data/segments` (docker volume)      |

All thresholds are asserted inside `scripts/load_test.py` — the script prints a `RESULTS` block followed by `PASS` or `FAIL` and exits 0 / 1 so CI can gate on it.

Observed numbers on a 2024-era laptop (compose default cpu/mem):

```
throughput            : ~2 000 logs/s
search p50            : ~3 ms
search p95            : ~8 ms
index per-doc (<=)    : ~5 ms
```

---

## Status

**Complete.** Every commit in the plan has landed:

| # | Commit                                                              |
|---|---------------------------------------------------------------------|
| 1 | Scaffold: config, models, Dockerfile, compose, Makefile             |
| 2 | `LogTokenizer` with IP / email / URL compound handling              |
| 3 | In-memory `Segment` with three posting maps                         |
| 4 | Atomic JSONL+gzip segment persistence                               |
| 5 | `InvertedIndex` orchestrator with flush + disk fan-out              |
| 6 | Redis stream consumer with batching + exponential backoff           |
| 7 | FastAPI app with lifespan, `/health`, `/api/stats`                  |
| 8 | `/api/search` with service / level filters + highlighting           |
| 9 | `POST /api/generate-sample` ingest endpoint                         |
| 10| Dashboard template with search UI and filters                       |
| 11| WebSocket `/ws` with `new_document` + `stats_update` broadcasts     |
| 12| Background segment merger                                           |
| 13| Load test, demo script, e2e test, final polish                      |

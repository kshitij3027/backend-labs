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

## Project Structure (planned)

```
real-time-log-indexing/
├── README.md
├── requirements.txt
├── .gitignore
└── (code, Dockerfile, tests, dashboard — to be added in subsequent commits)
```

---

## Status

**Scaffold only.** This commit contains the README, `requirements.txt`, and `.gitignore` — no code, no Dockerfile, no tests yet. Implementation will land in follow-up commits once the plan is approved.

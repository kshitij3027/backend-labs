# log-sys-performance-profiler

A profiling and optimization system that instruments a log ingestion pipeline to capture resource metrics, detect bottlenecks, recommend optimizations, and validate improvements through load testing.

## Overview

This project provides an end-to-end performance engineering workflow for a log ingestion pipeline:

1. **Instrument** — wrap pipeline stages (parse, enrich, filter, sink) with a lightweight library that records latency, throughput, CPU, memory, and queue depth per stage.
2. **Profile** — collect time-series metrics during real or synthetic workloads, surface them through a web dashboard.
3. **Detect bottlenecks** — analyze metrics to flag the slowest stage, the most resource-hungry stage, and saturation points.
4. **Recommend optimizations** — suggest concrete fixes (e.g., increase worker pool, batch writes, change serializer) based on the detected bottleneck pattern.
5. **Validate** — re-run a load test against the optimized configuration and produce a before/after comparison so improvements are measurable, not assumed.

## How It Runs

- A long-lived **FastAPI server** on port `8000` exposes:
  - A REST API for starting/stopping load tests, listing runs, fetching metrics, and pulling recommendations.
  - A **web dashboard** (HTML + minimal JS) for browsing runs, watching live metrics, and comparing runs side by side.
- An **instrumentation library** that wraps each stage of the log pipeline. The pipeline itself runs in-process inside the server for simplicity, with stages connected by bounded async queues so back-pressure is observable.
- A **load generator** built into the server triggers synthetic log traffic at configurable rates and payload shapes, then captures the resulting metrics into a run record.

Typical flow:

```
start server → open dashboard → kick off load test → watch live metrics
   → read bottleneck + recommendations → apply config change → re-run
   → compare before/after on the dashboard
```

## Tech Stack

- **Language:** Python 3.11+
- **Web framework:** FastAPI + Uvicorn
- **Templating:** Jinja2 (server-rendered dashboard)
- **Metrics collection:** `psutil` for process/system resource counters, in-process timers for stage latency
- **Storage:** SQLite (runs, stage metrics, recommendations) — no external DB required
- **Load testing:** in-process async load generator (httpx for any outbound calls)
- **Testing:** pytest, pytest-asyncio

## How to Run

> _To be filled in once implementation begins._

Planned entry point:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Dashboard will be available at `http://localhost:8000/`.

## API Surface (planned)

| Method | Path                              | Purpose                                    |
| ------ | --------------------------------- | ------------------------------------------ |
| POST   | `/api/runs`                       | Start a new load-test run                  |
| GET    | `/api/runs`                       | List historical runs                       |
| GET    | `/api/runs/{run_id}`              | Run summary + stage metrics                |
| GET    | `/api/runs/{run_id}/bottlenecks`  | Detected bottleneck stages                 |
| GET    | `/api/runs/{run_id}/recommendations` | Optimization recommendations            |
| GET    | `/api/runs/{run_id}/live`         | Server-Sent Events stream of live metrics  |
| GET    | `/api/compare?a={run_id}&b={run_id}` | Side-by-side comparison of two runs     |
| GET    | `/`                               | Dashboard (HTML)                           |

## Pipeline Stages (planned)

1. **Ingest** — accept log records from the load generator
2. **Parse** — structured-field extraction
3. **Enrich** — add derived fields (timestamps, host metadata)
4. **Filter** — drop or sample records by rule
5. **Sink** — write to an in-memory/SQLite store

Each stage runs as an async worker reading from a bounded queue, so queue depth is itself a profiled signal.

## What I Learned

> _To be filled in as the project evolves._

## Status

Scaffolded — implementation has **not** started. Only `README.md`, `requirements.txt`, and `.gitignore` exist at this point.

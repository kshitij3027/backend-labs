# Storage Format Optimizer

An **adaptive storage engine** that ingests log entries, **learns from query access
patterns**, and **automatically routes and migrates data** between row, columnar, and hybrid
storage formats — minimizing both query latency and on-disk size for whatever shape of
traffic it actually sees.

**Status: scaffold only.** This folder currently contains just the project scaffold
(`README.md`, `requirements.txt`, `.gitignore`). **No implementation exists yet** — there is
no `src/`, no API server, no tests, and no Docker setup. The sections below describe the
*intended* design; treat every command and endpoint as planned, not working.

---

## Overview

The engine is designed to run as a **long-lived server process** exposing an HTTP API plus a
real-time web dashboard. The intended responsibilities are:

- **Ingest** — accept batches of log entries over an HTTP endpoint and write them into
  partitions.
- **Query** — answer point lookups, full-record reads, and analytical scans/aggregations over
  the ingested data via the API.
- **Learn access patterns** — observe which fields and queries are *hot* (frequency, recency,
  query shape) and which partitions are scanned analytically vs. read whole.
- **Migrate in the background** — a background engine continuously re-evaluates each partition
  and rewrites it into the storage format that best fits its observed access pattern.
- **Observe** — a live dashboard (WebSocket-driven) and a `GET /api/stats` endpoint expose the
  current format distribution, migration activity, and query-latency metrics.

---

## The Core Idea — Row vs. Columnar vs. Hybrid

No single physical layout is best for all access patterns. The optimizer's whole job is to put
each partition in the layout that matches how it's actually being read.

- **Row format** — best for **write-heavy** workloads, **point lookups**, and
  **full-record reads**. Records are stored contiguously, so fetching or appending a whole
  entry is cheap. This is the natural home for **hot / recent** data that's still being written
  and read back in full.
- **Columnar format** — best for **analytical scans** and **aggregations over a few columns**,
  and it **compresses far better** because each column holds homogeneous values. This suits
  **cold / analytical** data that is mostly scanned over a handful of fields rather than read
  record-by-record.
- **Hybrid** — a deliberate mix of the two for partitions whose access pattern is mixed (e.g.
  warm data that still takes occasional point lookups but is increasingly scanned analytically).

The **optimizer observes** the live query stream — how often each partition is touched, how
recently, and the *shape* of the queries (point lookup vs. wide scan, which columns) — and
**migrates partitions** toward the format that minimizes **query latency + storage size** for
that observed pattern. Recent, write-heavy, point-looked-up data trends toward row; aging,
scan-heavy data trends toward columnar; mixed data lands on hybrid.

---

## Architecture (planned)

```
                         ┌─────────────────────┐
   client ─────────────► │   FastAPI server    │  /api/ingest, /api/query, /api/stats
                         └──────────┬──────────┘  /ws (live dashboard)   ◄──── dashboard (WS)
                                    │
            ┌───────────────────────┼───────────────────────────────┐
            ▼                       ▼                                 ▼
   ┌─────────────────┐    ┌───────────────────┐            ┌────────────────────┐
   │ Access-pattern  │    │  Pluggable storage │            │ Background migration│
   │ tracker         │◄───┤  formats           │◄──────────►│ engine              │
   │ (freq·recency·  │    │  row · columnar ·  │  rewrites  │ (re-evaluates each  │
   │  query shape)   │    │  hybrid            │  partitions│  partition's layout)│
   └─────────────────┘    └───────────────────┘            └────────────────────┘
            │                                                         │
            └──────────────────────► metrics ◄────────────────────────┘
                                        │
                                        ▼
                          live dashboard via WebSocket + vendored Chart.js
```

Intended components:

- **HTTP API server** — a FastAPI app that handles ingest, query, and stats, and serves the
  dashboard. Long-lived process listening on port `8000`.
- **Pluggable storage formats** — a common interface with row, columnar, and hybrid
  implementations, so a partition can be written/read through whichever layout it currently uses.
- **Access-pattern tracker** — records per-partition / per-field access (frequency, recency,
  query shape) to score how each partition is being used.
- **Background migration engine** — periodically re-evaluates partitions against their tracked
  access pattern and rewrites the ones whose ideal format has changed.
- **Real-time dashboard** — a WebSocket endpoint pushes live metrics to a browser page that
  renders them with a **vendored Chart.js** (no extra Python dependency for charting).

---

## Tech Stack

- **Language:** Python 3.11+
- **Web / API framework:** FastAPI
- **ASGI server:** Uvicorn (`uvicorn[standard]` — bundles WebSocket support and friends)
- **Real-time dashboard:** FastAPI-native **WebSocket** on the backend; the frontend uses a
  **vendored Chart.js** (served as a static asset — no Python dependency for charting)
- **Columnar storage:** Apache Arrow / Parquet via **PyArrow**
- **Row storage:** append-only **JSONL** and/or the stdlib **`sqlite3`** module (no extra
  dependency)
- **Data models / validation:** **Pydantic v2** (bundled with FastAPI)
- **Testing:** **pytest** + **httpx** (FastAPI `TestClient` and an E2E client)

---

## How to Run

> **Note:** the implementation is **not built yet** — this is currently a scaffold
> (`README.md` + `requirements.txt` + `.gitignore` only). The commands below document the
> *intended* run flow and will not work until the engine is implemented. There is no Docker
> setup yet.

```bash
# (planned) install dependencies
pip install -r requirements.txt

# (planned) start the long-lived server
python src/main.py
```

Once implemented, the server is intended to:

- serve on **http://localhost:8000**
- serve the **live dashboard** at `/`
- expose engine stats at `GET /api/stats`

Ingestion and queries happen via API calls; the background migration runs on its own inside the
server process.

---

## API (planned)

Base URL: `http://localhost:8000`. All endpoints below are **planned**, not yet implemented.

### REST

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/` | Live monitoring dashboard (HTML + vendored Chart.js). |
| `POST` | `/api/ingest` | Ingest a batch of log entries into the engine. |
| `GET` / `POST` | `/api/query` | Run a query (point lookup, full-record read, or analytical scan/aggregation) against the stored data. |
| `GET`  | `/api/stats` | Engine stats: current **format distribution** (row / columnar / hybrid), **migration activity**, and **query latencies**. |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws` | Pushes live engine metrics (format distribution, migration activity, query latencies) to the dashboard. |

---

## Project Structure (planned)

> All paths below are **TBD** — only `README.md`, `requirements.txt`, and `.gitignore` exist
> today.

```
storage-format-optimizer/
├── README.md            # this file
├── requirements.txt     # runtime + test dependencies
├── .gitignore
└── src/
    └── main.py          # (TBD) server entrypoint — `python src/main.py`
    # storage formats, access-pattern tracker, migration engine,
    # dashboard assets, and tests — all TBD
```

---

## What I Learned

<!-- TBD — to be filled in as the project is implemented (row vs. columnar vs. hybrid
     trade-offs, Arrow/Parquet vs. JSONL/sqlite in practice, access-pattern scoring,
     background migration, live WebSocket metrics). -->

---

## Status

Scaffold only — implementation has **not** started. This folder contains just `README.md`,
`requirements.txt`, and `.gitignore`.

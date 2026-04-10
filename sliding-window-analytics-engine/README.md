# Sliding Window Analytics Engine

A memory-efficient sliding window system that computes real-time moving averages and statistics over streaming log/metric events using ring buffers and incremental computation.

## Overview

This service ingests high-volume metric events through a REST API, maintains fixed-size sliding windows in memory using ring buffers, and computes statistical aggregates (mean, min, max, stddev, percentiles, count) incrementally as new events arrive and old events expire. Results are streamed to a live dashboard over WebSockets, enabling sub-second visibility into streaming data without the memory or CPU cost of re-scanning the full window on every update.

The core idea: instead of storing every event forever or recomputing aggregates from scratch, use a ring buffer sized to the window, and update running statistics in O(1) as events flow in and out of the window.

## Tech Stack

- **Language**: Python 3.11+
- **Framework**: FastAPI (async REST API + WebSocket support)
- **Server**: Uvicorn (ASGI)
- **Cache/State**: Redis (for cross-process state, persistence, and multi-window coordination)
- **Data Validation**: Pydantic v2
- **Numerics**: NumPy (for percentile calculations)
- **Dashboard**: HTML + vanilla JS + Chart.js (served by FastAPI)
- **Containerization**: Docker + Docker Compose

## How It Runs

This is a **long-lived server process** containerized via Docker Compose:

1. **FastAPI app container** — Exposes REST endpoints for ingestion, WebSocket endpoint for dashboard streaming, and serves the static dashboard.
2. **Redis container** — Stores window snapshots, handles pub/sub for broadcasting updates, and persists state across restarts.
3. **Docker Compose** — Orchestrates both containers on a shared network.

The server runs continuously, accepting metric events over HTTP and pushing live statistics to any connected WebSocket client.

## Features (Planned)

- **Ring-buffer-backed sliding windows** — Fixed memory footprint regardless of event rate.
- **Incremental computation** — O(1) updates for count, sum, mean; efficient stddev via Welford's algorithm.
- **Multiple concurrent windows** — Track different metrics and window sizes simultaneously (e.g., 1-minute CPU, 5-minute latency).
- **Time-based and count-based windows** — Choose between "last N events" and "last N seconds".
- **Percentile tracking** — p50, p90, p95, p99 over the window.
- **REST ingestion API** — Single-event and batch endpoints.
- **WebSocket dashboard** — Real-time chart updates as statistics change.
- **Redis persistence** — Window state survives restarts.
- **Metric tagging** — Group and filter events by labels (e.g., `service=api`, `host=web-1`).

## API (Planned)

### REST Endpoints
- `POST /events` — Ingest a single metric event.
- `POST /events/batch` — Ingest a batch of metric events.
- `GET /windows` — List all active windows and their current statistics.
- `GET /windows/{window_id}` — Get current statistics for a specific window.
- `POST /windows` — Create a new sliding window (specify size, type, metric).
- `DELETE /windows/{window_id}` — Remove a window.
- `GET /health` — Liveness/readiness probe.

### WebSocket Endpoint
- `WS /ws/stream` — Subscribe to live window statistic updates.

### Dashboard
- `GET /` — Real-time dashboard UI showing live charts of all active windows.

## How to Run

<!-- Will be filled in once the project is built -->

## What I Learned

<!-- Will be filled in as the project evolves -->

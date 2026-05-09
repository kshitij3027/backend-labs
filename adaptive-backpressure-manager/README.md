# Adaptive Backpressure Manager

A flow-control system that detects multi-dimensional pressure (queue depth, processing lag, resource utilization) and gracefully manages overload through adaptive throttling and priority-aware message dropping.

## Problem

When a service is hit harder than it can process, naive systems collapse: queues grow without bound, latency spikes, memory fills, and eventually the process is OOM-killed or starts dropping requests at random. The "load shedding" decision is usually too late, too coarse, or too late *and* too coarse.

A robust ingestion pipeline needs to:
- **See pressure early**, before queues explode — by watching multiple dimensions, not just queue length.
- **React proportionally** — slow down the *firehose*, not the whole system.
- **Shed work intelligently** — drop low-priority traffic first so critical work still gets through.
- **Recover automatically** — once pressure subsides, lift the throttle and resume full throughput.

This project is a self-contained implementation of that control loop, exposed as a long-lived HTTP service with built-in load-testing endpoints and a live dashboard.

## What It Does

- **Multi-dimensional pressure sensor.** A background async task samples queue depth, processing lag (oldest message age), CPU utilization, and memory utilization on a fixed cadence, then fuses them into a single normalized **pressure score** (0.0 = idle, 1.0 = critical).
- **Adaptive admission control.** Based on the current pressure score, the ingestion endpoint applies a probabilistic accept/reject decision per message, biased by the message's declared priority (e.g. `critical`, `high`, `normal`, `low`).
- **Priority-aware dropping.** Under high pressure, `low` priority traffic is dropped first; `critical` traffic is shed only as a last resort. Each dropped message is counted by reason and by priority.
- **Throttling vs. dropping.** Two distinct overload responses: producers can be asked to slow down (HTTP 429 + `Retry-After`) *or* messages can be silently dropped at the boundary — the system picks based on backlog vs. arrival rate.
- **Hysteresis and smoothing.** The pressure signal is smoothed (EWMA) and the throttle state machine has hysteresis bands, so the system doesn't oscillate between "fine" and "shedding" on every sample.
- **Live dashboard.** Optional Plotly/Dash UI that shows pressure score, queue depth, drop counters by priority, current throttle state, and a real-time throughput chart.
- **Built-in load generator.** Endpoints to start/stop synthetic traffic at configurable rates and priority mixes, so you can drive the system into overload and watch it recover — without needing a separate client.

## How It Runs

A single long-lived FastAPI process. On startup it spawns:

1. The **HTTP server** — log ingestion, status, control, and load-test endpoints.
2. A **background processor task** — pulls from the internal queue and simulates work with configurable per-message latency.
3. A **background monitor task** — samples pressure dimensions every N milliseconds and updates the shared `PressureState`.
4. An optional **dashboard task** — serves the real-time UI on a separate port.

The processor is intentionally rate-limited (configurable) so a high enough ingestion rate will *always* eventually produce backpressure — that's the point of the demo.

### Endpoint Sketch

| Method | Path                  | Purpose                                                       |
|--------|-----------------------|---------------------------------------------------------------|
| POST   | `/ingest`             | Submit a single log message with priority; may be 202/429/dropped |
| POST   | `/ingest/batch`       | Submit a batch; admission is per-message                      |
| GET    | `/status`             | Current pressure score, queue depth, throttle state, counters |
| GET    | `/metrics`            | Prometheus-style metrics                                      |
| POST   | `/loadtest/start`     | Start internal load generator (rate, duration, priority mix)  |
| POST   | `/loadtest/stop`      | Stop any running load generator                               |
| GET    | `/loadtest/status`    | Current generator state and emitted/accepted counts           |
| POST   | `/admin/config`       | Tune thresholds, weights, processor rate at runtime           |
| GET    | `/dashboard`          | Real-time visualization (separate Dash app)                   |

Exact shapes will be locked in during the planning step.

## Tech Stack

- **Language:** Python 3.11+
- **Web framework:** FastAPI + Uvicorn (async)
- **Concurrency model:** `asyncio` with bounded queues
- **Metrics:** `prometheus-client` for `/metrics`
- **Resource sampling:** `psutil` for CPU / memory
- **Dashboard (optional):** Plotly + Dash
- **Validation:** Pydantic v2
- **Testing:** pytest, pytest-asyncio, httpx for the test client

## How to Run

_To be filled in once implementation begins._

## What I Learned

_To be filled in as the project evolves._

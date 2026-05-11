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

Everything runs in Docker — no Python venv required on the host.

### Quickstart

```bash
make build      # build app, dashboard, and test images
make run        # start app (:8000) and dashboard (:8050) in the background
```

- **API:** `http://localhost:8000`
- **Dashboard:** `http://localhost:8050`
- **Stop:** `make stop`
- **Tail logs:** `make logs`
- **Clean up images:** `make clean`

### Running the test suites

```bash
make test       # unit tests inside the test container
make e2e        # spins up the app, runs scripts/verify_e2e.py, tears down
```

The e2e target drives the system through PRESSURE → OVERLOAD → RECOVERY → NORMAL in about 35 seconds. To run the slow pytest invariant suite as well:

```bash
docker compose up -d app
docker compose --profile test run --rm -e ABPM_BASE_URL=http://app:8000 test pytest -m e2e -v
docker compose down
```

### Driving a load test by hand

```bash
# 30-second smoke at 200 RPS, then watch the status endpoint
curl -X POST http://localhost:8000/api/v1/loadtest/start \
  -H 'Content-Type: application/json' \
  -d '{"profile":"smoke","rps":200,"duration_seconds":30}'

watch -n 1 'curl -fsS http://localhost:8000/api/v1/system/status | jq .'
```

The dashboard's **10× SPIKE** button does the same thing visually and is the fastest way to see the four-state transition.

### Endpoint cheat-sheet

| Method | Path | Purpose |
|---|---|---|
| GET | `/system/health` | Liveness (used by the docker healthcheck) |
| GET | `/api/v1/system/health` | Same, namespaced |
| GET | `/api/v1/system/status` | Pressure level, throttle rate, queue size, score |
| POST | `/api/v1/ingest` | Submit a log message with priority — 202/429/204/503 by admission verdict |
| POST | `/api/v1/loadtest/start` | Start the in-process load generator (smoke/ramp/spike/soak/recovery/full) |
| POST | `/api/v1/loadtest/stop` | Stop the load generator |
| GET | `/api/v1/loadtest/status` | Emitted/accepted/throttled/dropped/rejected counters |
| POST | `/api/v1/admin/config` | Live tuning (thresholds, EWMA alpha, AIMD beta, etc.) |
| GET | `/api/v1/metrics/json` | JSON snapshot for the dashboard |
| GET | `/metrics` | Prometheus text exposition |

## What I Learned

- **Multi-dimensional pressure beats any single signal.** Queue depth alone is a lagging indicator; CPU alone is jumpy; lag alone is undefined when the queue is empty. Fusing all three under `max(weighted_sum, peak)` and EWMA-smoothing it (α = 0.3) gives the controller something stable to act on at the edges without lagging when one dimension actually spikes.
- **Hysteresis + minimum dwell beat thresholds alone.** Even with a >=0.1 gap between up- and down-thresholds, the controller flapped near boundary scores until I added a 3-second minimum dwell per state. The dwell guard is what makes the OVERLOAD → RECOVERY → NORMAL path stable.
- **Drops belong at admission, not in the worker.** Once a message is dequeued, the work cost is already sunk. Per-state drop policies live in `Admission.decide()` — workers honor deadlines but never silently drop.
- **AIMD recovery needs a slow-start clamp.** Letting the AIMD limit recover at +1/3s without clamping after OVERLOAD lets a second wave of work flood in before the queue has fully drained. Halving `prev_limit` on the OVERLOAD → RECOVERY edge cleanly prevents that "second wave" overload.
- **Live tuning is essential for E2E testing.** The verifier shrinks `max_queue_size` and bumps `processing_latency_seconds` via `/admin/config` so the spike actually produces backpressure within a 30-second test window — without runtime tuning the test would have to run for minutes against the default settings.

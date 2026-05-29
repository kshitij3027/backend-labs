# Adaptive Batching Engine Optimizer

A real-time control-loop system that automatically tunes log-processing **batch size**
using **gradient ascent** to maximize throughput while respecting resource constraints
(CPU, memory, and latency budgets).

The engine runs a long-lived FastAPI server with an async optimization loop that, on
every tick, observes throughput/latency, climbs a noisy multi-objective utility with
respect to batch size, and backs off whenever resource limits are threatened. A vanilla
HTML + Chart.js dashboard visualizes the optimizer's behavior live over a WebSocket
stream.

**Status: complete.** Four required components, four operating states, REST + WebSocket
API, and a live dashboard are implemented and verified in Docker (162 tests; 57.8%
throughput improvement over static batching).

---

## Why this exists

Batch size is one of the highest-leverage knobs in any log/stream processing pipeline,
and it has a non-obvious sweet spot:

- **Batches too small** → per-batch overhead (syscalls, network round-trips, framing,
  serialization) dominates → low throughput.
- **Batches too large** → memory pressure, GC pauses, head-of-line latency, and
  occasional limit breaches → throughput collapses and tail latency spikes.

Throughput as a function of batch size `T(B)` is therefore roughly **concave** with a
maximum somewhere in the middle. Hand-tuning that knob is brittle: the optimum drifts
with load, payload size, and host conditions. This project treats it as an **online
optimization problem** and lets a control loop find and track the optimum automatically.

---

## How it works (gradient-ascent control loop)

The batch size `B` is the control variable. The objective is a scalar **multi-objective
utility** `U(B)` that trades raw throughput against per-batch latency:

```
U(B) = w_t · (throughput / throughput_scale) + w_l · (1 / (1 + latency_ms / latency_scale))
```

with default weights `w_t = 0.7`, `w_l = 0.3`. The first term rewards throughput; the
second is a diminishing-returns reward for low latency. Because only this scalar utility
is *observed* (not its analytic derivative), the engine hill-climbs with **direction
memory**:

On each tick of the async loop (every `optimization_interval` seconds):

1. **Observe** — process the batch at the *current* `B` through a concave cost model and
   read throughput, latency, plus blended CPU%/memory% (see "Resource blend" below).
2. **Estimate the slope** — a one-step finite difference `dU/dB` from the previous move.
   If the last move *raised* utility, keep heading the same way; if it *lowered* utility,
   flip direction. (`dU/dB` is surfaced as `last_gradient`.)
3. **Step (multiplicative)** — the raw target is `B · increase_factor` when climbing and
   `B · decrease_factor` when backing off, so steps are scale-free from `B=50` to `5000`.
4. **Smooth** — exponential smoothing blends the raw target with the current size,
   `new = B·(1 − α) + optimal·α`, so a single noisy sample cannot jerk the batch around
   (damps oscillation near the peak).
5. **Project (clamp)** — the smoothed value is clamped to `[min_batch_size, max_batch_size]`
   and rounded to an int.
6. **Constrain** — if CPU%, memory%, or latency breach their hard thresholds, the state
   machine forces **EMERGENCY** and the batch is slashed (halved, floored at the min)
   instead of stepping the optimizer, so it never chases throughput past a resource cliff.
7. **Publish** — push the fresh optimizer state + metrics to dashboard clients over
   WebSocket.

The optimizer step is **pure, synchronous, O(1)** — a fixed handful of float ops with no
hot-path allocation, comfortably under the 10 ms budget.

### Operating states

An explicit state machine governs *how* each tick chooses the next batch size:

```
LEARNING --(N baseline samples)--> OPTIMIZING --(settled)--> STABLE
STABLE --(drift)--> OPTIMIZING
any --(cpu/mem>90% or latency>1000ms)--> EMERGENCY
EMERGENCY --(recovery hysteresis)--> OPTIMIZING
```

- **LEARNING** — gather baseline `(B, throughput)` samples, then start climbing.
- **OPTIMIZING** — active gradient ascent.
- **STABLE** — recent batch sizes have settled into a tight band; hold and stop probing.
- **EMERGENCY** — a hard limit was breached; halve the batch and hold. Leaving EMERGENCY
  requires **hysteresis**: several consecutive *healthy* cycles below lower recovery
  thresholds (e.g. CPU 70% vs. the 90% breach line), creating a dead band so the system
  can't flap. On recovery it resumes in OPTIMIZING and re-climbs from the safe batch.

### Resource blend (demonstrating EMERGENCY without stressing the host)

The loop must drive itself into EMERGENCY *reproducibly* and *without ever loading the
real machine*, while still genuinely exercising `psutil`. CPU%/memory% are therefore a
blend: **simulated workload pressure dominates** (it climbs with batch size / arrival
rate) and only ~15% of the live psutil reading is mixed in, so emergencies fire on large
batches / traffic bursts rather than incidental host noise. `memory_available_mb` is
reported verbatim from psutil, so the dashboard shows a real number.

---

## Architecture / components

The spec mandates four separable components; all four are present (plus supporting
collaborators), wired together by `AdaptiveBatcher.tick()` — one synchronous control-loop
iteration:

| Component | Source | Responsibility |
|-----------|--------|----------------|
| **MetricsCollector** (required) | `src/metrics.py` | Rolling, bounded `deque` time-series of snapshots (O(1) writes); chartable parallel series. Paired with **ResourceMonitor** (non-blocking `psutil` CPU/mem sampler). |
| **OptimizationEngine** (required) | `src/optimizer.py` | Gradient-ascent hill-climber: finite-difference slope + direction memory, multiplicative probe, exponential smoothing, feasible-region clamp, multi-objective utility. |
| **AdaptiveBatcher** (required) | `src/batcher.py` | The control loop: pulls load, processes the batch, blends resources, evaluates constraints + recovery, advances the state machine, chooses the next batch size. |
| **Dashboard** (required) | `dashboard/` | Live vanilla-HTML + Chart.js UI fed by `/ws/metrics`. |
| BatchProcessor | `src/processor.py` | Concave cost model: given a batch size + arrival rate, returns throughput, latency, and *simulated* CPU/memory pressure. |
| LoadSimulator | `src/loadsim.py` | Synthetic arrivals with configurable rate + burst probability. |
| ConstraintHandler | `src/constraints.py` | Hard safety limits + emergency reduction + recovery hysteresis. |
| StateMachine | `src/states.py` | Explicit LEARNING / OPTIMIZING / STABLE / EMERGENCY transitions. |
| API + loop | `src/main.py`, `src/api/`, `src/websocket.py` | FastAPI routers, `/health`, the background async loop, and the WebSocket fan-out. |

---

## Tech stack

- **Language:** Python 3.12
- **Backend:** FastAPI 0.115 (async) on Uvicorn 0.34 (ASGI)
- **Numerics:** NumPy 2.2
- **System metrics:** psutil 6.1
- **Models / config:** Pydantic v2 (2.10) + pydantic-settings
- **Live updates:** WebSockets 14
- **Dashboard:** vanilla HTML + Jinja2 templates + vendored Chart.js 4 (no CDN, no build step)
- **Testing:** pytest 8.3, pytest-asyncio, httpx
- **Infra:** Docker + Docker Compose (`python:3.12-slim`)

---

## Quick start (Docker)

```bash
make build        # build app + tester images
make up           # start the app at http://localhost:8000
open http://localhost:8000/    # live dashboard
make logs         # tail app logs
make test         # full unit+integration+e2e suite (in Docker)
make e2e          # live traffic-pattern probe (steady→burst→emergency→recovery)
make improvement  # adaptive-vs-static throughput demo (asserts >=30%)
make down         # stop
```

All tests run **inside Docker** via the `tester` profile — never on the host.
(`make test-unit` / `make test-int` / `make test-e2e` run the suites individually;
`make clean` tears the stack down and prunes caches.)

The Compose file sets `OPTIMIZATION_INTERVAL=1.0` for a livelier demo (the in-code
default is `5.0`).

---

## API

Base URL: `http://localhost:8000`.

### REST

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/` | Live dashboard (HTML + Chart.js). |
| `GET`  | `/health` | Liveness probe → `{"status":"healthy"}`. |
| `GET`  | `/api/metrics` | Latest snapshot + recent chartable series (last ~20) + current optimizer status. |
| `GET`  | `/api/optimizer` | Current `OptimizerStatus` (state, batch size, last gradient, alpha, bounds, constraint flag, reason). |
| `POST` | `/api/optimizer/config` | Partial `OptimizerConfigUpdate` — retune interval / alpha / bounds / probe factors / thresholds / objective weights **live** (no restart). |
| `POST` | `/api/optimizer/reset` | Reset every control-loop component to defaults. |
| `POST` | `/api/load` | `LoadConfig`: `messages_per_second`, `burst_probability`, `payload_size_bytes` — retarget the synthetic traffic. |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws/metrics` | Pushes the current state on connect, then broadcasts `{type, snapshot, status, series}` on every loop tick. |

```bash
# Drive a burst of load
curl -X POST http://localhost:8000/api/load \
  -H "Content-Type: application/json" \
  -d '{"messages_per_second": 1000, "burst_probability": 0.3, "payload_size_bytes": 512}'

# Read the current snapshot + optimizer status
curl http://localhost:8000/api/metrics

# Retune the loop live (faster cadence, more responsive smoothing)
curl -X POST http://localhost:8000/api/optimizer/config \
  -H "Content-Type: application/json" \
  -d '{"optimization_interval": 0.5, "smoothing_alpha": 0.3}'
```

---

## Dashboard

Open `http://localhost:8000/`. The page is fed entirely by the `/ws/metrics` WebSocket
and shows:

- **Status badges** — connection health, the operating-state badge
  (LEARNING / OPTIMIZING / STABLE / EMERGENCY), and a constraint indicator.
- **Live metric cards** — batch size, throughput (rec/s), latency (ms), CPU %, memory %,
  queue depth, last gradient.
- **Two live line charts** — throughput + batch size, and CPU/memory utilisation —
  over the last ~20 points.
- **Auto-reconnect** — the client re-establishes the WebSocket on drop.

---

## Configuration (env vars)

All fields are overridable via env var (case-insensitive) or a `.env` file
(see `.env.example`); defaults live in `src/settings.py`.

| Variable | Default | Notes |
|----------|---------|-------|
| `MIN_BATCH_SIZE` | `50` | Lower clamp bound. |
| `MAX_BATCH_SIZE` | `5000` | Upper clamp bound. |
| `INITIAL_BATCH_SIZE` | `100` | Starting batch size / reset seed. |
| `SMOOTHING_ALPHA` | `0.2` | Exponential smoothing factor (higher = more responsive). |
| `OPTIMIZATION_INTERVAL` | `5.0` | Seconds between loop ticks (Compose overrides to `1.0`). |
| `BATCH_INCREASE_FACTOR` | `1.1` | Multiplier when probing larger batches. |
| `BATCH_DECREASE_FACTOR` | `0.9` | Multiplier when backing off. |
| `CPU_CONSTRAINT_THRESHOLD` | `90` | CPU % breach → EMERGENCY. |
| `MEMORY_CONSTRAINT_THRESHOLD` | `90` | Memory % breach → EMERGENCY. |
| `LATENCY_CONSTRAINT_THRESHOLD` | `1000` | Latency (ms) breach → EMERGENCY. |
| `WEIGHT_THROUGHPUT` | `0.7` | Throughput weight in the utility. |
| `WEIGHT_LATENCY` | `0.3` | Latency-benefit weight in the utility. |
| `DEFAULT_MESSAGES_PER_SECOND` | `100` | Load-simulator default rate. |
| `DEFAULT_BURST_PROBABILITY` | `0.2` | Load-simulator default burst probability. |
| `DASHBOARD_POINTS` | `20` | Trailing data points charted. |

(Additional internals — `LEARNING_SAMPLES`, recovery thresholds/cycles,
`METRICS_HISTORY_SIZE`, `API_HOST`/`API_PORT`/`LOG_LEVEL` — are also configurable; see
`src/settings.py`.)

---

## Results / success criteria

Verified in Docker (`make test`, `make e2e`, `make improvement`):

| Success criterion | Result |
|-------------------|--------|
| 30–70% throughput increase over static batching | **57.8%** — adaptive converges to batch ≈509 → ~15,650 rec/s vs. static batch=100 → ~9,921 rec/s. (Analytic optimum `B*≈790`, pulled left by the latency objective.) |
| Adaptation to load changes within ~5 s | Loop adapts within a few ticks (Compose runs at 1 s/tick). |
| Stable utilisation; no oscillation | Smoothing + STABLE hold → no oscillation in steady state. |
| Zero constraint violations in normal operation | Zero violations observed in steady state. |
| Emergency reduction + gradual re-optimization | Verified live and in tests: batch slashed on breach, re-climbs after recovery hysteresis. |
| Optimization calculation < 10 ms | `OptimizationEngine.update` is O(1), well under budget. |
| Unit tests pass (gradient, constraints) | **137 unit** tests pass. |
| Integration tests pass (metrics → optimization → batching) | **19 integration** tests pass. |
| Four traffic patterns (steady, burst, constraint, recovery) | All four validated (`make e2e`, **6 e2e** tests). |
| **Total** | **162 tests pass** (137 unit + 19 integration + 6 e2e). |

---

## What I learned

- Online optimization of a **noisy, concave** objective: you can't trust a single sample,
  so a finite-difference slope **plus exponential smoothing** is what keeps the climb from
  oscillating around the peak.
- **Direction-memory hill-climbing** ("keep going while it helps, flip when it hurts") is
  a simple, robust way to track a moving optimum when you only observe a scalar utility,
  not its derivative — and multiplicative steps make it scale-free across a 50–5000 range.
- **Constrained back-off + hysteresis** matters: a hard breach must win immediately, but
  leaving the emergency state needs a dead band (lower recovery thresholds, sustained over
  several cycles) or the system flaps.
- Running a **background async control loop** alongside FastAPI: read the cadence from
  `app.state` each iteration so config changes retune it live, and never let one bad tick
  (or a dead client) kill the loop.
- **Streaming to a browser over WebSocket**: broadcast the whole dashboard payload in one
  envelope and push state on connect so a fresh client paints instantly.
- Demonstrating EMERGENCY **without stressing the host**: blend *simulated* workload
  pressure (dominant) with a small slice of real psutil so emergencies are reproducible
  yet psutil is genuinely exercised.

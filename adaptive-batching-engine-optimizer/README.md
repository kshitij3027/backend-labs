# Adaptive Batching Engine Optimizer

A real-time control-loop system that automatically tunes log-processing **batch size**
using **gradient ascent** to maximize throughput while respecting resource constraints
(CPU, memory, and latency budgets).

The engine runs a long-lived FastAPI server with an async optimization loop that
periodically observes throughput, estimates the local gradient of throughput with
respect to batch size, and nudges the batch size toward the optimum — backing off
whenever resource limits are threatened. A React dashboard visualizes the optimizer's
behavior live over a WebSocket stream.

---

## Why This Exists

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

## How It Works

### The control loop (gradient ascent)

The batch size `B` is the control variable. The objective is to maximize measured
throughput `T(B)` (records processed per second) subject to resource constraints.

On each tick of the async loop (every `interval` seconds):

1. **Observe** — read recent metrics: throughput, p95 latency, CPU%, memory% at the
   current batch size.
2. **Estimate the gradient** — approximate `dT/dB` numerically using finite differences
   over recent `(B, T)` observations (and/or a small deliberate probe step).
3. **Step** — update the batch size in the ascent direction:

   ```
   B ← B + η · (dT/dB)
   ```

   where `η` is the learning rate.
4. **Project onto the feasible region** — clamp `B` to `[B_min, B_max]` and apply a
   constraint penalty / back-off when CPU%, memory%, or latency exceed their thresholds,
   so the optimizer never chases throughput past a resource cliff.
5. **Publish** — push the new optimizer state and metrics to subscribers over WebSocket.

The result is a system that climbs toward peak throughput, settles near the optimum,
and re-adapts when the operating conditions change.

### Components (planned)

| Component | Responsibility |
|-----------|----------------|
| **Optimizer** | Gradient-ascent controller: gradient estimate, learning-rate step, constraint projection. |
| **Metrics collector** | Rolling time series of throughput, latency, CPU, memory, and batch size. |
| **Load simulator** | Generates synthetic log load (configurable arrival rate / payload size) to exercise the engine. |
| **Batch processor** | Simulated processing of a batch with a realistic cost model (fixed overhead + per-record cost + resource usage). |
| **API layer (FastAPI)** | REST endpoints + WebSocket stream + the background async optimization loop. |
| **Dashboard (React)** | Live charts: throughput vs. batch size, resource usage, and optimizer trajectory. |

---

## Tech Stack

- **Language:** Python 3.12
- **Backend framework:** FastAPI (async) on Uvicorn (ASGI)
- **Numerics:** NumPy (gradient estimation, smoothing)
- **Models / config:** Pydantic v2, pydantic-settings
- **Live updates:** WebSockets
- **Frontend:** React (Vite) with a charting library for live visualization
- **Testing:** pytest, pytest-asyncio, httpx

---

## Project Status

🚧 **Scaffold only.** This commit contains the project README, Python dependency
manifest, and `.gitignore`. Backend and frontend implementation has **not** started yet.

---

## How to Run

> _To be filled in as development progresses._

Planned shape:

```bash
# Backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload      # FastAPI server + async optimization loop

# Frontend
cd frontend
npm install
npm run dev                        # React dashboard
```

---

## API (planned)

### REST

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Liveness/readiness check. |
| `GET`  | `/api/metrics` | Current snapshot + recent rolling time series. |
| `GET`  | `/api/optimizer` | Current optimizer state (batch size, learning rate, last gradient, constraint status). |
| `POST` | `/api/load` | Start/update the load simulation (arrival rate, payload size). |
| `POST` | `/api/optimizer/config` | Tune learning rate, batch-size bounds, interval, and resource thresholds. |
| `POST` | `/api/optimizer/reset` | Reset optimizer state to defaults. |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws/metrics` | Live stream of metrics + optimizer state, pushed on every loop tick. |

---

## What I Learned

> _To be filled in as the project evolves._

Topics this project explores:

- Treating a system tuning knob as an **online convex/concave optimization** problem.
- **Gradient ascent** with noisy, real-world objective measurements (finite-difference
  gradients, smoothing, learning-rate selection).
- **Constrained optimization** via projection / back-off so the controller respects
  resource limits.
- Running a **background async control loop** alongside a FastAPI request/response API.
- Streaming live system state to a browser dashboard over **WebSockets**.

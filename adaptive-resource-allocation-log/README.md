# Adaptive Resource Allocation (Log Processing)

A system that monitors real-time cluster metrics, predicts near-future load, and
automatically scales processing resources (containers/workers) up or down to
maintain optimal performance. It runs as a long-lived server process exposing an
HTTP API and a live, browser-based dashboard.

## What It Does

- **Collects metrics** — a background loop continuously samples real host metrics
  via `psutil` (CPU, per-core, memory, load average) blended with a simulated
  log-processing workload (arrival rate, queue depth, throughput, latency,
  effective utilization) on a fixed interval.
- **Forecasts near-future load** — a Holt double-exponential-smoothing model
  (level + trend) projects effective utilization a configurable horizon ahead and
  attaches a `confidence` score derived from series stability and the predictor's
  recent track record.
- **Auto-scales reactively and predictively** — an orchestration loop combines
  reactive thresholds (CPU / memory / utilization) with the predictive forecast to
  scale the worker pool, applying **hysteresis** (separate up/down thresholds),
  **cooldowns** (scale-down held longer than scale-up), and **min/max bounds** to
  prevent thrashing.
- **Learns patterns and flags anomalies** — a z-score anomaly detector surfaces
  sudden utilization spikes (an optional scale-up trigger), and a time-of-day
  pattern learner accumulates a seasonality profile so the forecast is
  pre-positioned for the level a recurring hour is historically known to need.
- **Reports cost optimization** — the orchestrator integrates worker-seconds
  consumed and compares the adaptive pool against a hypothetical static system
  sized for the observed peak, reporting the percentage saved.
- **Streams live state** — current metrics, forecasts, scaling decisions, anomaly
  state, and cost are pushed to a browser dashboard in real time over WebSockets
  and rendered with Chart.js.

A **simulated worker pool is the default** so the whole system (and its E2E test)
runs hermetically with no container runtime; a real **Docker worker backend** is
opt-in via `USE_DOCKER=1`.

## Architecture

### Module map (`src/`)

| Module | Responsibility |
|---|---|
| `config.py` | Flat `Settings` dataclass + loader; precedence **defaults → YAML → env**. |
| `metrics.py` | `MetricCollector` (psutil + workload → canonical snapshot) and `RollingHistory` (time-ordered ring with per-field series extraction). |
| `forecast.py` | Pure-math Holt linear-trend forecast + `confidence` + `build_forecast` assembler. |
| `load_model.py` | Models incoming **demand** (arrival rate), supports ramping for load injection. |
| `workers.py` | `WorkerPool` interface + `SimulatedWorkerPool` (in-process queue sim) and opt-in `DockerWorkerPool` (manages real containers); `create_worker_pool` factory. |
| `scaler.py` | Stateless decision engine: reactive thresholds + predictive branch, hysteresis, asymmetric up/down, cooldowns → canonical decision dict. |
| `patterns.py` | `AnomalyDetector` (z-score spike detection) and `PatternLearner` (time-of-day seasonality factor). |
| `orchestrator.py` | Owns shared state + the two control loops; wires every component; produces the canonical `snapshot()` (incl. cost block). |
| `dashboard.py` | Flask + Flask-SocketIO app factory: HTTP API, SocketIO events, and the two background emitter loops. |
| `main.py` | Eventlet bootstrap (monkey-patch first), builds the object graph, runs the SocketIO server. |

### Control loops + WebSocket emitter

Two independent loops run alongside the web server (cadences are configured
separately), plus an emit-on-connect handler:

- **Collector loop** (`collector_tick`) — the fast, reactive loop. Advances the
  workload/capacity simulation by the elapsed interval, samples a canonical metric
  snapshot, appends it to the rolling history, feeds the pattern learner, and
  records forecast residuals. It is a reader/stepper — it never decides scaling.
- **Orchestration loop** (`orchestration_tick`) — the slower, deliberative loop.
  Builds the forecast (pre-positioned by the seasonality factor), runs anomaly
  detection, asks the scaler for a decision, and — if not a hold — scales the pool
  and logs the action.
- **WebSocket emitter** — the metrics loop broadcasts `metrics_update`
  (time-series for charts) and the orchestration loop broadcasts `status_update`
  (the full status payload); both are also emitted to a client on connect so the
  page paints immediately.

All shared state lives behind a single lock so the API/SocketIO layer always reads
a consistent snapshot. Under eventlet this is automatically a cooperative *green*
lock.

### Key design decision: real + simulated metrics

The system **blends real host metrics (`psutil`) with a simulated log-processing
workload** so that scaling behavior is observable and testable without needing a
real cluster under load: the simulated queue makes `effective_utilization` respond
to injected demand, which drives the autoscaler in a fully deterministic, hermetic
way. The **simulated worker pool is the default** (no Docker required for unit/E2E
tests); the **Docker backend is opt-in via `USE_DOCKER=1`** and manages real
worker containers. (Mounting the Docker socket is root-equivalent, so the Docker
backend is strictly opt-in.)

## How to Run

Everything runs in Docker — **no host Python is required**. Targets live in the
`Makefile`.

```bash
# Run the app live (detached): dashboard + API at http://localhost:8080
make up
make logs        # tail the app logs
make down        # stop and remove the stack

# Tests (all in Docker; the tester image is rebuilt first so a stale image
# can never mask a code change):
make test        # full pytest suite
make test-unit   # unit tests only
make test-int    # integration tests only

# End-to-end + load (each brings the app up, runs against it by service name,
# captures the verifier's exit code, then tears the stack down):
make e2e         # black-box verifier proving the load → autoscale causal chain
make load        # load test with pass/fail gates

make clean       # down + remove volumes and orphans
```

`make e2e` runs with a fast scale-down cooldown (`COOLDOWN_PERIOD_SECONDS=2`) so
the autoscaler reacts within the E2E window.

## API

The dashboard process exposes the following HTTP endpoints (all `/api/*` routes
return `503` JSON if the control plane is not yet wired):

| Method | Path | Body | Description |
|---|---|---|---|
| `GET` | `/health` | — | Liveness probe; always `200` independent of the orchestrator. |
| `GET` | `/api/status` | — | The canonical status snapshot (metrics, forecast, workers, last decision, cooldown, scaling history, anomaly, cost). |
| `GET` | `/api/metrics` | — | Current metrics plus the plotted time-series block. |
| `POST` | `/api/scaling` | `{"direction":"up"\|"down"}` or `{"target":N}` | Manual scale (bypasses thresholds/cooldown); returns the decision dict with `reason="manual"`. |
| `POST` | `/api/load` | `{"arrival_rate":N,"ramp_seconds":S}` | Load-injection test hook: ramps the simulated arrival rate toward the target (`ramp_seconds` default `10`). |

### Example

```bash
curl -s http://localhost:8080/api/status
```

```jsonc
{
  "timestamp": 1718600000.0,
  "current_metrics": {
    "cpu_percent": 31.4,
    "memory_percent": 58.2,
    "effective_utilization": 82.5,
    "queue_depth": 1200,
    "throughput": 1600.0,
    "latency_ms": 750.0,
    "arrival_rate": 1650.0,
    "workers": 5
  },
  "forecast": {
    "metric": "effective_utilization",
    "predicted": 91.3,
    "trend": "rising",
    "confidence": 0.78,
    "seasonality_factor": 1.05
  },
  "workers": { "current": 5, "min": 2, "max": 20, "backend": "simulated" },
  "last_decision": {
    "action": "scale_up",
    "reason": "predicted_utilization_high",
    "from_workers": 4,
    "to_workers": 5,
    "trigger_metric": "effective_utilization",
    "trigger_value": 91.3,
    "confidence": 0.78,
    "cooldown_active": false
  },
  "cooldown_remaining_s": 42,
  "scaling_history": [ /* recent pool-moving decisions */ ],
  "anomaly": { "active": false, "zscore": 1.2 },
  "cost": {
    "adaptive_worker_seconds": 1840.0,
    "static_worker_seconds": 3000.0,
    "peak_workers": 6,
    "savings_pct": 38.7
  }
}
```

(Field values above are illustrative; the shape matches the live payload.)

## WebSocket Events

Served over Socket.IO at the same origin (`http://localhost:8080`). Both events are
emitted to a client immediately on connect and then broadcast on a cadence:

| Event | When | Payload |
|---|---|---|
| `status_update` | on connect + every `orchestration_interval_seconds` | The full status payload (same object as `GET /api/status`). |
| `metrics_update` | on connect + every `monitoring_interval_seconds` | `current_metrics` + a `series` map (per-field time-series) + a separate `workers_series`, for the Chart.js charts. |

## Configuration

Configuration precedence is **dataclass defaults → `config/config.yaml` →
environment variables** (env wins). The YAML accepts both the documented nested
sections (`dashboard`, `scaling`, `monitoring`, `forecast`, `workload`) and flat
field names; each env var is the **UPPERCASE** field name.

| Setting (env var) | Default | Meaning |
|---|---|---|
| `CPU_THRESHOLD_SCALE_UP` / `_DOWN` | `75` / `40` | CPU % reactive scale-up / scale-down thresholds. |
| `MEMORY_THRESHOLD_SCALE_UP` / `_DOWN` | `80` / `50` | Memory % reactive scale-up / scale-down thresholds. |
| `UTIL_THRESHOLD_SCALE_UP` / `_DOWN` | `75` / `40` | Effective-utilization % scale-up / scale-down thresholds. |
| `MIN_WORKERS` / `MAX_WORKERS` | `2` / `20` | Worker-count bounds (all scaling is clamped here). |
| `COOLDOWN_PERIOD_SECONDS` | `60` | Cooldown after a scale-up action. |
| `SCALE_DOWN_COOLDOWN_SECONDS` | `120` | Longer cooldown after a scale-down (damps flapping). |
| `MONITORING_INTERVAL_SECONDS` | `5` | Collector / metrics-emit cadence. |
| `ORCHESTRATION_INTERVAL_SECONDS` | `5` | Decision / status-emit cadence. |
| `HISTORY_WINDOW_MINUTES` | `15` | Rolling history window fed to charts/forecast. |
| `METRICS_RETENTION_HOURS` | `24` | Max age of retained snapshots. |
| `FORECAST_ALPHA` | `0.25` | Holt level smoothing factor. |
| `FORECAST_BETA` | `0.10` | Holt trend smoothing factor. |
| `HORIZON_MINUTES` | `10` | How far ahead the forecast projects. |
| `CONFIDENCE_THRESHOLD` | `0.70` | Min forecast confidence for the predictive branch to act. |
| `BASE_ARRIVAL_RATE` | `500` | Baseline simulated demand (msgs/sec). |
| `CAPACITY_PER_WORKER` | `400` | Throughput a single worker handles (msgs/sec). |
| `USE_DOCKER` | unset | `1` selects the real Docker worker backend (default is the simulated pool). |

## What I Learned

- **Eventlet correctness is order-sensitive.** `eventlet.monkey_patch()` must be
  the very first thing in the process — ahead of every other import — and `psutil`
  must be sampled non-blocking (`interval=None`) so a sample inside a green thread
  never starves the eventlet hub.
- **Holt's method gives a cheap, honest forecast.** Double-exponential smoothing
  (level + trend) projects load a horizon ahead with almost no dependencies, and a
  `confidence` derived from the inverse coefficient of variation plus recent
  residuals means confidence is *earned from measured accuracy*, not assumed.
- **Anti-thrash is the hard part of autoscaling.** HPA-style asymmetric response —
  aggressive scale-up, one-worker-at-a-time conservative scale-down — combined with
  hysteresis (separate up/down thresholds) and cooldowns is what stops the pool
  oscillating around a single trip point.
- **Blending real and simulated metrics makes scaling testable.** Driving
  `effective_utilization` from a simulated queue (while still reading real CPU/mem)
  lets an injected load ramp deterministically exercise the whole control loop with
  no real cluster.
- **Vendoring frontend deps keeps E2E hermetic.** Bundling Chart.js / Socket.IO
  rather than pulling from a CDN means the Docker E2E flow has no network
  dependency and is fully reproducible.
- **A black-box E2E should prove a causal chain.** The verifier injects load and
  then asserts the autoscaler *reacts* (workers increase), validating the real
  end-to-end data flow rather than just unit-level behavior.

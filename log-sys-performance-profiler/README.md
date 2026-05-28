# log-sys-performance-profiler

A profiling and optimization system that instruments a 4-stage async log pipeline, captures CPU/memory/I/O/concurrency metrics per stage into an in-memory ring buffer, classifies bottlenecks into four categories (serial, resource, contention, architectural), generates rule-based recommendations, and validates improvements through synthetic load tests + a before/after benchmarking harness.

## Architecture

```
SyntheticLogGenerator
       │
       ▼
   ┌─────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────┐
   │  parse  │ ─▶ │  validate   │ ─▶ │  transform   │ ─▶ │  write  │
   └─────────┘    └─────────────┘    └──────────────┘    └─────────┘
       │  bounded asyncio.Queue between stages — queue depth is itself profiled
       ▼
   @profile_stage decorator → MetricsCollector (asyncio.Queue + batched flush)
       │
       ▼
   ┌──────────────────┐   ResourceSampler (psutil, fixed 0.5s interval)
   │   RingBuffer     │  ◀── joined per stage in MetricsCollector.flush
   │ (collections.deque + lock)
   └──────────────────┘
       │
       ▼
   BottleneckDetector (4-class, z-score gated) ──▶ RecommendationEngine
       │
       ▼
   FastAPI: /api/runs · /api/metrics · /api/compare · /api/optimizations
       │
       ▼
   Vanilla HTML + Chart.js dashboard (2s polling) at http://localhost:8000/
```

## Tech stack

- Python 3.12, FastAPI 0.115 + uvicorn 0.30
- psutil 6.0, pyinstrument 4.7 (function profiling, not always-on)
- structlog 24, Pydantic 2.9 + pydantic-settings 2.6
- httpx 0.27, aiofiles 24.1, sse-starlette 2.1
- pytest 8.3 + pytest-asyncio 0.24 + pytest-cov 5.0
- Vanilla HTML + vendored Chart.js 4.4.1 (no CDN)
- Docker + Docker Compose (python:3.12-slim, non-root appuser)

## Quick start (Docker)

```bash
make build     # builds app + tester images
make up        # starts the app on http://localhost:8000
open http://localhost:8000/
make logs      # tail app logs
make down      # stop
```

## Demo walkthrough

1. Open the dashboard at `http://localhost:8000/` — 4 live charts (CPU%, memory MB, queue depth, throughput), an optimization dropdown, and "Start load test" / "Apply optimization" buttons.
2. Click **Start load test** to fire a baseline run (1000 records). Watch the charts populate; the latest run appears under "Recent runs".
3. Pick an optimization from the dropdown (e.g. `batch_writer`) and click **Apply optimization** — this triggers a baseline + optimized pair via the `/api/runs` compare-mode endpoint.
4. After a couple of seconds, click the latest run in the recent list (or navigate to `/compare?a=<baseline_id>&b=<optimized_id>`) to see the side-by-side comparison: verdict pill, throughput / p95 / p99 deltas, baseline vs optimized panels.

## API surface

| Method | Path                                       | Purpose                                                      |
| ------ | ------------------------------------------ | ------------------------------------------------------------ |
| GET    | `/`                                        | Live dashboard (HTML + Chart.js)                             |
| GET    | `/compare?a=<id>&b=<id>`                   | Compare view (side-by-side baseline vs optimized)            |
| GET    | `/health`                                  | `{"status":"ok"}`                                            |
| POST   | `/api/runs`                                | Start a baseline run, or a compare run (with `optimization_name`) |
| GET    | `/api/runs?limit=N`                        | List recent runs                                             |
| GET    | `/api/runs/{run_id}`                       | RunSummary detail                                            |
| GET    | `/api/runs/{run_id}/bottlenecks`           | Detected bottlenecks (4-class)                               |
| GET    | `/api/runs/{run_id}/recommendations`       | Optimization recommendations                                 |
| GET    | `/api/metrics/snapshot?window_sec=N`       | Ring-buffer snapshot of recent MetricSamples                 |
| GET    | `/api/metrics/live`                        | Server-Sent Events stream (every `DASHBOARD_REFRESH_SEC`)    |
| GET    | `/api/compare?a=<id>&b=<id>`               | JSON DiffReport + both summaries                             |
| GET    | `/api/optimizations`                       | Registered optimization variants                             |

Example:

```bash
# Start a baseline + batch_writer compare
curl -X POST http://localhost:8000/api/runs \
  -H "Content-Type: application/json" \
  -d '{"log_count":1000,"concurrency":4,"seed":42,"optimization_name":"batch_writer"}'

# Inspect the comparison
curl 'http://localhost:8000/api/compare?a=<baseline_id>&b=<optimized_id>' | jq .diff
```

## Optimization variants

| Name                  | What it changes                                              | Targets                       |
| --------------------- | ------------------------------------------------------------ | ----------------------------- |
| `batch_writer`        | Coalesce records into batches at the write stage             | resource/write, contention/transform→write |
| `object_pool`         | Recycle per-record dicts at the transform stage              | resource/transform, architectural |
| `fsm_parser`          | FSM-based parser instead of `json.loads`                     | serial/parse                  |
| `precompiled_validator` | Frozen-set required-key check (single hash lookup)         | serial/validate               |
| `async_io_variant`    | aiofiles-based async write stage                             | contention/validate→transform |
| `mmap_reader`         | mmap-backed file input source (use when reading from disk)   | large_file_read/parse         |

## Bottleneck taxonomy

| Type            | Detection rule                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------- |
| `serial`        | One stage's p95 latency `>= 1.5x` median of the other stages' p95, z-score `>= 2.0`            |
| `resource`      | Stage CPU `>= 85%` for `>=80%` of window samples, OR memory growth `>= 10 MB/s`                |
| `contention`    | Adjacent queue at maxsize (`>=60%`) → back-pressure; at zero (`>=60%`) → starvation            |
| `architectural` | Throughput `< 30%` of `theoretical_max_lps` AND no other class firing                          |

Every candidate is gated by `BOTTLENECK_Z_THRESHOLD` (default 2.0) against the trailing 60s baseline so transient spikes don't fire. Severity escalates to `high` after two consecutive evaluations on the same `(type, stage)` pair.

## Configuration (env vars)

| Variable                    | Default                                | Notes                                          |
| --------------------------- | -------------------------------------- | ---------------------------------------------- |
| `PROFILER_PORT`             | `8000`                                 | HTTP server port                               |
| `DASHBOARD_REFRESH_SEC`     | `2`                                    | Dashboard poll interval / SSE tick             |
| `DETECTION_WINDOW_SEC`      | `10`                                   | BottleneckDetector window                      |
| `OVERHEAD_TARGET_PCT`       | `2`                                    | Profiling overhead budget                      |
| `METRICS_BUFFER_SIZE`       | `10000`                                | RingBuffer maxlen                              |
| `METRICS_BATCH_SIZE`        | `100`                                  | MetricsCollector flush batch size              |
| `LOAD_TEST_LOG_COUNT`       | `1000`                                 | Default records per run                        |
| `LOAD_TEST_CONCURRENCY`     | `4`                                    | Default concurrency knob                       |
| `BOTTLENECK_Z_THRESHOLD`    | `2.0`                                  | Statistical gate for transient-spike filtering |
| `INSTRUMENTED_STAGES`       | `parse,validate,transform,write`       | CSV of stage names                             |
| `LOG_LEVEL`                 | `INFO`                                 | structlog level                                |

## Testing

```bash
make test          # full unit + integration suite, in Docker
make test-unit     # unit only
make test-int     # integration only
make test-e2e      # tests/e2e/test_optimization_improves.py
make loadtest      # scripts/load_test.py --count 1000
make e2e           # scripts/e2e.sh — full demo flow
```

All tests run **inside Docker** via the `tester` profile — never on the host.

## What I learned

- Per-record `psutil.cpu_times()` calls in the instrumentation hot path will blow any reasonable overhead budget — CPU and memory must be sampled at a fixed interval (ResourceSampler), with the per-call decorator only timing wall-clock duration.
- `collections.deque(maxlen=N) + threading.Lock` is the cleanest in-memory ring buffer; pair with a window-filter `snapshot(window_sec)` for time-series queries.
- Drop-on-full for the metrics queue (`put_nowait` + try/except) is the right back-pressure policy — never block the hot path with the instrumentation queue.
- ASGITransport doesn't support true SSE streaming; real SSE behavior must be exercised against a running container, not inside the test process.
- Z-score gating against a trailing 60s baseline cleanly separates one-off spikes from systematic bottlenecks; severity escalation on two consecutive evaluations gives a small amount of hysteresis without a heavy state machine.

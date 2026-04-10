# Sliding Window Analytics Engine

A FastAPI service that ingests streaming metric events, computes O(1) incremental statistics over in-memory ring-buffer sliding windows, and streams live results to a Chart.js dashboard over WebSockets with Redis-backed checkpointing for restart continuity.

## Overview

The engine accepts metric events (response time, throughput, error rate) from two producers: a built-in `LogEventGenerator` that simulates four services at ~600 evt/s, and a `POST /api/metric` HTTP endpoint for operator-driven ingest. Events flow into a bounded `asyncio.Queue` (backpressure boundary), get drained by a single consumer, and are dispatched to a `WindowManager` that maintains seven concurrent sliding windows at multiple time resolutions. A broadcast loop snapshots every window every 5 seconds and pushes the payload to any connected WebSocket client.

The core idea is that a fixed-size ring buffer plus a pair of running scalars (`sum` and `sum_of_squares`) is enough to compute count, mean, and variance in constant time per insert/remove; mins and maxes come from a monotonic-deque trick with the same amortized O(1) cost. That means memory is bounded regardless of event rate, and every window update touches a handful of cache lines instead of rescanning the full window.

Each architectural piece exists for a reason: **multi-resolution** windows (1m/15m/4h) give you short-term spikes and long-term trends off the same event stream with near-zero extra cost; **backpressure** (bounded queue + drop-oldest + adaptive 1-in-N sampling) keeps the server stable when the producer outpaces the consumer; **Redis checkpointing** serialises the whole manager to a single JSON blob every 10s so trends survive `docker compose restart app`.

## Architecture

```
+--------------------+        POST /api/metric         +--------------------+
|  LogEventGenerator |                                 |      FastAPI       |
|  ~600 evt/s, async |                                 |  GET  /           |
|  spikes 10%        |                                 |  GET  /api/health |
+---------+----------+                                 |  GET  /api/stats  |
          |                                            |  POST /api/metric |
          | submit_generated (sampled)                 |  WS   /ws         |
          |                                            +----+----------+---+
          v                                                 |          ^
    +-----+-------------------+   submit_user (never sampled)|         |
    |  IngestPipeline         | <-------------------------- +          |
    |  asyncio.Queue(maxsize) |                                        |
    |  drop-oldest + sampling |                                        |
    +-----+-------------------+                                        |
          |                                                            |
          | run_consumer (single task)                                 |
          v                                                            |
    +----------------------------------------+                         |
    |           WindowManager                |                         |
    |  response_time: 1m / 15m / 4h          |                         |
    |  throughput:    1m / 15m               |                         |
    |  error_rate:    1m / 15m               |                         |
    |  (7 sliding windows total)             |                         |
    +-----+----------------------------+-----+                         |
          |                            |                               |
          | checkpoint every 10s       | snapshot every 5s             |
          v                            v                               |
    +-----------+            +-------------------+                     |
    |   Redis   |            |  Broadcast loop   |---------------------+
    |  JSON blob|            |  ConnectionManager|     metrics_update
    +-----------+            +-------------------+     + ingest counters
                                                       over WebSocket
                                                              |
                                                              v
                                                     +-----------------+
                                                     |  Chart.js       |
                                                     |  dashboard (/)  |
                                                     +-----------------+
```

Each `SlidingWindow` holds a `deque(maxlen=max_size)` ring buffer plus `IncrementalStats` (count, sum, sum_sq) and `MonotonicMinMax` (two deques). `add(event)` evicts events older than `window_size`, updates the running scalars, and amortises to O(1). `snapshot(now)` returns count / mean / min / max / stddev in constant time.

## Tech Stack

- Python 3.11 (slim base image, multi-stage Docker build)
- FastAPI 0.115 + Uvicorn 0.32 (async REST + WebSocket)
- Pydantic v2 (request validation)
- `redis` 5 / `redis.asyncio` (checkpoint persistence)
- `websockets` 13 (E2E client + FastAPI server)
- NumPy (tests only, used as ground truth for stats accuracy)
- Chart.js (CDN, vanilla JS dashboard, no build step)
- Docker + Docker Compose (4 profiles: default, test, e2e, loadtest)

## How to Run

All commands run inside Docker. There is no host-side Python setup.

```bash
make build     # docker compose build (app + test image)
make up        # redis + app detached, dashboard at http://localhost:8000/
make down      # stop and remove containers
make logs      # follow app container logs
make test      # unit + integration tests inside the test container
make e2e       # full end-to-end: health + POST + /api/stats + WebSocket + checkpoint
make load      # loadtest profile: 1500 req/s for 10s, prints p50/p99 + ingest counters
make clean     # down + remove volumes
```

Once `make up` is green, open `http://localhost:8000/` in a browser and the dashboard will connect to `/ws` and start rendering live charts within ~5 seconds.

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Serves the Chart.js dashboard (static HTML template) |
| `/api/health` | GET | Liveness probe: `{"status":"healthy","active_windows":7}` |
| `/api/metric` | POST | Submit a single metric event. User-submitted events are never sampled |
| `/api/stats` | GET | Nested snapshot: `metrics[name][resolution]` + `active_windows` + `ingest` counters |
| `/ws` | WS | Pushes a `metrics_update` JSON frame every `WS_UPDATE_INTERVAL_SECONDS` |

### `POST /api/metric` body

```json
{
  "metric": "response_time",
  "value": 123.45,
  "metadata": {"source": "manual"}
}
```

Accepted metrics: `response_time`, `throughput`, `error_rate` (events for unknown metrics are dropped by the dispatcher). Response: `{"accepted": true, "event_id": "<uuid>"}`.

### `GET /api/stats` shape

```json
{
  "metrics": {
    "response_time": {
      "1m":  {"count": 1247, "mean": 127.3, "min": 45.1, "max": 412.0, "stddev": 38.2, ...},
      "15m": {...},
      "4h":  {...}
    },
    "throughput":  {"1m": {...}, "15m": {...}},
    "error_rate":  {"1m": {...}, "15m": {...}}
  },
  "active_windows": 7,
  "timestamp": 1712729847.123,
  "ingest": {
    "queue_depth": 3,
    "queue_maxsize": 10000,
    "enqueued": 15234,
    "processed": 15231,
    "dropped": 0,
    "sampled": 0
  }
}
```

## Configuration

All configuration comes from environment variables. Defaults live in `src/config.py`.

| Env var | Default | Purpose |
|---|---|---|
| `WINDOW_SIZE_SECONDS` | `30.0` | Default window duration for the short (1m) resolution family |
| `SLIDE_INTERVAL_SECONDS` | `5.0` | How often windows advance / stats are snapshotted |
| `MAX_EVENT_BUFFER_SIZE` | `10000` | Ring-buffer cap per window; also the ingest queue size |
| `REDIS_HOST` | `localhost` | Redis host for checkpoint storage |
| `REDIS_PORT` | `6379` | Redis port |
| `API_PORT` | `8000` | HTTP port the app binds on |
| `WS_UPDATE_INTERVAL_SECONDS` | `5.0` | Broadcast loop cadence |
| `SPIKE_PROBABILITY` | `0.1` | `LogEventGenerator` spike rate (10% of events) |
| `CHECKPOINT_INTERVAL_SECONDS` | `10.0` | Redis save frequency |
| `CHECKPOINT_MAX_AGE_SECONDS` | `3600.0` | Discard checkpoints older than this on restore |
| `DISABLE_GENERATOR` | `0` | If `1`, skip the `LogEventGenerator` background task (used by unit tests) |
| `DISABLE_CHECKPOINT` | `0` | If `1`, skip Redis connect/save/restore (used by unit tests) |

## What I Learned

- **Welford's algorithm is stable forward but not backward.** For a sliding window that has to remove old events, the subtraction form of Welford's is numerically shaky. Keeping a separate `sum` and `sum_of_squares` and deriving variance as `E[X^2] - E[X]^2` is far simpler, removal-friendly, and still well under the 1% accuracy budget at this scale.
- **Monotonic deques give amortised O(1) sliding min/max.** Each `add` pops elements from the tail that will never again be the min/max because the new element dominates them; each expiry pops from the head. No element is ever touched more than twice, so the per-insert cost is O(1) amortised even though individual calls can be O(k).
- **Wall-clock vs monotonic-clock is a real trap.** During development the generator's "events per second" target was drifting because the loop timer used `loop.time()` while the event timestamps used `time.time()`. Mixing the two against a sleeping loop made the output rate non-stationary. Lesson: pick one clock domain per subsystem and stick to it.
- **Backpressure is about what you drop, not how much you buffer.** A bounded `asyncio.Queue` with drop-oldest + a 1-in-N sampler kicked in once depth exceeds 80% capacity keeps the service stable at 1500 req/s without unbounded memory growth. The user-facing `POST /api/metric` path is marked non-samplable so operator ingest always lands.
- **Multi-resolution windows are essentially free.** Since dispatch is O(k) in the number of windows for that metric (seven total in our config), running a 1m window alongside a 4h window over the same event stream costs almost nothing beyond the extra ring buffers.
- **Redis checkpointing is simpler as a single JSON blob.** Field-at-a-time serialisation tempts you with incremental writes, but it opens a window for partial-restore races on startup. Serialising the full manager state to one key lets restore be "load or start empty" with no intermediate states.
- **A single broadcast loop beats per-connection fan-out.** One asyncio task snapshots the windows once per interval and `gather`s sends to every connected WebSocket, evicting dead clients on failure. Chart.js with vanilla JS and a WebSocket auto-reconnect is plenty for real-time dashboards at this scale, no React or build pipeline needed.
- **Test WebSocket broadcasts with a FakeWebSocket, not a real client.** A tiny stub that records `.send_json` calls makes the `ConnectionManager` unit tests synchronous, deterministic, and ~100x faster than spinning up a real `TestClient` WebSocket. The real client lives in the E2E harness where it belongs.

## Project Structure

```
sliding-window-analytics-engine/
├── Dockerfile              # python:3.11-slim multi-stage (builder + runtime)
├── Dockerfile.test         # adds pytest + E2E script deps
├── docker-compose.yml      # redis + app + test/e2e/loadtest profiles
├── Makefile                # build / up / down / logs / test / e2e / load / clean
├── requirements.txt
├── README.md
├── project_requirements.md
├── src/
│   ├── main.py             # FastAPI app, lifespan, routes
│   ├── config.py           # env-var driven Config dataclass
│   ├── models.py           # Event, WindowResult, WindowConfig dataclasses
│   ├── stats.py            # IncrementalStats + MonotonicMinMax
│   ├── sliding_window.py   # Ring-buffer SlidingWindow class
│   ├── window_manager.py   # Multi-resolution registry + dispatch
│   ├── generator.py        # LogEventGenerator (four simulated services)
│   ├── ingest.py           # IngestPipeline: bounded queue + backpressure
│   ├── websocket.py        # ConnectionManager + broadcast_loop
│   ├── checkpoint.py       # Redis JSON-blob save/restore
│   └── templates/
│       └── dashboard.html  # Chart.js + vanilla JS dashboard
├── tests/
│   ├── conftest.py
│   ├── test_stats.py
│   ├── test_sliding_window.py
│   ├── test_window_manager.py
│   ├── test_generator.py
│   ├── test_ingest.py
│   ├── test_websocket.py
│   ├── test_checkpoint.py
│   └── test_api.py
└── scripts/
    ├── verify_e2e.py       # full-flow E2E harness (e2e profile)
    └── load_test.py        # 1500 req/s driver (loadtest profile)
```

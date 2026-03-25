# Time-Based Window Log Analytics

A system that groups incoming log events into fixed time windows (5-minute, hourly, daily) and computes real-time aggregated metrics with a live dashboard. Supports e-commerce order tracking, late event handling via grace periods, and historical replay.

## How It Runs

Long-lived server process with a FastAPI-based REST/WebSocket API, backed by Redis for state persistence, serving a real-time analytics dashboard and accepting log events continuously.

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST + WebSocket)
- **State Store**: Redis 7 (hashes, sorted sets, TTL-based expiry)
- **Dashboard**: HTML/JS with Chart.js, live updates via WebSocket
- **Task Scheduling**: APScheduler (background window rotation and cleanup)
- **Testing**: pytest, pytest-asyncio, httpx (async test client)
- **Containerization**: Docker & Docker Compose

## Architecture

```
┌──────────────┐       ┌──────────────────────┐       ┌───────────┐
│  Log Sources │──────▶│  FastAPI Ingest API   │──────▶│   Redis   │
│  (HTTP POST) │       │  POST /api/v1/logs    │       │  Windows  │
└──────────────┘       └──────────────────────┘       └─────┬─────┘
                                                            │
                       ┌──────────────────────┐             │
                       │  Window Aggregator   │◀────────────┘
                       │  (5m / 1h / 1d)      │
                       └──────────┬───────────┘
                                  │
                       ┌──────────▼───────────┐
                       │  Window Rotator      │
                       │  active→grace→closed │
                       └──────────┬───────────┘
                                  │
                       ┌──────────▼───────────┐
                       │  WebSocket Broadcast  │
                       │  /ws/dashboard        │
                       └──────────┬───────────┘
                                  │
                       ┌──────────▼───────────┐
                       │  Live Dashboard (JS)  │
                       │  Charts + Counters    │
                       └──────────────────────┘
```

## Core Concepts

### Time Windows

- **5-minute windows** (`5m`): Fine-grained, real-time view of recent activity
- **Hourly windows** (`1h`): Medium-term trend analysis
- **Daily windows** (`1d`): Long-term pattern detection

Windows are aligned to epoch boundaries using floor division: `start_ts = (unix_ts // window_size) * window_size`. This ensures all events in the same wall-clock interval land in the same window regardless of ingestion order.

### Window Lifecycle

Each window progresses through three states:

1. **Active** -- accepting events normally
2. **Grace** -- window has ended, but late-arriving events are still accepted
3. **Closed** -- no more events accepted, data retained for queries until TTL expiry

The `WindowRotator` runs on a background scheduler (APScheduler) and transitions windows through this state machine.

### Aggregated Metrics (per window)

- Total log count
- Counts by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Counts by source/service
- Error rate percentage
- Average response time
- Throughput (events/second)

### E-Commerce Window Types

In addition to standard log windows, the system tracks order-specific metrics:

- **`order_5m`**: 5-minute order tracking windows
- **`revenue_1h`**: Hourly revenue aggregation windows

E-commerce metrics per window:
- Order count
- Total revenue
- Average order value
- Order status breakdown (placed, confirmed, cancelled)

### Redis Data Model

- Window counters stored as Redis hashes with TTL-based expiry
- Sorted sets (`windows:active:{type}`) track active window keys
- JSON-encoded hash fields (`levels`, `services`, `order_statuses`) with optimistic locking for safe concurrent updates

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/logs` | Ingest a single log event |
| POST | `/api/v1/logs/batch` | Ingest a batch of log events |
| GET | `/api/v1/windows/{type}` | Get active window metrics (type: `5m`, `1h`, `1d`, `order_5m`, `revenue_1h`) |
| GET | `/api/v1/windows/{type}/history` | Get closed window summaries |
| GET | `/api/v1/windows/{type}/ecommerce` | Get e-commerce metrics for a window type |
| POST | `/api/v1/replay` | Re-process historical events to reconstruct windows |
| GET | `/api/v1/stats` | Get system statistics (uptime, active windows, event counts) |
| WS | `/ws/dashboard` | WebSocket stream for live dashboard updates |
| GET | `/dashboard` | Serve the live analytics dashboard |
| GET | `/health` | Health check |

### Log Event Schema

```json
{
  "timestamp": "2026-03-24T10:15:30.123Z",
  "level": "ERROR",
  "source": "auth-service",
  "message": "Failed to validate JWT token",
  "metadata": {
    "user_id": "u-1234",
    "request_id": "req-5678"
  },
  "response_time": 0.245,
  "order_id": "ORD-001",
  "order_value": 99.95,
  "order_status": "placed"
}
```

The `metadata`, `response_time`, `order_id`, `order_value`, and `order_status` fields are all optional. When `order_id` is present, the event is also tracked in e-commerce aggregations.

## How to Run

```bash
# Build and start all services (app + Redis)
docker compose up --build -d

# Server available at http://localhost:8080
# Dashboard at http://localhost:8080/dashboard
# API docs at http://localhost:8080/docs

# View logs
make logs

# Stop everything
make down
```

### Running Tests

```bash
# Unit and integration tests
make test

# End-to-end verification (starts app + Redis, sends real traffic)
make e2e

# Load test (sends sustained traffic, measures throughput)
make loadtest

# Full cleanup (removes containers, volumes, images)
make clean
```

### Quick Ingest Example

```bash
# Send a single event
curl -X POST http://localhost:8080/api/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": "2026-03-24T10:15:30Z",
    "level": "ERROR",
    "source": "auth-service",
    "message": "JWT validation failed"
  }'

# Check 5-minute window metrics
curl http://localhost:8080/api/v1/windows/5m
```

## What I Learned

- **Time-window alignment via epoch floor division** -- `(unix_ts // window_size) * window_size` is a simple, efficient way to bucket events into fixed intervals without needing calendar-aware logic. All events within the same wall-clock interval share the same start timestamp regardless of arrival order.

- **Redis hash-based aggregation with atomic operations** -- storing each window as a Redis hash and using `HINCRBY`/`HINCRBYFLOAT` for counters gives lock-free, O(1) updates for the common case. Complex fields (level counts, service counts) require a different approach since Redis has no native nested-hash increment.

- **Optimistic locking for JSON field updates** -- for hash fields that store JSON objects (like per-level counts), the system uses Redis `WATCH`/`MULTI` transactions: read the field, deserialize, increment, write back in a pipeline. On conflict the operation retries automatically. This avoids needing distributed locks while keeping correctness under concurrency.

- **APScheduler integration with asyncio** -- the `BackgroundScheduler` runs in a separate thread but bridges into the asyncio event loop via `asyncio.run_coroutine_threadsafe()`. This pattern lets periodic tasks (window rotation, expired key cleanup) coexist with the async FastAPI server without blocking the event loop.

- **WebSocket broadcasting for real-time dashboard updates** -- a background `asyncio.create_task` loop periodically collects metrics from all active windows and broadcasts a JSON payload to all connected WebSocket clients. Dead connections are detected by catching send errors and pruned from the connection set.

- **Chart.js real-time chart patterns** -- the dashboard uses Chart.js with a fixed-length data buffer, shifting old points off the left edge as new data arrives from WebSocket messages. This creates a sliding-window visualization that mirrors the backend's time windows.

- **Late event handling with grace periods** -- each window type defines a grace period after the window officially closes. Events arriving during grace are accepted and aggregated (flagged as "late") rather than rejected. This handles clock skew and delayed log delivery without losing data.

- **Docker Compose multi-service orchestration** -- health checks with `service_healthy` conditions ensure the app waits for Redis before starting. Profiles (`test`, `e2e`, `loadtest`) keep test containers out of the default `up` command while sharing the same compose file.

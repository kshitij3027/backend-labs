# JSON Log Analytics

> A structured log ingestion server with real-time analytics, alerting, and a web dashboard.

## Tech Stack

- **Python 3.12**
- **Flask 3.1** (web framework)
- **JSONSchema 4.23** (log validation against Draft 2020-12)
- **APScheduler 3.10** (periodic background alert checks)
- **Chart.js** (client-side charting via CDN)
- **Gunicorn 23.0** (production WSGI server)
- **Docker + Docker Compose**

## Architecture

The application runs as a single Gunicorn process with 1 worker and 4 threads. All state is held in-memory (no database) -- a bounded `deque` for log storage, `defaultdict` for analytics buckets, and lists for alerts. Thread safety is achieved through `threading.Lock` on every shared data structure.

A background APScheduler job runs every 30 seconds to detect services that have gone silent (no logs for 2+ minutes).

### Module Dependency Flow

```
config.py --> validator.py --> log_store.py --> analytics.py --> alerting.py
                                                                    |
                              simulator.py <-- app.py (Flask routes, wires all)
                                                  |
                                          templates/dashboard.html
```

- **config.py** -- Loads `config.yaml`, deep-merges with hardcoded defaults, exposes dict-like access.
- **validator.py** -- Validates incoming JSON against `schemas/log_schema.json` using Draft 2020-12. Tracks validation stats (total/valid/invalid, error types).
- **log_store.py** -- Thread-safe bounded deque (`maxlen=1000` by default). Tracks both current size and all-time total count.
- **analytics.py** -- Time-bucketed analytics engine. Groups logs into per-minute buckets keyed by ISO-format strings (`2024-01-15T14:30`). Computes error rates, service health, user activity, and time series data.
- **alerting.py** -- Evaluates three alert rules: error rate threshold, high volume per service, and service-down detection. Uses a Protocol-based handler system and per-rule cooldowns.
- **simulator.py** -- Generates realistic random log entries across 5 services with weighted log levels and optional error rate overrides.
- **app.py** -- Flask application factory. Wires all components together, registers routes, and starts the APScheduler.

## Key Design Decisions

1. **No numpy/pandas** -- Counters, `defaultdict`, and `deque` handle all analytics. Keeps the dependency footprint minimal and the code easy to reason about.
2. **Single Gunicorn worker** -- In-memory state cannot be shared across processes. One worker with 4 threads provides concurrency without state duplication.
3. **Time bucket keys as ISO minute strings** -- Keys like `"2024-01-15T14:30"` are hashable, human-readable, and naturally sortable. Old buckets are evicted when the count exceeds `max_buckets` (default 60).
4. **Alert evaluation on ingest** -- Error rate and high volume checks run on every `POST /api/logs` call, so alerts fire in near real-time rather than waiting for a polling interval.
5. **Protocol-based plugin system** -- `AlertHandler` is a `typing.Protocol`. Any object with a `handle(alert: dict) -> None` method works as a handler, no inheritance required.
6. **Cooldown-based alert deduplication** -- Each alert rule has a per-service cooldown (default 300s) to prevent alert storms during sustained error conditions.

## How to Run

### Docker (recommended)

```bash
docker compose up -d app
# App available at http://localhost:5050
```

The container runs Gunicorn on port 5000 internally, mapped to host port 5050. A Docker healthcheck pings `/health` every 10 seconds.

### Generate sample data

Seed the system with normal traffic:

```bash
curl -X POST http://localhost:5050/api/simulate-logs \
  -H "Content-Type: application/json" \
  -d '{"count": 200}'
```

Inject errors to trigger alerts:

```bash
curl -X POST http://localhost:5050/api/simulate-errors \
  -H "Content-Type: application/json" \
  -d '{"count": 50, "error_rate": 0.8}'
```

You can also target a specific service:

```bash
curl -X POST http://localhost:5050/api/simulate-errors \
  -H "Content-Type: application/json" \
  -d '{"count": 30, "error_rate": 1.0, "service": "payment-service"}'
```

### Run tests

```bash
docker compose run --rm tests
```

Tests run with `pytest` inside a dedicated container (`Dockerfile.test`) with coverage reporting.

### Local development (without Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
# Runs Flask dev server on http://localhost:5000
```

## API Reference

### `GET /health`

Health check endpoint.

**Response** `200 OK`:
```json
{
  "status": "healthy",
  "total_logs": 250,
  "current_stored": 250
}
```

### `POST /api/logs`

Ingest a single log entry. The request body is validated against the JSON schema before storage.

**Request body**:
```json
{
  "timestamp": "2024-01-15T14:30:00+00:00",
  "level": "ERROR",
  "service": "auth-service",
  "message": "Authentication failed",
  "user_id": "user-5",
  "metadata": {
    "processing_time_ms": 142.5,
    "request_id": "req-1234"
  }
}
```

Required fields: `timestamp`, `level`, `service`, `message`.
Optional fields: `user_id`, `metadata`.
Valid levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.

**Response** `201 Created`:
```json
{"status": "accepted"}
```

**Response** `400 Bad Request` (validation failure):
```json
{
  "status": "invalid",
  "errors": ["'level' is a required property"]
}
```

### `GET /api/advanced-dashboard-data`

Returns the full dashboard payload in a single call: summary stats, time series, error trends, service health, most active services, user activity, recent logs, active alerts, and validation stats.

**Response** `200 OK`:
```json
{
  "summary": {
    "total_logs": 250,
    "total_errors": 30,
    "error_rate": 0.12,
    "active_services": 5,
    "time_range": {"first": "2024-01-15T14:00", "last": "2024-01-15T14:30"}
  },
  "time_series": [{"time": "2024-01-15T14:28", "total": 15, "errors": 2}],
  "error_trends": [{"time": "2024-01-15T14:28", "error_rate": 0.13}],
  "service_health": {"auth-service": {"status": "healthy", "last_seen": "...", "log_count": 50}},
  "most_active_services": [{"service": "api-gateway", "count": 80}],
  "user_activity": [{"user_id": "user-3", "count": 12}],
  "recent_logs": [],
  "active_alerts": [],
  "validation_stats": {"total": 250, "valid": 248, "invalid": 2, "error_types": {}}
}
```

### `POST /api/simulate-logs`

Generate random log entries with realistic distribution across services and log levels.

**Request body**:
```json
{"count": 100}
```

`count` is capped at 1000. Logs are spread across the last 5 minutes.

**Response** `200 OK`:
```json
{"status": "simulated", "requested": 100, "accepted": 100}
```

### `POST /api/simulate-errors`

Generate log entries with a configurable error rate for testing alerts.

**Request body**:
```json
{"count": 50, "error_rate": 0.8, "service": "payment-service"}
```

`count` is capped at 500. `error_rate` controls the probability of ERROR/CRITICAL levels (0.0 to 1.0). `service` is optional (random if omitted). Logs are spread across the last 2 minutes.

**Response** `200 OK`:
```json
{"status": "simulated", "requested": 50, "accepted": 50}
```

### `GET /api/time-series`

Returns time-bucketed log volume and error counts.

**Query parameters**: `minutes` (default 30) -- number of recent minutes to include.

**Response** `200 OK`:
```json
[
  {"time": "2024-01-15T14:28", "total": 15, "errors": 2},
  {"time": "2024-01-15T14:29", "total": 22, "errors": 5}
]
```

### `GET /api/validation-stats`

Returns cumulative schema validation statistics.

**Response** `200 OK`:
```json
{
  "total": 300,
  "valid": 295,
  "invalid": 5,
  "error_types": {"required": 3, "enum": 2}
}
```

### `GET /`

Serves the web dashboard (HTML + Chart.js).

## Dashboard Features

- **Real-time stats cards** -- Total logs, error rate (turns red above 10%), services tracked, active alert count.
- **Log volume chart** -- Line chart showing per-minute log counts over the last 30 minutes.
- **Error rate chart** -- Line chart showing per-minute error rates with filled area.
- **Active alerts panel** -- Color-coded alert cards (red for CRITICAL, orange for WARNING) showing rule name, message, timestamp, and affected service.
- **Service health grid** -- Per-service status indicators (green/yellow/red dot) with log count and last-seen time.
- **Recent logs table** -- Last 20 log entries with color-coded level badges (DEBUG, INFO, WARNING, ERROR, CRITICAL).
- **Auto-refresh** -- Dashboard fetches new data from `/api/advanced-dashboard-data` every 5 seconds.

## Log Schema

Logs are validated against a JSON Schema (Draft 2020-12) defined in `schemas/log_schema.json`:

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `timestamp` | string (date-time) | Yes | ISO 8601 format |
| `level` | string (enum) | Yes | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `service` | string | Yes | Non-empty service identifier |
| `message` | string | Yes | Non-empty log message |
| `user_id` | string | No | User identifier |
| `metadata` | object | No | Arbitrary metadata, supports `processing_time_ms`, `request_id`, `ip_address` |

No additional top-level properties are allowed (`additionalProperties: false`).

## Configuration

Application configuration is loaded from `config.yaml` and merged with built-in defaults. Override with the `CONFIG_PATH` environment variable.

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `server` | `host` | `0.0.0.0` | Bind address |
| `server` | `port` | `5000` | Server port |
| `storage` | `max_logs` | `1000` | Maximum logs held in memory |
| `analytics` | `max_buckets` | `60` | Maximum time buckets retained |
| `alerting` | `error_rate_threshold` | `0.10` | Error rate to trigger alert (10%) |
| `alerting` | `high_volume_threshold` | `100` | Log count per service to trigger alert |
| `alerting` | `service_down_minutes` | `2` | Minutes of silence before service-down alert |
| `alerting` | `cooldown_seconds` | `300` | Seconds between repeated alerts of the same type |

## What I Learned

- **Thread-safe in-memory data structures** -- Using `threading.Lock` to protect shared state (`deque`, `defaultdict`, lists) across Gunicorn threads without a database.
- **Time-bucketed analytics** -- Grouping events into per-minute buckets with `defaultdict` and evicting old buckets with a bounded `deque` for memory control.
- **JSON Schema validation** -- Using `jsonschema` with Draft 2020-12 to validate incoming payloads and track validation error statistics by type.
- **Flask application factory pattern** -- Structuring the app with `create_app()` for clean dependency injection and testability.
- **Protocol-based plugin systems** -- Using `typing.Protocol` with `@runtime_checkable` to define handler interfaces without requiring class inheritance.
- **APScheduler for background tasks** -- Running periodic service-down checks alongside the Flask request/response cycle in the same process.
- **Gunicorn worker/thread configuration** -- Understanding why a single-worker, multi-thread setup is necessary when all state lives in-memory.
- **Chart.js integration** -- Building a real-time dashboard with auto-refreshing charts and dynamic DOM updates using vanilla JavaScript.
- **Alert cooldown and deduplication** -- Preventing alert storms by tracking per-rule, per-service cooldown timestamps.

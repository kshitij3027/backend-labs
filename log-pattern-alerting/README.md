# Intelligent Log Pattern Alerting System

A pattern-based alert detection engine that analyzes incoming logs against configurable regex rules, correlates related alerts within time windows to prevent notification storms, and serves a real-time WebSocket dashboard for alert lifecycle management.

## Tech Stack

- **Language:** Python 3.11
- **Web Framework:** FastAPI (async REST API + WebSocket)
- **ORM:** SQLAlchemy 2.x (async with asyncpg)
- **Database:** PostgreSQL 16
- **Cache / Rate Limiting:** Redis 7
- **Real-time:** WebSocket (native FastAPI)
- **Frontend:** Single-file vanilla JS dashboard (served by FastAPI)
- **Containerization:** Docker Compose

## Architecture

```
                        +-----------------------+
                        |     REST Client       |
                        |  (curl / dashboard)   |
                        +----------+------------+
                                   |
                    POST /test/inject_log
                                   |
                                   v
+--------------------------------------------------------------+
|                       FastAPI Server                         |
|                                                              |
|   +------------------+    +-----------------------------+    |
|   |  REST API        |    |  WebSocket Endpoint  /ws    |    |
|   |  /test/inject_log|    |  (real-time alert feed)     |    |
|   |  /alerts         |    +-------------+---------------+    |
|   |  /stats          |                  ^                    |
|   |  /health         |                  | broadcast          |
|   +--------+---------+                  |                    |
|            |                            |                    |
|            v                            |                    |
|   +--------+----------------------------+---------------+    |
|   |            Alert Processing Pipeline                |    |
|   |                                                     |    |
|   |  1. Pattern Matcher (compiled regex rules)          |    |
|   |  2. Alert Correlator (time-window dedup)            |    |
|   |  3. Rate Limiter (per-minute cap via Redis)         |    |
|   |  4. Alert Store (PostgreSQL persistence)            |    |
|   |  5. WebSocket Broadcast (real-time push)            |    |
|   +--------+--------------------+-----------------------+    |
+------------|--------------------|--------------------------+
             |                    |
             v                    v
    +--------+--------+  +-------+--------+
    |   PostgreSQL 16  |  |    Redis 7     |
    |  - alert_rules   |  |  - rate limits |
    |  - alerts        |  |  - correlation |
    |  - log_entries   |  |    cache       |
    +-----------------+  +----------------+
```

**Data flow:** Log injected via REST -> Pattern Matcher checks all enabled regex rules -> matches are correlated within a time window to prevent duplicate alerts -> Rate Limiter enforces per-minute caps via Redis -> Alert stored in PostgreSQL -> WebSocket broadcasts the alert to connected dashboard clients.

## How to Run

```bash
# Start the full stack
docker compose up -d --build

# Visit the real-time dashboard:
#   http://localhost:8000

# API docs (Swagger UI):
#   http://localhost:8000/docs

# Inject a test log:
curl -X POST http://localhost:8000/test/inject_log \
  -H "Content-Type: application/json" \
  -d '{"message": "Authentication failed for user admin", "level": "ERROR"}'

# List active alerts:
curl http://localhost:8000/alerts

# Check system stats:
curl http://localhost:8000/stats

# Acknowledge an alert:
curl -X POST http://localhost:8000/alerts/1/acknowledge \
  -H "Content-Type: application/json" \
  -d '{"acknowledged_by": "oncall-engineer"}'

# Resolve an alert:
curl -X POST http://localhost:8000/alerts/1/resolve
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Real-time alert dashboard (HTML) |
| GET | `/health` | Health check (database + Redis status) |
| GET | `/stats` | Alert statistics (active count, by severity) |
| GET | `/alerts` | List all alerts (optional `?state=` filter) |
| GET | `/alerts/{id}` | Get a single alert by ID |
| POST | `/alerts/{id}/acknowledge` | Acknowledge an alert |
| POST | `/alerts/{id}/resolve` | Resolve an alert |
| POST | `/test/inject_log` | Inject a log entry into the alert pipeline |
| WS | `/ws` | WebSocket for real-time alert updates |

## Testing

```bash
# Unit tests
docker compose --profile test run --rm test

# End-to-end tests (full pipeline verification)
docker compose --profile e2e run --rm e2e

# Load test (concurrent injection + performance metrics)
docker compose --profile loadtest run --rm loadtest
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://alertuser:alertpass@postgres:5432/alertdb` | Async database connection string |
| `SYNC_DATABASE_URL` | `postgresql://alertuser:alertpass@postgres:5432/alertdb` | Sync database URL (for init script) |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `CORRELATION_WINDOW` | `300` | Time window (seconds) for correlating related alerts |
| `MAX_ALERTS_PER_MINUTE` | `10` | Rate limit: max alerts created per minute |
| `AUTO_ESCALATION_TIMEOUT` | `900` | Seconds before an unacknowledged alert auto-escalates |

## Default Alert Patterns

These rules are seeded on first startup via the `db-init` container:

| Name | Regex Pattern | Threshold | Window | Severity |
|------|--------------|-----------|--------|----------|
| `auth_failure` | `authentication\s+failed\|login\s+failed\|auth\s+error` | 5 | 60s | high |
| `database_error` | `database\s+error\|connection\s+timeout\|query\s+failed` | 3 | 120s | critical |
| `api_error` | `api\s+error\|endpoint\s+failed\|request\s+timeout` | 5 | 60s | medium |

## What I Learned

- **Compiled regex caching** matters at scale -- precompiling patterns on startup and reusing them across requests avoids repeated compilation overhead during log processing.
- **Alert correlation with time windows** is the key to preventing notification storms. Grouping related alerts within a configurable window and incrementing a counter instead of creating new alerts keeps the signal-to-noise ratio high.
- **Redis-based rate limiting** provides a simple and effective mechanism for capping alert creation. Using Redis TTL keys for sliding windows is both atomic and horizontally scalable.
- **WebSocket lifecycle management** in FastAPI requires careful handling of connection/disconnection events, especially under concurrent load where clients may disconnect mid-broadcast.
- **Async end-to-end pipelines** (asyncpg + httpx + asyncio Semaphore) can sustain high throughput with low latency, but database connection pool sizing becomes the bottleneck before CPU does.

# Dead Letter Queue Log Processor

A message processing system that catches failed log entries, retries them with exponential backoff, and routes permanently failed messages to a dead letter queue with monitoring, analysis, and recovery capabilities.

## Tech Stack

- **Language:** Python 3.11 (async/await throughout)
- **Message Broker / State Store:** Redis (lists, sorted sets, hashes)
- **Web Dashboard:** aiohttp (REST API + WebSocket for real-time updates)
- **Containerization:** Docker & Docker Compose
- **Testing:** pytest, pytest-asyncio, fakeredis

## Architecture

```
┌──────────────┐     ┌───────────────────┐     ┌─────────────────┐
│   Producer   │────>│   Redis List      │────>│   Processor     │
│ (log entries)│     │  (main queue)     │     │ (consume + ack) │
└──────────────┘     └───────────────────┘     └────────┬────────┘
                                                        │
                                              success?──┤
                                              │         │
                                             YES       NO
                                              │         │
                                              v         v
                                           [done]  ┌──────────┐
                                                    │ Classify │
                                                    │ failure  │
                                                    └────┬─────┘
                                                         │
                                          ┌──────────────┤
                                          │              │
                                    retries left?   max retries
                                          │          exceeded
                                          v              │
                                   ┌────────────┐        v
                                   │   Retry    │   ┌─────────┐
                                   │  Scheduler │   │  Dead   │
                                   │ (sorted set│   │ Letter  │
                                   │ w/ backoff)│   │  Queue  │
                                   └──────┬─────┘   └────┬────┘
                                          │              │
                                          v              v
                                    [re-enqueue]   ┌────────────┐
                                                   │ DLQ Handler│
                                                   │ (analyze,  │
                                                   │  alert,    │
                                                   │  recover)  │
                                                   └──────┬─────┘
                                                          │
                                                          v
                                                   ┌────────────┐
                                                   │  Dashboard │
                                                   │  (aiohttp  │
                                                   │  REST + WS)│
                                                   └────────────┘
```

## Components

| Component | Description |
|---|---|
| **Producer** | Generates log entries at configurable rates, injecting a mix of valid and intentionally malformed messages to simulate real-world failure scenarios |
| **Processor** | Consumes messages from the main Redis list via BRPOP, attempts processing, and manages the retry lifecycle with exponential backoff |
| **Retry Scheduler** | Polls the retry sorted set, re-enqueuing messages whose backoff timer has expired back into the main queue |
| **Failure Classifier** | Categorizes exceptions into failure types (PARSING, NETWORK, RESOURCE, UNKNOWN) with per-type retry limits |
| **DLQ Handler** | Receives permanently failed messages, provides analysis breakdowns, and supports selective or bulk replay back to the main queue |
| **Stats Tracker** | Tracks processing counters, failure trends over rolling windows, DLQ growth rate, and alert conditions |
| **Web Dashboard** | Real-time aiohttp dashboard with REST API and WebSocket push, showing processing metrics, DLQ contents, trends, and alerts |
| **Run Orchestrator** | Entry point that launches all components as concurrent asyncio tasks with graceful signal-based shutdown |

## Key Features

- **Exponential backoff** for retry scheduling (base * 2^attempt)
- **Per-failure-type retry limits** -- parsing errors retry once, network errors retry up to 5 times
- **Failure classification** -- messages in the DLQ are tagged by failure type (PARSING, NETWORK, RESOURCE, UNKNOWN)
- **Message replay** -- recover and re-enqueue all DLQ messages or filter by failure type
- **Real-time dashboard** -- live metrics via WebSocket (processed/failed/retried counts, DLQ depth, throughput)
- **Trend analysis** -- rolling-window failure breakdowns by type and source, top errors
- **Alerting** -- DLQ size threshold alerts, growth rate warnings, dominant failure type detection
- **Graceful shutdown** -- all components handle SIGTERM/SIGINT cleanly via asyncio.Event

## How to Run

```bash
# Build and start all services
docker-compose up --build

# Dashboard available at
# http://localhost:8000

# Run unit tests
docker-compose run --rm --build test

# Run end-to-end verification (starts app + redis, then runs checks)
docker-compose --profile e2e up --build --abort-on-container-exit
```

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Dashboard HTML page |
| GET | `/health` | Health check |
| GET | `/api/stats` | Processing statistics (processed, failed, retried, DLQ size, queue length) |
| GET | `/api/dlq` | List all DLQ messages |
| GET | `/api/dlq/analysis` | DLQ breakdown by failure type, source, retry counts |
| POST | `/api/dlq/reprocess` | Move all DLQ messages back to the main queue |
| POST | `/api/dlq/reprocess/{type}` | Reprocess only messages of a specific failure type |
| POST | `/api/dlq/purge` | Delete all DLQ messages |
| GET | `/api/trends?window=300` | Failure trends over a rolling window (seconds) |
| GET | `/api/alerts` | Currently active alerts |
| GET | `/ws` | WebSocket endpoint for real-time stats |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `MAIN_QUEUE` | `log_processing` | Name of the main processing queue |
| `DLQ_QUEUE` | `dead_letter_queue` | Name of the dead letter queue |
| `MAX_RETRIES` | `3` | Global max retry attempts (overridden per failure type) |
| `BACKOFF_BASE` | `1.0` | Base delay (seconds) for exponential backoff |
| `PRODUCER_RATE` | `10.0` | Messages produced per second |
| `FAILURE_RATE` | `0.3` | Fraction of messages that simulate failure (0.0-1.0) |
| `DASHBOARD_PORT` | `8000` | Web dashboard port |
| `RETRY_POLL_INTERVAL` | `0.5` | How often (seconds) to check for due retries |
| `WS_BROADCAST_INTERVAL` | `1.5` | How often (seconds) to push stats via WebSocket |
| `DLQ_ALERT_THRESHOLD` | `50` | DLQ size that triggers a high-severity alert |
| `FAILURE_HISTORY_MAX` | `10000` | Max failure history entries kept in Redis |

## What I Learned

- **Redis sorted sets for time-based retry scheduling.** Using ZADD with Unix timestamps as scores and ZRANGEBYSCORE to poll for due retries creates an efficient, persistent delay queue without any external scheduler.
- **Exponential backoff with per-failure-type retry limits.** Not all failures deserve the same retry effort -- parsing errors (malformed data) are unlikely to succeed on retry, while network timeouts often resolve. Classifying failures and assigning different max-retry limits per type prevents wasted work.
- **asyncio.gather() for concurrent component orchestration.** Running the producer, processor, retry scheduler, and dashboard as parallel coroutines in a single process keeps the architecture simple while still achieving concurrent I/O through the event loop.
- **aiohttp WebSocket for real-time dashboard updates.** The server maintains a set of connected WebSocket clients and broadcasts stats snapshots on a timer. Dead connections are detected and pruned automatically.
- **Dead letter queue pattern for fault-tolerant message processing.** Messages that exhaust their retries are moved to a separate queue with full failure context (type, error details, retry count, timestamps), preserving them for analysis and later replay instead of silently dropping them.
- **Failure trend analysis over rolling time windows.** By recording each failure event with a timestamp in a Redis list and filtering by time window on read, the system can surface patterns like "90% of recent failures are PARSING errors from api-gateway" without any time-series database.
- **Graceful shutdown with signal handling.** A shared asyncio.Event propagated to every component ensures that SIGINT/SIGTERM triggers a clean wind-down: the producer stops sending, the processor finishes its current message, and the dashboard server closes open connections before the process exits.

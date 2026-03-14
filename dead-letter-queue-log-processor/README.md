# Dead Letter Queue Log Processor

A message processing system that catches failed log entries, retries them with exponential backoff, and routes permanently failed messages to a dead letter queue with monitoring, analysis, and recovery capabilities.

## Tech Stack

- **Language:** Python 3.11
- **Message Broker / State Store:** Redis (streams, sorted sets, hashes)
- **Web Dashboard:** Flask + Flask-SocketIO (real-time WebSocket updates)
- **Containerization:** Docker & Docker Compose
- **Testing:** pytest, fakeredis

## Architecture

```
┌──────────────┐     ┌───────────────────┐     ┌─────────────────┐
│   Producer   │────▶│  Redis Streams     │────▶│   Processor     │
│ (log entries)│     │  (main queue)      │     │ (consume + ack) │
└──────────────┘     └───────────────────┘     └────────┬────────┘
                                                        │
                                              success?──┤
                                              │         │
                                             YES       NO
                                              │         │
                                              ▼         ▼
                                           [done]  ┌──────────┐
                                                   │  Retry   │
                                                   │  Queue   │
                                                   │ (sorted  │
                                                   │  set w/  │
                                                   │  backoff)│
                                                   └────┬─────┘
                                                        │
                                                  max retries?
                                                  │         │
                                                 NO        YES
                                                  │         │
                                                  ▼         ▼
                                             [re-enqueue] ┌─────────┐
                                                          │  Dead   │
                                                          │ Letter  │
                                                          │  Queue  │
                                                          └────┬────┘
                                                               │
                                                               ▼
                                                        ┌────────────┐
                                                        │ DLQ Handler│
                                                        │ (analyze,  │
                                                        │  alert,    │
                                                        │  recover)  │
                                                        └────────────┘
```

## Components

| Component | Description |
|---|---|
| **Producer** | Generates log entries at configurable rates, injecting a mix of valid and intentionally malformed messages to simulate real-world failure scenarios |
| **Processor** | Consumes messages from the main Redis stream, attempts processing, and manages the retry lifecycle with exponential backoff |
| **Retry Scheduler** | Monitors the retry sorted set, re-enqueuing messages whose backoff timer has expired back into the main stream |
| **DLQ Handler** | Receives permanently failed messages, categorizes failure reasons, triggers alerts, and provides recovery/replay capabilities |
| **Web Dashboard** | Real-time Flask-SocketIO dashboard showing processing metrics, retry counts, DLQ contents, and failure analysis |

## Key Features

- **Exponential backoff** with jitter for retry scheduling
- **Configurable max retries** before routing to DLQ
- **Failure categorization** — messages in the DLQ are tagged by failure type (malformed, oversized, encoding error, etc.)
- **Message replay** — recover and re-enqueue DLQ messages after fixes
- **Real-time dashboard** — live metrics via WebSocket (processed/failed/retried counts, DLQ depth, throughput)
- **Graceful shutdown** — all components handle SIGTERM/SIGINT cleanly

## How to Run

```bash
# Build and start all services
docker-compose up --build

# Dashboard available at
http://localhost:5555

# Run tests
docker-compose run --rm app pytest tests/ -v
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `MAX_RETRIES` | `5` | Max retry attempts before DLQ |
| `BASE_BACKOFF_SEC` | `1` | Base delay for exponential backoff |
| `PRODUCER_RATE` | `10` | Messages produced per second |
| `FAILURE_RATE` | `0.3` | Fraction of messages that simulate failure |
| `DASHBOARD_PORT` | `5555` | Web dashboard port |

## What I Learned

_To be filled in after implementation._

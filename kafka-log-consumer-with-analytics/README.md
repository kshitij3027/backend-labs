# Kafka Log Consumer with Analytics

A production-style Kafka consumer system that reads log messages from multiple topics, processes them in configurable batches with manual offset management, computes real-time analytics (throughput, percentiles, error rates), and exposes results via a live web dashboard with WebSocket updates.

## Tech Stack

- **Language**: Python 3.12
- **Messaging**: Apache Kafka (via confluent-kafka)
- **Web Framework**: FastAPI + Uvicorn
- **Persistence**: Redis (analytics snapshot storage)
- **Frontend**: HTML/JS dashboard with WebSocket live updates
- **Infrastructure**: Docker Compose (Kafka, Zookeeper, Redis)
- **Testing**: pytest

## Architecture

```
┌──────────────────┐     ┌───────────────────────────────────────────────────┐
│   Kafka Topics   │     │  Consumer Application (port 8080)                │
│                  │     │                                                   │
│  - web-logs      │────>│  ┌──────────────┐   ┌─────────────┐             │
│  - app-logs      │     │  │ LogConsumer   │──>│   Batch     │             │
│  - error-logs    │     │  │ (group:       │   │  Processor  │             │
│                  │     │  │  log-         │   └──────┬──────┘             │
│  - dead-letter-  │<──┐ │  │  processing-  │          │                    │
│    logs          │   │ │  │  group)       │   ┌──────▼──────┐             │
└──────────────────┘   │ │  └──────────────┘   │  Analytics   │             │
                       │ │    │ retry/DLQ       │  Engine      │             │
                       │ │    └─────────────────┘──────┬──────┘             │
                       │ │                             │                    │
                       │ │                      ┌──────▼──────┐             │
                       │ │                      │    Redis     │             │
                       │ │                      │  (snapshots) │             │
                       │ │                      └──────┬──────┘             │
                       │ │                             │                    │
                       │ │  ┌──────────────────────────▼───────────────┐   │
                       │ │  │  FastAPI Dashboard (HTTP + WebSocket)    │   │
                       └─│──│  GET /api/stats, /api/analytics, /ws    │   │
                         │  └─────────────────────────────────────────┘   │
                         └───────────────────────────────────────────────────┘
```

### Key Components

1. **LogConsumer** -- Background Kafka consumer thread with manual offset commit, subscribed to `web-logs`, `app-logs`, and `error-logs`. Handles rebalancing, exponential backoff retry, dead-letter routing, and dynamic throttling.

2. **BatchProcessor** -- Accumulates messages into configurable batches (by count or time window), parses JSON payloads into typed Pydantic models (`WebAccessLog`, `AppLog`, `ErrorLog`), and routes them to the analytics engine.

3. **AnalyticsEngine** -- Computes real-time metrics over a sliding window:
   - Throughput (messages/sec via pre-aggregated second buckets)
   - Response time percentiles (P50, P95, P99)
   - Per-endpoint stats (request count, error count, avg/p95 response time)
   - Error rates by endpoint
   - Geographic distribution
   - Consumer lag per partition with high-lag alerting

4. **RedisStore** -- Periodic snapshot persistence so analytics state survives container restarts.

5. **FastAPI Dashboard** -- Web UI with real-time charts via WebSocket push, plus a REST API for programmatic access.

## How to Run

```bash
# 1. Start the full stack (Kafka, Zookeeper, Redis, topic init, consumer)
docker-compose up -d

# 2. Open the dashboard
open http://localhost:8080

# 3. Produce test log messages
docker-compose run --rm --no-deps producer

# 4. Run E2E verification
docker-compose run --rm --no-deps e2e

# 5. Stop everything
docker-compose down -v
```

### Running Tests

```bash
# Unit tests in Docker
docker-compose run --rm test

# Unit tests locally (requires Python 3.12 and dependencies)
PYTHONPATH=. pytest tests/ -v

# With coverage
PYTHONPATH=. pytest tests/ -v --cov=src --cov-report=term-missing
```

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | `log-processing-group` | Consumer group ID |
| `KAFKA_TOPICS` | `web-logs,app-logs,error-logs` | Comma-separated topic list |
| `BATCH_SIZE` | `100` | Messages per batch before processing |
| `BATCH_TIMEOUT_S` | `5.0` | Max seconds before flushing a partial batch |
| `REDIS_HOST` | `redis` | Redis host for snapshot persistence |
| `REDIS_PORT` | `6379` | Redis port |
| `DASHBOARD_PORT` | `8080` | Web dashboard port |
| `SLIDING_WINDOW_SECONDS` | `60` | Sliding window size for throughput metrics |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Web dashboard UI (HTML) |
| `GET` | `/api/stats` | Consumer status, processor stats, analytics summary |
| `GET` | `/api/analytics` | Per-endpoint breakdown, percentiles, geo distribution |
| `GET` | `/api/metrics` | Throughput history, processing latency, error rates |
| `GET` | `/health` | Health check (consumer running, partitions, Redis) |
| `WS`  | `/ws` | WebSocket stream of live stats (1s interval) |

### Example: `/api/stats` response

```json
{
  "consumer": {
    "is_running": true,
    "total_consumed": 1234,
    "total_committed": 1234,
    "total_errors": 0,
    "batches_processed": 13,
    "uptime_seconds": 120.5,
    "throughput": 10.28,
    "assigned_partitions": 9,
    "current_batch_size": 42
  },
  "processor": {
    "total_processed": 1234,
    "total_failed": 2,
    "success_rate": 99.84,
    "web_count": 800,
    "app_count": 300,
    "error_count": 134
  },
  "analytics": {
    "total_messages": 1234,
    "total_errors": 50,
    "error_rate": 4.05,
    "throughput_per_sec": 10.28,
    "window_seconds": 60,
    "consumer_lag": {"partition-0": 0, "partition-1": 12},
    "total_lag": 12,
    "high_lag_alert": false
  }
}
```

## Project Structure

```
kafka-log-consumer-with-analytics/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.test
├── requirements.txt
├── README.md
├── src/
│   ├── __init__.py
│   ├── main.py              # Entry point — wires components, runs uvicorn
│   ├── config.py             # Settings dataclass, env var loading
│   ├── models.py             # Pydantic log models + parser
│   ├── consumer.py           # LogConsumer with retry, DLQ, throttling
│   ├── batch_processor.py    # Batch parsing and routing
│   ├── analytics.py          # Sliding window analytics engine
│   ├── redis_store.py        # Redis snapshot persistence
│   ├── producer.py           # Test log producer (for demos)
│   ├── dashboard.py          # FastAPI app with REST + WebSocket
│   └── websocket_manager.py  # WebSocket connection manager
├── templates/
│   └── index.html            # Dashboard web UI
├── scripts/
│   ├── create_topics.sh      # Kafka topic initialization
│   ├── wait_for_kafka.py     # Kafka readiness check
│   └── verify_e2e.py         # End-to-end verification script
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_models.py
    ├── test_config.py
    ├── test_consumer.py
    ├── test_batch_processor.py
    ├── test_analytics.py
    ├── test_dashboard.py
    ├── test_producer.py
    ├── test_redis_store.py
    └── test_error_handling.py
```

## Error Handling Features

- **Exponential backoff retry** -- Failed batches are retried up to 3 times with delays of 1s, 2s, and 4s before giving up.
- **Dead letter queue** -- Messages that fail all retries are forwarded to the `dead-letter-logs` Kafka topic with error metadata headers.
- **Schema evolution** -- Malformed or partial JSON payloads are gracefully handled: parseable fields get defaults, unparseable messages increment failure counters without crashing.
- **Dynamic throttling** -- When batch processing exceeds 500ms, the consumer automatically adds a proportional pause before the next poll cycle to avoid overwhelming downstream systems.
- **Consumer lag alerting** -- The analytics engine tracks per-partition lag and raises a high-lag alert when total lag exceeds 10,000 messages.

## What I Learned

- **Manual offset management** is essential for at-least-once delivery guarantees. Committing after successful processing (not after polling) ensures no messages are lost on crashes, at the cost of possible reprocessing.
- **Batch processing trade-offs**: larger batches improve throughput but increase latency and memory usage. A time-based flush timeout prevents stale partial batches from sitting idle.
- **Consumer group rebalancing** requires careful handling -- flushing in-progress batches during `on_revoke` prevents data loss when partitions move between consumers.
- **Sliding window analytics** using pre-aggregated second buckets is far more efficient than storing every individual timestamp, especially at high message rates.
- **Dead letter queues** are critical in production. Rather than dropping or retrying forever, routing persistently-failing messages to a separate topic allows the main consumer to keep making progress while failures can be investigated offline.
- **Dynamic throttling** (backpressure) prevents a slow downstream from causing the consumer to fall further and further behind -- a simple proportional delay on slow batches is surprisingly effective.
- **WebSocket broadcasting** from a background asyncio task gives the dashboard sub-second update latency without polling, but requires careful thread safety between the Kafka consumer thread and the async FastAPI event loop.

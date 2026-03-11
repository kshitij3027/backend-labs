# log-consumer-system

A horizontally-scalable log consumer system that pulls messages from a Redis stream, processes web server access logs to extract metrics, and exposes real-time statistics via a monitoring dashboard.

## How It Works

Long-lived process with an embedded FastAPI server — multiple async consumer workers poll a Redis stream continuously while a dashboard API serves real-time stats at `http://localhost:8000`.

### Architecture

```
┌─────────────────┐
│   Redis Stream   │
│  (access logs)   │
└────────┬────────┘
         │ XREADGROUP
         ▼
┌─────────────────────────────────┐
│     Consumer Process            │
│                                 │
│  ┌───────────┐ ┌───────────┐   │
│  │ Worker 1  │ │ Worker N  │   │
│  │ (async)   │ │ (async)   │   │
│  └─────┬─────┘ └─────┬─────┘   │
│        │              │         │
│        ▼              ▼         │
│  ┌──────────────────────────┐   │
│  │   Metrics Aggregator     │   │
│  │  (in-memory stats)       │   │
│  └────────────┬─────────────┘   │
│               │                 │
│  ┌────────────▼─────────────┐   │
│  │   FastAPI Dashboard API  │   │
│  │   http://localhost:8000  │   │
│  └──────────────────────────┘   │
└─────────────────────────────────┘
```

### Key Features

- **Consumer groups** — multiple workers in a consumer group for parallel log processing with automatic message acknowledgement
- **Access log parsing** — extracts IP, method, path, status code, response size, and latency from common/combined log formats
- **Real-time metrics** — requests per second, status code distribution, top paths, top IPs, error rates, p50/p95/p99 latencies
- **Horizontal scaling** — run multiple consumer instances that share work via Redis consumer groups
- **Graceful shutdown** — handles SIGINT/SIGTERM, drains in-flight messages, and cleanly disconnects from Redis
- **Dashboard API** — FastAPI endpoints serving live stats with auto-refreshing HTML dashboard
- **Idempotency** — `SET NX` guard with configurable TTL prevents duplicate processing of the same message across restarts or redeliveries
- **Ordered processing** — optional ordering mode routes messages with an `ordering_key` field to a deterministic worker via consistent hashing, so all events for the same key are processed by the same consumer
- **XCLAIM recovery** — on startup, each consumer scans for messages idle beyond `claim_idle_ms` in the pending list and claims them from dead consumers using `XPENDING` + `XCLAIM`
- **Dead letter queue** — messages that exhaust retries are forwarded to a DLQ stream with full error context for later inspection
- **Retry with exponential backoff** — configurable retry count with jittered exponential backoff to avoid thundering herd on transient failures

## Tech Stack

- **Language**: Python 3.11+
- **Async framework**: asyncio
- **Web framework**: FastAPI + Uvicorn
- **Message broker**: Redis Streams (via redis.asyncio)
- **Log parsing**: Custom parser for Apache/Nginx combined log format
- **Metrics**: In-memory aggregation with sliding windows

## How to Run

### Prerequisites

- Docker and Docker Compose installed

### Quick Start

```bash
# Build and start all services
docker compose up --build

# Dashboard available at
open http://localhost:8000

# Scale consumers horizontally
docker compose up --scale consumer=3
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis:6379` | Redis connection URL |
| `STREAM_KEY` | `logs:access` | Redis stream key to consume from |
| `CONSUMER_GROUP` | `log-processors` | Consumer group name |
| `CONSUMER_NAME` | `consumer-{hostname}` | Unique consumer name within the group |
| `NUM_WORKERS` | `4` | Number of async worker tasks per process |
| `BATCH_SIZE` | `100` | Messages to read per XREADGROUP call |
| `BLOCK_MS` | `2000` | Block timeout for XREADGROUP (ms) |
| `DASHBOARD_PORT` | `8000` | Port for the FastAPI dashboard |
| `METRICS_WINDOW_SEC` | `300` | Sliding window size for rate metrics (seconds) |
| `MAX_RETRIES` | `3` | Max retry attempts before sending to DLQ |
| `RETRY_BASE_DELAY` | `1.0` | Base delay in seconds for exponential backoff |
| `RETRY_MAX_DELAY` | `30.0` | Maximum backoff delay in seconds |
| `DLQ_STREAM_KEY` | `logs:dlq` | Redis stream key for dead letter queue |
| `IDEMPOTENCY_TTL` | `3600` | TTL in seconds for idempotency keys |
| `ENABLE_ORDERING` | `false` | Route messages by ordering_key to consistent workers |
| `CLAIM_IDLE_MS` | `30000` | Idle threshold (ms) before claiming abandoned messages |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Auto-refreshing HTML dashboard |
| `/api/stats` | GET | JSON snapshot of all current metrics |
| `/api/stats/requests` | GET | Request rate and total count |
| `/api/stats/status-codes` | GET | Status code distribution |
| `/api/stats/top-paths` | GET | Top N requested paths |
| `/api/stats/top-ips` | GET | Top N client IPs |
| `/api/stats/latency` | GET | Latency percentiles (p50/p95/p99) |
| `/api/stats/errors` | GET | Error rate and recent errors |
| `/health` | GET | Consumer health check |

## What I Learned

- **Redis Streams consumer groups** — XREADGROUP delivers each message to exactly one consumer in the group, giving you parallel processing without an external partitioning layer. The `>` ID reads new messages while `0` replays your own pending entries on restart.
- **Idempotency with `SET NX`** — a single atomic Redis command (`SET key 1 NX EX ttl`) acts as a deduplication gate. If the key already exists the message was already processed, so we skip and ACK. The TTL keeps the keyspace bounded.
- **XCLAIM for dead-consumer recovery** — `XPENDING` lists messages stuck in other consumers' pending lists; `XCLAIM` transfers ownership to a live consumer once the idle time exceeds a threshold, preventing message loss when a process crashes without ACKing.
- **Ordered processing via consistent hashing** — hashing an `ordering_key` to a worker index ensures all events for the same logical entity hit the same consumer, preserving causal order without giving up parallelism across different keys.
- **Exponential backoff with jitter** — `base * 2^attempt + random(0, 0.5)` spreads retries over time and avoids thundering-herd spikes when many messages fail simultaneously.
- **Dead letter queues** — after exhausting retries, forwarding the message (with full error context) to a separate DLQ stream lets operators inspect failures without blocking the main pipeline.
- **Sliding-window metrics** — keeping timestamped entries in a deque and evicting anything older than the window gives O(1) amortised inserts and accurate rate calculations.
- **Graceful shutdown** — catching SIGINT/SIGTERM, setting an `asyncio.Event`, and awaiting in-flight coroutines prevents half-processed messages and unclean Redis state.

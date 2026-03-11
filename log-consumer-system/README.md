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

- How Redis Streams and consumer groups enable scalable, fault-tolerant message consumption
- Building long-lived async Python processes that combine background workers with an HTTP API
- Implementing sliding-window metrics aggregation for real-time monitoring
- Graceful shutdown patterns for async consumer processes
- Horizontal scaling patterns using consumer groups for work distribution

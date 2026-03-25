# Time-Based Window Log Analytics

A system that groups incoming log events into fixed time windows (5-minute, hourly, daily) and computes real-time aggregated metrics with a live dashboard.

## How It Runs

Long-lived server process with a FastAPI-based REST/WebSocket API, backed by Redis for state persistence, serving a real-time analytics dashboard and accepting log events continuously.

## Tech Stack

- **Language**: Python 3.11+
- **Web Framework**: FastAPI (REST + WebSocket)
- **State Store**: Redis (window counters, sorted sets, expiring keys)
- **Dashboard**: HTML/JS served by FastAPI, live updates via WebSocket
- **Task Scheduling**: APScheduler (window rotation and flush)
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
- **5-minute windows**: Fine-grained, real-time view of recent activity
- **Hourly windows**: Medium-term trend analysis
- **Daily windows**: Long-term pattern detection

### Aggregated Metrics (per window)
- Total log count
- Counts by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Counts by source/service
- Error rate percentage
- Top error messages

### Redis Data Model
- Window counters stored as Redis hashes with TTL-based expiry
- Sorted sets for top-N queries
- Pub/Sub for real-time dashboard push

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/logs` | Ingest a single log event |
| POST | `/api/v1/logs/batch` | Ingest a batch of log events |
| GET | `/api/v1/windows/{type}` | Get current window metrics (type: 5m, 1h, 1d) |
| GET | `/api/v1/windows/{type}/history` | Get historical window summaries |
| GET | `/api/v1/stats` | Get overall system statistics |
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
  }
}
```

## How to Run

```bash
# Build and start all services
docker-compose up --build

# Server available at http://localhost:8000
# Dashboard at http://localhost:8000/dashboard
# API docs at http://localhost:8000/docs
```

## Testing

```bash
# Run all tests in Docker
docker-compose run --rm app pytest -v

# Run with coverage
docker-compose run --rm app pytest --cov=src -v
```

## What I Learned

- Designing time-window aggregation with Redis hash keys and TTLs
- Real-time push to browser clients using FastAPI WebSockets
- Efficient batch ingestion and pipeline processing
- Using APScheduler for periodic window rotation alongside an async web server
- Building a live-updating dashboard with Chart.js and WebSocket

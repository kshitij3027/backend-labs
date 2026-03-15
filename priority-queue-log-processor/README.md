# Priority Queue Log Processor

A multi-tier priority queue system that classifies incoming log messages by criticality and processes them in priority order with real-time monitoring via a web dashboard.

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** Flask (HTTP API + web dashboard)
- **Concurrency:** Threading (background worker threads)
- **Queue:** Python `queue.PriorityQueue` (thread-safe)
- **Frontend:** HTML/CSS/JavaScript (dashboard)

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│  Load Generator  │────▶│  Flask API (:8080)    │────▶│  Priority Queue  │
│  (synthetic logs)│     │  - POST /logs         │     │  (multi-tier)    │
└─────────────────┘     │  - GET /dashboard     │     └────────┬─────────┘
                        │  - GET /api/stats     │              │
                        └──────────────────────┘     ┌────────▼─────────┐
                                                     │  Worker Threads   │
                                                     │  (process by      │
                                                     │   priority order) │
                                                     └──────────────────┘
```

### Priority Tiers

| Priority | Level    | Description                          |
|----------|----------|--------------------------------------|
| 0        | CRITICAL | System failures, data loss           |
| 1        | ERROR    | Application errors, exceptions       |
| 2        | WARNING  | Degraded performance, retries        |
| 3        | INFO     | Normal operations, status updates    |
| 4        | DEBUG    | Verbose debugging information        |

### Components

1. **Priority Queue Engine** — Multi-tier thread-safe priority queue that orders log messages by criticality level
2. **Log Classifier** — Analyzes incoming log messages and assigns priority tiers based on content and metadata
3. **Worker Pool** — Concurrent background threads that dequeue and process messages in priority order
4. **Flask API & Dashboard** — HTTP API for log ingestion (`POST /logs`), stats (`GET /api/stats`), and a real-time web dashboard on port 8080
5. **Load Generator** — Synthetic log producer for testing under various load patterns

## How to Run

```bash
# Build and run with Docker
docker build -t priority-queue-log-processor .
docker run -p 8080:8080 priority-queue-log-processor

# Or run locally
pip install -r requirements.txt
python main.py
```

- Dashboard: http://localhost:8080/dashboard
- Submit logs: `POST http://localhost:8080/logs`
- View stats: `GET http://localhost:8080/api/stats`

## API Endpoints

| Method | Endpoint        | Description                        |
|--------|-----------------|------------------------------------|
| POST   | `/logs`         | Submit a log message for processing|
| GET    | `/api/stats`    | Queue and processing statistics    |
| GET    | `/dashboard`    | Real-time monitoring dashboard     |

### Example: Submit a Log

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"message": "Database connection failed", "source": "db-service", "level": "CRITICAL"}'
```

## What I Learned

_(To be filled in after implementation)_

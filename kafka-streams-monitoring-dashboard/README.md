# Kafka Streams Monitoring Dashboard

A stream processing engine that consumes Kafka topics, aggregates metrics in configurable time windows, and serves them to a live web dashboard over WebSockets.

## How It Runs

Long-lived server process — a Flask web app with a background stream-processing loop that continuously polls Kafka, updates in-memory metrics, and pushes updates to connected browser clients every 2 seconds.

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** Flask + Flask-SocketIO
- **Kafka Client:** confluent-kafka
- **WebSockets:** python-socketio / gevent
- **Frontend:** HTML/CSS/JS with Chart.js for live metric visualization
- **Containerization:** Docker + Docker Compose (Kafka + Zookeeper + App)

## Architecture

```
┌──────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
│  Kafka Topic  │────▶│  Stream Processor Loop   │────▶│  In-Memory Store │
└──────────────┘     │  (background thread)      │     │  (metrics/aggs)  │
                     └─────────────────────────┘     └────────┬─────────┘
                                                               │
                                                               ▼
                     ┌─────────────────────────┐     ┌──────────────────┐
                     │  Browser (Chart.js)      │◀───│  Flask-SocketIO  │
                     │  Live Dashboard          │    │  WebSocket Push  │
                     └─────────────────────────┘     └──────────────────┘
```

### Key Components

1. **Kafka Consumer** — Polls one or more Kafka topics for incoming messages (log events, metrics, etc.)
2. **Stream Processor** — Aggregates consumed messages into configurable time windows (tumbling/sliding) and computes metrics (count, avg, min, max, percentiles)
3. **In-Memory Metrics Store** — Holds windowed aggregations; old windows are evicted after expiry
4. **WebSocket Server** — Pushes metric snapshots to all connected dashboard clients every 2 seconds
5. **Web Dashboard** — Single-page HTML/JS app with live-updating charts powered by Chart.js

## Features

- Configurable time windows (e.g., 10s, 30s, 1m, 5m)
- Multiple aggregation functions (count, sum, avg, min, max, p95, p99)
- Topic and field-level filtering
- Live WebSocket push to browser clients
- Responsive dashboard with multiple chart types
- Graceful shutdown with consumer group rebalancing

## How to Run

```bash
# Start Kafka infrastructure + app
docker-compose up --build

# Dashboard available at
http://localhost:5000
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPICS` | `logs` | Comma-separated list of topics to consume |
| `KAFKA_GROUP_ID` | `dashboard-consumer` | Consumer group ID |
| `WINDOW_SIZES` | `10,30,60` | Window sizes in seconds |
| `PUSH_INTERVAL` | `2` | WebSocket push interval in seconds |
| `FLASK_PORT` | `5000` | Dashboard server port |

## What I Learned

_(To be filled in as the project progresses)_

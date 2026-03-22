# Kafka Streams Monitoring Dashboard

Real-time monitoring dashboard that consumes Kafka topics, aggregates metrics in configurable time windows, and pushes live updates to a web UI over WebSockets.

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** Flask + Flask-SocketIO (gevent async mode)
- **Kafka Client:** confluent-kafka (librdkafka-based)
- **Frontend:** HTML/CSS/JS with Chart.js for live charts
- **WebSockets:** python-socketio / gevent-websocket
- **Containerization:** Docker + Docker Compose (Kafka + Zookeeper + App)

## Architecture

```
┌────────────────┐     ┌──────────────┐     ┌──────────────────┐
│  Data Generator │────▶│    Kafka      │────▶│  Kafka Consumer  │
│  (producer)     │     │  (3 topics)   │     │  (background     │
└────────────────┘     └──────────────┘     │   thread)         │
                                             └────────┬─────────┘
                                                      │
                                                      ▼
                       ┌──────────────────────────────────────────┐
                       │         Stream Processor                  │
                       │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
                       │  │ Metrics  │ │ Business │ │   Geo    │ │
                       │  │  Store   │ │ Metrics  │ │ Analyzer │ │
                       │  └────┬─────┘ └──────────┘ └──────────┘ │
                       └───────┼──────────────────────────────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
                ▼              ▼              ▼
         ┌────────────┐ ┌──────────┐ ┌──────────────┐
         │ Flask REST │ │ SocketIO │ │   Derived    │
         │    APIs    │ │   Push   │ │   Metrics    │
         └────────────┘ └────┬─────┘ │  (producer)  │
                              │       └──────────────┘
                              ▼
                       ┌────────────┐
                       │  Browser   │
                       │ (Chart.js) │
                       └────────────┘
```

**Data flow:** The data generator produces synthetic log, error, and user events to three Kafka topics. A background consumer thread reads these messages and routes them through the stream processor, which updates the in-memory metrics store, business metrics tracker, and geo analyzer. A Flask app serves REST APIs and a live dashboard. A WebSocket background task pushes metric snapshots to all connected browser clients every 2 seconds. An alert manager evaluates thresholds on each push cycle. Derived/aggregated metrics are also produced back to Kafka.

## How to Run

```bash
# Start everything (Kafka + Zookeeper + App + Data Generator)
docker compose up --build -d

# Dashboard available at
open http://localhost:5050

# Run unit tests
docker compose --profile test run --rm test

# Run E2E verification
docker compose --profile e2e run --rm e2e

# Tear down
docker compose --profile e2e --profile test down -v
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Dashboard HTML page with live charts |
| `/health` | GET | Health check (`{"status": "healthy"}`) |
| `/api/metrics` | GET | Current windowed metrics (totals, error rate, response times, EPS) |
| `/api/historical` | GET | Time-bucketed arrays for Chart.js (labels, events, error_rate, response_times) |
| `/api/alerts` | GET | Active alerts and alert history |
| `/api/business-metrics` | GET | API version distribution, payment funnel counts, auth success/failure rates |
| `/api/geo` | GET | Traffic counts and average latency by geographic region |

## WebSocket Events

| Event | Direction | Payload |
|---|---|---|
| `metrics_update` | Server -> Client | `{metrics, historical, business_metrics, geo}` -- pushed every 2s |
| `alert_update` | Server -> Client | `{alerts: [...]}` -- pushed when new alerts fire |
| `connect` | Client -> Server | On connect, server sends initial `metrics_update` |

## Configuration

All settings are configurable via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_GROUP_ID` | `dashboard-consumer` | Consumer group ID |
| `AUTO_OFFSET_RESET` | `earliest` | Where to start consuming |
| `WINDOW_SECONDS` | `60` | Aggregation window size (seconds) |
| `DEQUE_MAX_LENGTH` | `1000` | Max events held in memory |
| `WS_EMIT_INTERVAL` | `2.0` | WebSocket push interval (seconds) |
| `POLL_TIMEOUT_S` | `1.0` | Kafka poll timeout (seconds) |
| `DASHBOARD_HOST` | `0.0.0.0` | Flask bind address |
| `DASHBOARD_PORT` | `5000` | Flask listen port (mapped to 5050 on host) |
| `ALERT_ERROR_RATE_WARNING` | `3.0` | Error rate % warning threshold |
| `ALERT_ERROR_RATE_CRITICAL` | `5.0` | Error rate % critical threshold |
| `ALERT_RESPONSE_TIME_WARNING` | `1000.0` | P95 response time ms warning threshold |
| `ALERT_RESPONSE_TIME_CRITICAL` | `2000.0` | P95 response time ms critical threshold |
| `ALERT_COOLDOWN_SECONDS` | `60.0` | Minimum seconds between duplicate alerts |

## Features

**Core:**
- Real-time Kafka topic consumption with manual offset commits
- Configurable tumbling-window metric aggregation (count, avg, p95, error rate, EPS)
- Live WebSocket push to all connected dashboard clients
- Responsive Chart.js dashboard with auto-updating charts
- Derived metrics produced back to a Kafka topic

**Extended:**
- Threshold-based alerting with warning/critical severity and cooldown periods
- Business metrics: API version distribution, payment funnel tracking, auth success/failure rates
- Geographic traffic analysis with deterministic IP-to-region mapping and per-region latency
- Data generator producing realistic synthetic log, error, and user events
- Full Docker Compose setup with health checks and topic auto-creation

## What I Learned

- **gevent monkey-patching** must happen before any other imports, and `thread=False` is required to keep confluent-kafka's C extension threads working correctly.
- **confluent-kafka vs kafka-python:** confluent-kafka (librdkafka) is significantly faster but requires careful threading since its C internals use real OS threads that gevent cannot patch.
- **WebSocket backpressure:** Pushing data to many clients every 2 seconds requires efficient serialization. Flask-SocketIO with gevent async mode handles this well without blocking the main thread.
- **Windowed aggregation design:** Using a bounded deque with timestamp-based filtering provides a simple but effective sliding window without needing a full stream processing framework.
- **Alert cooldowns** prevent alert storms when metrics hover near thresholds. A simple timestamp-based cooldown per alert key is sufficient for most monitoring use cases.
- **Docker health checks** with `depends_on: condition: service_healthy` are essential for ensuring services start in the correct order, especially when Kafka needs time to initialize.

# Real-Time Analytics Dashboard

A WebSocket-streaming analytics dashboard that ingests log metrics, performs statistical anomaly detection and trend analysis, and renders interactive visualizations via a browser-based UI.

## Tech Stack

- **Language**: Python 3.11
- **Web Framework**: FastAPI (REST + WebSocket)
- **Time-Series Store**: Redis 7 (sorted sets with TTL)
- **Anomaly Detection**: NumPy / SciPy (Z-score statistical methods)
- **Trend Analysis**: NumPy (linear regression)
- **Frontend**: Plotly.js interactive charts, vanilla HTML/CSS/JS
- **Containerization**: Docker + Docker Compose

## Architecture

```
┌────────────┐       ┌──────────────────────────────────┐       ┌───────────┐
│  Log Metric│──REST─▶│         FastAPI Backend           │◀─────▶│   Redis   │
│  Producers │  POST  │                                    │       │ (cache +  │
└────────────┘       │  ┌────────────┐  ┌──────────────┐ │       │  pub/sub) │
                      │  │ Ingestion  │  │  Anomaly     │ │       └───────────┘
                      │  │ Pipeline   │──▶│  Detection   │ │
                      │  └────────────┘  └──────┬───────┘ │
                      │                         │          │
                      │  ┌──────────────────────▼───────┐ │
                      │  │  Trend Analysis / Aggregation │ │
                      │  └──────────────────────┬───────┘ │
                      │                         │          │
                      │                    WebSocket       │
                      │                    Stream          │
                      └──────────────────────┬─────────────┘
                                             │
                                      ┌──────▼──────┐
                                      │  Browser UI  │
                                      │  Dashboard   │
                                      └─────────────┘
```

### Key Components

1. **Ingestion API** -- REST endpoint accepting log metrics (JSON payloads with timestamp, service, metric name, value).
2. **Metric Store** -- Redis-backed storage using sorted sets for time-series queries with TTL-based expiry.
3. **Anomaly Detection Engine** -- Z-score based statistical outlier detection running on each ingest batch.
4. **Trend Analyzer** -- Linear regression over sliding windows computing direction (increasing/decreasing/stable) and slope.
5. **WebSocket Streamer** -- Pushes metric updates, anomaly alerts, and heartbeat pings to connected dashboard clients.
6. **Dashboard UI** -- Single-page browser app with Plotly.js interactive charts, anomaly highlights, and auto-refresh.

## How to Run

```bash
# Build and start all services
docker compose up --build -d

# Open dashboard
open http://localhost:8000

# API docs (Swagger)
open http://localhost:8000/docs

# Run unit tests
make test

# Run E2E verification
make e2e

# Run load test
make load

# View logs
make logs

# Stop and clean up
make clean
```

## API Reference

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe returning Redis connectivity status |
| `POST` | `/api/ingest` | Ingest raw log entries, extract metrics, detect anomalies |
| `POST` | `/api/generate-sample-data` | Generate and store sample metric data |
| `GET` | `/api/metrics/{service}/{metric_name}` | Query stored metrics with trend analysis |
| `GET` | `/api/anomalies` | Query detected anomalies |
| `GET` | `/api/services` | List all known service names |
| `GET` | `/api/export` | Export metrics as CSV or JSON download |
| `GET` | `/api/ws-status` | Active WebSocket connection info |
| `WS` | `/ws` | WebSocket endpoint for real-time streaming |

### Example Requests

**Health check:**
```bash
curl http://localhost:8000/health
# {"status":"healthy","redis_connected":true}
```

**Ingest logs:**
```bash
curl -X POST http://localhost:8000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "logs": [{
      "timestamp": 1713100000,
      "service": "web-api",
      "response_time": 250.5,
      "method": "GET",
      "endpoint": "/api/users",
      "level": "INFO"
    }]
  }'
# {"ingested":1,"metrics_stored":3,"services":["web-api"],"anomalies_detected":0}
```

**Generate sample data:**
```bash
curl -X POST "http://localhost:8000/api/generate-sample-data?service=web-api&count=50"
# {"logs_generated":50,"metrics_stored":150,"services":["web-api"]}
```

**Query metrics with trend:**
```bash
curl "http://localhost:8000/api/metrics/web-api/response_time?minutes=10&include_trend=true"
# {"service":"web-api","metric_name":"response_time","data_points":[...],"count":50,"trend":{"direction":"stable","slope":0.02,"r_squared":0.85}}
```

**Query anomalies:**
```bash
curl "http://localhost:8000/api/anomalies?hours=1&service=web-api"
# {"anomalies":[...],"count":2,"hours":1.0}
```

**List services:**
```bash
curl http://localhost:8000/api/services
# {"services":["web-api","auth-service"]}
```

**Export as CSV:**
```bash
curl "http://localhost:8000/api/export?service=web-api&metric_name=response_time&format=csv"
# timestamp,value,service,metric_name,tags
# 1713100000.0,250.5,web-api,response_time,...
```

**Export as JSON:**
```bash
curl "http://localhost:8000/api/export?service=web-api&metric_name=response_time&format=json"
# {"service":"web-api","metric_name":"response_time","minutes":60.0,"data_points":[...],"count":50}
```

### WebSocket Protocol

Connect to `ws://localhost:8000/ws` for real-time streaming.

**Message flow:**

1. **Server sends connected message** on connection:
   ```json
   {"type": "connected", "client_id": "abc123"}
   ```

2. **Client subscribes** to streams (`metrics`, `alerts`):
   ```json
   {"type": "subscribe", "streams": ["metrics", "alerts"]}
   ```

3. **Server confirms** subscription:
   ```json
   {"type": "subscribed", "streams": ["alerts", "metrics"]}
   ```

4. **Server pushes metric updates** periodically:
   ```json
   {"type": "metrics_update", "data": {...}, "timestamp": 1713100000}
   ```

5. **Server pushes anomaly alerts** when detected:
   ```json
   {"type": "alert", "data": {"service": "web-api", "metric_name": "response_time", "value": 950.2, "z_score": 3.5}}
   ```

6. **Server sends heartbeat pings**, client replies with pong:
   ```json
   {"type": "ping"}
   ```
   ```json
   {"type": "pong"}
   ```

7. **Client unsubscribes** from streams:
   ```json
   {"type": "unsubscribe", "streams": ["alerts"]}
   ```

## Configuration

All settings are configurable via environment variables. See `.env.example` for defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `redis` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `SERVER_HOST` | `0.0.0.0` | FastAPI bind address |
| `SERVER_PORT` | `8000` | FastAPI bind port |
| `ANOMALY_ZSCORE_THRESHOLD` | `2.5` | Z-score threshold for flagging anomalies |
| `TREND_WINDOW_MINUTES` | `5` | Sliding window size (minutes) for trend analysis |
| `METRIC_TTL_SECONDS` | `3600` | TTL for stored metrics in Redis (seconds) |
| `WS_HEARTBEAT_INTERVAL` | `30` | Seconds between WebSocket heartbeat pings |
| `WS_BROADCAST_INTERVAL` | `5` | Seconds between metric broadcast pushes |

## Testing

### Unit Tests

```bash
make test
# Runs pytest inside Docker against a real Redis instance
```

### End-to-End Verification

```bash
make e2e
# Spins up app + Redis, then runs scripts/verify_e2e.py which tests:
#   - Health check with retry
#   - Sample data generation for multiple services
#   - Metric querying with trend analysis
#   - Anomaly detection endpoint
#   - Service listing
#   - CSV and JSON export
#   - WebSocket connect and subscribe
```

### Load Test

```bash
make load
# Runs scripts/load_test.py which sends a mixed workload:
#   - 50% ingest requests
#   - 30% metric queries
#   - 20% health checks
# Reports throughput, p50/p95/p99 latency, and error rate
# Pass/fail threshold: <5% error rate
```

## Project Structure

```
real-time-analytics/
├── docker-compose.yml          # Multi-service orchestration
├── Dockerfile                  # Production app image
├── Dockerfile.test             # Test/E2E/loadtest image
├── Makefile                    # Build, test, run shortcuts
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable defaults
├── src/
│   ├── main.py                 # FastAPI app, all routes and WebSocket
│   ├── config.py               # Environment-based configuration
│   ├── models.py               # Pydantic request/response models
│   ├── storage.py              # Redis sorted-set time-series storage
│   ├── ingestion.py            # Log parsing and metric extraction
│   ├── websocket.py            # WebSocket connection manager + background loops
│   └── engine/
│       ├── anomalies.py        # Z-score anomaly detection
│       └── trends.py           # Linear regression trend analysis
├── static/
│   └── index.html              # Dashboard UI (Plotly.js charts)
├── scripts/
│   ├── verify_e2e.py           # End-to-end verification script
│   └── load_test.py            # Load testing script
└── tests/
    ├── test_anomalies.py       # Anomaly detection unit tests
    ├── test_config.py          # Configuration unit tests
    ├── test_ingestion.py       # Ingestion pipeline unit tests
    ├── test_main.py            # API route integration tests
    ├── test_storage.py         # Redis storage unit tests
    ├── test_trends.py          # Trend analysis unit tests
    └── test_websocket.py       # WebSocket unit tests
```

## What I Learned

- **Real-time WebSocket streaming architecture** -- Designing a bidirectional WebSocket protocol with subscribe/unsubscribe semantics, heartbeat keep-alive, and background broadcast loops that push aggregated metrics on a configurable interval.

- **Redis sorted sets for time-series data** -- Using Redis ZADD/ZRANGEBYSCORE with timestamp-based scores for efficient time-range queries, combined with TTL-based key expiry to bound memory usage without manual cleanup.

- **Statistical anomaly detection with Z-score** -- Implementing real-time outlier detection by computing Z-scores against rolling statistics (mean and standard deviation), with configurable sensitivity thresholds per metric stream.

- **Linear regression for trend analysis** -- Using NumPy's polyfit for least-squares linear regression over sliding time windows to determine metric direction (increasing/decreasing/stable) with R-squared confidence scoring.

- **Plotly.js for interactive dashboards** -- Building a single-page dashboard with live-updating time-series charts, anomaly scatter overlays, and auto-refresh driven by both WebSocket pushes and periodic polling.

- **Docker multi-service orchestration** -- Structuring a docker-compose setup with health checks, profile-based test/E2E/loadtest containers, resource limits, and proper service dependency ordering for reliable CI pipelines.

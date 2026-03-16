# Kafka Log Producer

A high-performance Kafka producer that ingests log entries from multiple services, applies smart batching/partitioning/routing, and exposes a real-time web dashboard with Prometheus metrics.

## Tech Stack

- **Language:** Python 3.12
- **Kafka Client:** confluent-kafka (librdkafka-based, high performance)
- **Web Framework:** FastAPI + Uvicorn
- **Real-time Updates:** WebSocket (via FastAPI)
- **Metrics:** Prometheus client library (exposed on port 8000)
- **Dashboard:** Jinja2 templates + vanilla JS with Canvas charts
- **Serialization:** JSON
- **CLI:** Typer + Rich
- **Testing:** pytest, pytest-asyncio, httpx
- **Containerization:** Docker, Docker Compose (Kafka + ZooKeeper + App)

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Server (:8080)                │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────────┐  │
│  │   REST API   │  │  WebSocket │  │  Web Dashboard  │  │
│  │ /api/send-*  │  │    /ws     │  │       /         │  │
│  └──────┬───────┘  └─────┬──────┘  └─────────────────┘  │
│         │                │                               │
│  ┌──────▼────────────────▼──────────────────────────┐    │
│  │           KafkaLogProducer                        │    │
│  │  ┌──────────┐  ┌──────────────┐  ┌────────────┐  │    │
│  │  │  Router  │  │  Partitioner │  │  Callbacks  │  │    │
│  │  └──────────┘  └──────────────┘  └────────────┘  │    │
│  └──────────────────────┬───────────────────────────┘    │
│                         │                                │
│  ┌──────────────────────▼───────────────────────────┐    │
│  │    confluent-kafka Producer                       │    │
│  │  (gzip, acks=all, idempotent, 16KB batch, 5ms)   │    │
│  └──────────────────────┬───────────────────────────┘    │
│                         │                                │
│  ┌──────────────────────▼──────┐  ┌──────────────────┐   │
│  │  Prometheus Metrics (:8000) │  │ Fallback Storage │   │
│  │  messages_sent_total        │  │ (disk buffer)    │   │
│  │  send_latency_seconds       │  └──────────────────┘   │
│  │  buffer_available_bytes     │                         │
│  └─────────────────────────────┘                         │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  Kafka Broker   │
                 │  (single node)  │
                 └─────────────────┘
                    4 topics:
              logs-application
              logs-database
              logs-errors
              logs-security
```

## Features

### Core Producer
- **Smart Batching** — 16KB batch size + 5ms linger for throughput optimization
- **Topic Routing** — ERROR/CRITICAL → `logs-errors`, database services → `logs-database`, auth/security → `logs-security`, everything else → `logs-application`
- **Partition Keys** — priority chain: `user_id > session_id > service` for message ordering
- **Compression** — gzip compression on all messages
- **Reliability** — idempotent producer, acks=all, infinite retries
- **Fallback Storage** — disk-based JSONL buffer when Kafka is unavailable, auto-replay on reconnect

### REST API
- `POST /api/send-sample` — generate and send 10 sample logs
- `POST /api/send-error-burst` — generate and send 5 error/critical logs
- `GET /api/stats` — current producer statistics
- `GET /health` — liveness/readiness probe

### Dashboard & Monitoring
- **Web Dashboard** (port 8080) — dark-themed UI with stat cards, per-topic table, throughput chart
- **WebSocket** (`/ws`) — live stats pushed every 2 seconds
- **Prometheus Metrics** (port 8000) — `messages_sent_total`, `send_latency_seconds`, `buffer_available_bytes`

### CLI
- `python -m src.main server` — start the FastAPI dashboard
- `python -m src.main demo --count 100 --rate 10` — send sample logs at a configurable rate
- `python -m src.main performance 60 1000` — sustained throughput test

## How to Run

### Prerequisites
- Docker and Docker Compose installed

### Quick Start
```bash
cd kafka-log-producer
make up          # Start ZooKeeper + Kafka + create topics
docker compose up -d app   # Start the app
```

### Full Stack
```bash
docker compose up --build
```

### Services
| Service     | Port  | Description                    |
|-------------|-------|--------------------------------|
| App         | 8080  | FastAPI dashboard + REST API   |
| Prometheus  | 8000  | Prometheus metrics endpoint    |
| Kafka       | 9092  | Kafka broker (external)        |
| ZooKeeper   | 2181  | Kafka coordination             |

### Testing
```bash
make test        # Unit tests (41 tests)
make e2e         # End-to-end verification (11 checks)
make benchmark   # Throughput benchmark
```

### API Usage

**Send sample logs:**
```bash
curl -X POST http://localhost:8080/api/send-sample
# {"logs_sent": 10, "logs_failed": 0, "sent": 10, "failed": 0}
```

**Send error burst:**
```bash
curl -X POST http://localhost:8080/api/send-error-burst
# {"logs_sent": 5, "logs_failed": 0, "sent": 5, "failed": 0}
```

**Get stats:**
```bash
curl http://localhost:8080/api/stats
```

**Dashboard:** Open `http://localhost:8080` in a browser.

**WebSocket:**
```bash
websocat ws://localhost:8080/ws
```

## Project Structure

```
kafka-log-producer/
├── config/
│   └── producer_config.yaml   # Kafka, Prometheus, dashboard settings
├── src/
│   ├── __init__.py
│   ├── models.py              # LogEntry, LogLevel, topic routing
│   ├── config.py              # YAML loader + env var overrides
│   ├── log_generator.py       # Realistic sample log generator
│   ├── producer.py            # KafkaLogProducer with delivery tracking
│   ├── metrics.py             # Prometheus counters/histograms + throughput
│   ├── fallback_storage.py    # Disk buffer for Kafka unavailability
│   ├── websocket_manager.py   # WebSocket connection broadcasting
│   ├── dashboard.py           # FastAPI app factory with all routes
│   └── main.py                # CLI: server, demo, performance commands
├── templates/
│   └── index.html             # Dashboard UI (dark theme, charts, WebSocket)
├── tests/
│   ├── conftest.py            # Shared fixtures
│   ├── test_models.py         # 9 tests: routing, keys, serialization
│   ├── test_config.py         # 3 tests: YAML loading, env overrides
│   ├── test_log_generator.py  # 4 tests: generation, levels, batches
│   ├── test_producer.py       # 7 tests: produce, callbacks, stats
│   ├── test_metrics.py        # 6 tests: counters, throughput, snapshots
│   ├── test_fallback.py       # 7 tests: write, drain, count
│   └── test_dashboard.py      # 5 tests: endpoints, WebSocket, HTML
├── scripts/
│   ├── create_topics.sh       # Create 4 Kafka topics (3 partitions each)
│   ├── wait_for_kafka.py      # Poll until Kafka broker is ready
│   ├── verify_e2e.py          # 11-check E2E verification suite
│   └── benchmark.py           # Throughput + sustained rate benchmarks
├── Dockerfile                 # Production image
├── Dockerfile.test            # Test runner image
├── docker-compose.yml         # Full stack: ZK + Kafka + App + profiles
├── Makefile                   # build, up, down, test, e2e, benchmark
├── pytest.ini                 # Test configuration
└── requirements.txt           # Python dependencies
```

## Test Results

```
Unit Tests:     41/41 passed
E2E Checks:     11/11 passed
Burst Throughput: ~10,000 msg/s
Sustained Rate:  1,000 msg/s for 60s, zero failures
```

## What I Learned

- **confluent-kafka vs kafka-python**: confluent-kafka wraps librdkafka (C library), delivering 10x+ throughput over pure-Python kafka-python. The async delivery callback model requires thread-safe counters.
- **Producer tuning matters**: The interplay between `batch.size`, `linger.ms`, and `compression.type` directly controls throughput. 16KB batches + 5ms linger + gzip gave the best balance of latency and throughput.
- **Idempotent producers**: Setting `enable.idempotence=true` with `acks=all` prevents duplicate messages on retries, but requires `max.in.flight.requests.per.connection <= 5` (librdkafka handles this automatically).
- **Topic routing at the producer**: Routing logs to different topics by severity/service at produce time enables downstream consumers to subscribe only to what they need, reducing processing overhead.
- **Fallback storage pattern**: Buffering to disk (JSONL) when the broker is unreachable and replaying on reconnect provides graceful degradation without losing messages.
- **WebSocket broadcasting**: Using a connection manager with dead-connection cleanup prevents memory leaks when clients disconnect unexpectedly.
- **Benchmark accuracy**: Monotonic clocks and absolute time targets (instead of relative sleeps) prevent timing drift in sustained rate tests.

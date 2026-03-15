# Priority Queue Log Processor

A multi-tier priority queue system that classifies incoming log messages by criticality and processes them in priority order with real-time monitoring via a web dashboard.

## Tech Stack

- **Language:** Python 3.11
- **Web Framework:** Flask 3.1
- **Concurrency:** Threading (daemon worker threads, dynamic scaling)
- **Queue:** Custom thread-safe heap (`heapq` + `threading.Lock`)
- **Monitoring:** Prometheus metrics (`prometheus-client`)
- **Frontend:** HTML/CSS/JavaScript + Chart.js
- **Testing:** pytest, Docker
- **Infrastructure:** Docker, Docker Compose

## Architecture

```
┌─────────────────┐     ┌───────────────────────┐     ┌──────────────────────────┐
│  Load Generator  │────>│  Flask API (:8080)     │────>│  ThreadSafePriorityQueue  │
│  (synthetic logs)│     │  - POST /api/inject    │     │  (heapq + backpressure)  │
└─────────────────┘     │  - GET  /api/status    │     └────────────┬─────────────┘
                        │  - GET  /metrics       │                  │
                        │  - GET  /              │     ┌────────────v─────────────┐
                        └───────────────────────┘     │  DynamicWorkerPool        │
                                                      │  (auto-scaling threads)   │
                                                      └────────────┬─────────────┘
                                                                   │
                                                      ┌────────────v─────────────┐
                                                      │  PriorityAgingMonitor     │
                                                      │  (stale msg promotion)    │
                                                      └──────────────────────────┘
```

### Priority Levels

| Priority | Level    | Processing Time | Example Triggers |
|----------|----------|----------------|-----------------|
| P0       | CRITICAL | ~10ms          | Payment failure, security breach, system down |
| P1       | HIGH     | ~50ms          | High latency, memory threshold, timeout |
| P2       | MEDIUM   | ~100ms         | User error, validation failure, auth fail |
| P3       | LOW      | ~200ms         | Normal operations, health checks |

### Key Features

- **Thread-safe priority queue** with O(log n) heap operations
- **Watermark backpressure**: Rejects LOW at 80%, MEDIUM at 90%, HIGH at 95% capacity
- **Priority aging**: Stale messages auto-promote (LOW->MEDIUM->HIGH->CRITICAL)
- **Dynamic worker scaling**: Auto-adjusts thread count based on queue utilization
- **Regex-based classification**: 13 patterns across 4 priority levels
- **Real-time dashboard**: Live counters, charts, injection buttons
- **Prometheus metrics**: Counters, histograms, gauges for observability
- **Alerting**: Log-based alerts when queue depth exceeds threshold

## How to Run

```bash
# Build and run with Docker Compose
make up

# Or manually
docker compose up --build -d app

# View dashboard
open http://localhost:8080

# Run tests
make test

# Run E2E verification
make e2e

# View logs
make logs

# Clean up
make clean
```

## API Endpoints

| Method | Endpoint               | Description |
|--------|------------------------|-------------|
| GET    | `/`                   | Real-time monitoring dashboard |
| GET    | `/health`             | Health check (< 100ms) |
| GET    | `/api/status`         | Full queue/metrics/worker status JSON |
| POST   | `/api/inject/<priority>` | Inject test message (critical/high/medium/low) |
| GET    | `/metrics`            | Prometheus-compatible metrics |

### Example: Inject a Message

```bash
curl -X POST http://localhost:8080/api/inject/critical
```

### Example: Check Status

```bash
curl http://localhost:8080/api/status | python -m json.tool
```

## Configuration

| Parameter | Env Var | Default | Description |
|-----------|---------|---------|-------------|
| Max queue size | `MAX_QUEUE_SIZE` | 10000 | Maximum messages in queue |
| Worker count | `NUM_WORKERS` | 4 | Initial worker thread count |
| Dashboard port | `DASHBOARD_PORT` | 8080 | HTTP server port |
| Generator rate | `GENERATOR_RATE` | 100 | Synthetic messages/second |
| Aging threshold | `AGING_THRESHOLD_SECONDS` | 300 | Seconds before priority promotion |
| Alert threshold | `ALERT_QUEUE_DEPTH_THRESHOLD` | 8000 | Queue depth alert trigger |
| Min workers | `MIN_WORKERS` | 2 | Minimum workers (scaling) |
| Max workers | `MAX_WORKERS` | 16 | Maximum workers (scaling) |

## Performance

| Metric | Target | Achieved |
|--------|--------|----------|
| Throughput | >= 1,000 msg/s | Yes |
| Critical latency (p95) | <= 50ms | Yes |
| Memory under load | <= 100MB | Yes |
| Health check | < 100ms | Yes (~2ms avg) |
| Priority ordering | 100% accurate | Yes |

## What I Learned

- **Heap internals**: Python's `heapq` is a min-heap operating on plain lists. Wrapping it with a lock and monotonic counter ensures thread-safety and FIFO within same priority.
- **Lazy deletion pattern**: Instead of removing items from the heap (O(n)), mark them as "REMOVED" sentinel and skip on pop. This enables O(log n) priority updates for aging.
- **Watermark backpressure**: Production systems (Kafka, RabbitMQ) use multi-level watermarks -- not binary full/empty -- to gracefully shed load by priority tier.
- **Dynamic thread scaling**: `ThreadPoolExecutor` doesn't support shrinking. Custom pools need per-worker stop events for graceful scale-down.
- **Prometheus in Python**: The `prometheus_client` library is thread-safe by default. Module-level instrument singletons avoid registration conflicts.

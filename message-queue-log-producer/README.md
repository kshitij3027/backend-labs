# Message Queue Log Producer

A resilient Python log producer that accepts logs via HTTP, batches them intelligently, and publishes to RabbitMQ with circuit breaking, retry logic, and local fallback storage for zero data loss during broker outages.

## Tech Stack

- **Language:** Python 3.11
- **Web Framework:** Flask 3.1
- **Message Broker:** RabbitMQ 3.13 (with management plugin)
- **Libraries:** pika 1.3.2 (AMQP client), tenacity (retry), PyYAML (config)
- **Testing:** pytest, Docker
- **Containerization:** Docker, Docker Compose

## Architecture

```
Flask POST /logs -> BatchManager -> queue.Queue -> PublisherThread -> RabbitMQ
                                                        |  (on failure)
                                                  FallbackStorage (.jsonl)
                                                        |  (on recovery)
                                                  Drain back to RabbitMQ
```

**Thread model:** Flask runs in the main thread. Two daemon threads handle batching (BatchManager) and publishing (PublisherThread). Cross-thread communication uses `queue.Queue` and `threading.Lock`. The pika connection is exclusively owned by the publisher thread for thread safety.

**Key components:**
- **BatchManager** -- Hybrid flush: triggers on buffer size threshold OR time interval (whichever comes first)
- **CircuitBreaker** -- Protects against cascading failures with CLOSED -> OPEN -> HALF_OPEN state machine
- **FallbackStorage** -- JSONL file writer for zero data loss during broker outages
- **PublisherThread** -- Dedicated thread that publishes to RabbitMQ, handles circuit breaker logic, and drains fallback on recovery
- **MetricsCollector** -- Thread-safe counters for throughput, latency P95, error rates

## How to Run

### Prerequisites
- Docker and Docker Compose

### Quick Start
```bash
# Build all images
make build

# Start RabbitMQ + app
make run

# Run unit tests (in Docker)
make test

# Run E2E verification (in Docker)
make e2e

# Run throughput test
make throughput

# Run broker outage resilience test
bash scripts/test_outage.sh

# Stop and clean up
make clean
```

## API

### POST /logs
Accept one or more log entries.
```bash
# Single entry
curl -X POST http://localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '{"level": "info", "message": "User logged in", "source": "auth-service"}'

# Batch
curl -X POST http://localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '[{"level": "info", "message": "msg1", "source": "app"}, {"level": "error", "message": "msg2", "source": "app"}]'
```
Response: `202 {"accepted": N}`

### GET /health
```json
{"healthy": true, "status": "ok", "throughput": 150.5, "latency_p95": 12.3, "circuit_breaker": "closed"}
```

### GET /metrics
```json
{
  "messages_received": 5000,
  "messages_published": 4950,
  "batches_flushed": 50,
  "publish_errors": 1,
  "fallback_writes": 50,
  "fallback_drained": 50,
  "throughput": 150.5,
  "latency_p95": 12.3,
  "uptime_seconds": 33.2,
  "circuit_breaker_state": "closed",
  "buffer_size": 5,
  "queue_depth": 0
}
```

## Configuration

Settings in `config.yaml` with environment variable overrides:

| Setting | Env Var | Default |
|---------|---------|---------|
| RabbitMQ host | `RABBITMQ_HOST` | localhost |
| RabbitMQ port | `RABBITMQ_PORT` | 5672 |
| Batch size | `BATCH_SIZE` | 100 |
| Flush interval | `BATCH_FLUSH_INTERVAL` | 2.0s |
| Circuit breaker threshold | `CIRCUIT_BREAKER_THRESHOLD` | 5 failures |
| Circuit breaker timeout | `CIRCUIT_BREAKER_TIMEOUT` | 30s |
| HTTP port | `HTTP_PORT` | 8080 |

## What I Learned

- **pika is NOT thread-safe.** The AMQP client must be used from a single thread. Solved by isolating all pika operations in a dedicated `PublisherThread` and using `queue.Queue` for cross-thread communication.
- **Circuit breaker pattern** prevents cascading failures. Lazy OPEN->HALF_OPEN transition via `time.monotonic()` avoids needing a background timer.
- **Hybrid batching** (size OR time threshold) balances latency vs throughput -- high-volume bursts flush immediately by size, low-volume periods flush by time interval.
- **Fallback storage with automatic drain** ensures zero data loss during broker outages. JSONL format keeps it simple and appendable.
- **`threading.Event.wait(timeout)`** elegantly combines size-based and time-based flush triggers in a single blocking call.
- **Publish confirms** (`channel.confirm_delivery()`) provide reliable delivery guarantees from RabbitMQ.

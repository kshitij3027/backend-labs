# RabbitMQ Log Message Queue

A message queuing system that uses RabbitMQ to route, persist, and distribute log messages across multiple consumers via topic-based exchanges and dead-letter handling.

## Architecture

```
                           RabbitMQ Broker
  ┌────────────────────────────────────────────────────────────┐
  │                                                            │
  │   ┌──────────────────────────┐    ┌─────────────────────┐  │
  │   │  Exchange: logs (topic)  │    │  DLX: logs_dlx      │  │
  │   └───────┬──────┬──────┬────┘    └──────────┬──────────┘  │
  │           │      │      │                    │             │
  │   ┌───────┘      │      └───────┐    ┌───────┘             │
  │   │              │              │    │                     │
  │   ▼              ▼              ▼    ▼                     │
  │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────┐ │
  │ │log_msgs  │ │error_msgs│ │debug_msgs│ │dead_letter_queue│ │
  │ │info.*    │ │error.*   │ │debug.*   │ │(failed msgs)   │ │
  │ └──────────┘ └──────────┘ └──────────┘ └────────────────┘ │
  └────────────────────────────────────────────────────────────┘
       ▲                                         │
       │                                         ▼
  ┌──────────┐                           ┌──────────────┐
  │Publisher  │                           │  Consumers   │
  │  CLI      │                           │  (per queue) │
  └──────────┘                           └──────────────┘
```

**Components:**

- **Config** (`src/config.py`) -- Loads settings from `rabbitmq_config.yaml` with env var overrides.
- **Connection** (`src/connection.py`) -- Manages pika connections with retry logic and context manager support.
- **Setup** (`src/setup.py`) -- Declares the full topology: exchanges, queues, bindings, and dead-letter infrastructure.
- **Publisher** (`src/publisher.py`) -- Publishes JSON log messages with topic routing keys (`logs.<level>.<source>`).
- **Consumer** (`src/consumer.py`) -- Consumes messages from a queue with manual acknowledgement and graceful shutdown.
- **Queue Manager** (`src/queue_manager.py`) -- High-level CLI for publishing and viewing queue statistics via the Management API.
- **Health Checker** (`src/health_checker.py`) -- Validates connectivity, Management API reachability, and queue status.

## Tech Stack

- **Language:** Python 3.12
- **Message Broker:** RabbitMQ 3.13 (management-alpine image)
- **Client Library:** pika 1.3.2
- **CLI Framework:** click 8.1
- **Formatting:** rich 13.9
- **Containerization:** Docker / Docker Compose
- **Testing:** pytest 8.3 (unit/integration), standalone E2E scripts

## Prerequisites

- Docker and Docker Compose (v2)
- GNU Make (optional, for convenience targets)

## Quick Start

```bash
# Build containers
make build

# Start RabbitMQ broker (with management UI at http://localhost:15672)
make run

# Run the full E2E verification
make e2e

# Stop everything
make stop
```

## Available Commands

| Command          | Description                                       |
|------------------|---------------------------------------------------|
| `make build`     | Build Docker images                               |
| `make run`       | Start RabbitMQ in the background                  |
| `make stop`      | Stop all services                                 |
| `make test`      | Run unit and integration tests in Docker          |
| `make e2e`       | Run full end-to-end verification in Docker        |
| `make throughput` | Run throughput benchmark (1000 messages)          |
| `make clean`     | Stop services and remove volumes                  |
| `make logs`      | Tail Docker Compose logs                          |

## Project Structure

```
rabbitmq-log-message-queue/
├── Dockerfile                 # App container
├── Dockerfile.test            # Test container
├── Makefile                   # Build/test/run targets
├── docker-compose.yml         # RabbitMQ + app services
├── rabbitmq_config.yaml       # Exchange, queue, and DLX configuration
├── requirements.txt           # Python dependencies
├── README.md
├── src/
│   ├── __init__.py
│   ├── config.py              # YAML + env var config loader
│   ├── connection.py          # Pika connection manager with retries
│   ├── setup.py               # Topology setup (exchanges, queues, DLX)
│   ├── publisher.py           # Log message publisher CLI
│   ├── consumer.py            # Log message consumer CLI
│   ├── queue_manager.py       # Queue stats + publish CLI
│   └── health_checker.py      # Health check CLI
├── scripts/
│   ├── wait_for_rabbitmq.py   # Waits for broker readiness
│   ├── verify_e2e.py          # End-to-end verification suite
│   └── throughput_test.py     # Throughput benchmark
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_config.py
    ├── test_connection.py
    ├── test_setup.py
    ├── test_publisher.py
    ├── test_queue_manager.py
    ├── test_health_checker.py
    └── test_integration.py
```

## CLI Usage

All CLIs run inside the app container. Prefix commands with `docker compose run --rm app`.

### Publish a log message

```bash
docker compose run --rm app python -m src.publisher \
    --level info --source web -m "User logged in"
```

### Consume messages from a queue

```bash
docker compose run --rm app python -m src.consumer \
    --queue error_messages
```

### View queue statistics

```bash
docker compose run --rm app python -m src.queue_manager stats
```

### Run a health check

```bash
docker compose run --rm app python -m src.health_checker
```

### Set up topology manually

```bash
docker compose run --rm app python -m src.setup
```

## Configuration

### rabbitmq_config.yaml

The main configuration file defines the broker connection, exchange, queues, and dead-letter settings:

```yaml
rabbitmq:
  host: localhost          # Overridden by RABBITMQ_HOST env var in Docker
  port: 5672
  management_port: 15672
  credentials:
    username: guest
    password: guest
  heartbeat: 600
  blocked_connection_timeout: 300
  connection:
    retry_max: 5
    retry_delay: 1

exchange:
  name: logs
  type: topic
  durable: true

queues:
  - name: log_messages
    routing_key: "logs.info.*"
    durable: true
  - name: error_messages
    routing_key: "logs.error.*"
    durable: true
  - name: debug_messages
    routing_key: "logs.debug.*"
    durable: true

dead_letter:
  exchange: logs_dlx
  queue: dead_letter_queue
  routing_key: failed
```

### Environment Variables

| Variable        | Default     | Description                     |
|-----------------|-------------|---------------------------------|
| `RABBITMQ_HOST` | `localhost` | RabbitMQ broker hostname        |

## Testing

```bash
# Unit and integration tests (mocked, no broker required)
make test

# Full end-to-end verification (requires running broker)
make e2e

# Throughput benchmark (1000 persistent messages)
make throughput
```

The E2E suite verifies:
- Topology creation (exchange, queues, DLX)
- Message routing to the correct queue by level
- Message payload structure (timestamp, level, source, message)
- Health check subsystem
- Queue statistics via the Management API

## What I Learned

- **Topic exchanges** provide flexible routing with wildcard binding keys (`*` matches one word, `#` matches zero or more). This is ideal for log categorization by level and source.
- **Dead-letter exchanges** give undeliverable or rejected messages a second chance by routing them to a dedicated queue for inspection, rather than silently dropping them.
- **Message persistence** requires both a durable queue declaration and `delivery_mode=2` on the message properties -- missing either one means messages can be lost on broker restart.
- **Manual acknowledgement** (`basic_ack`) ensures a message is only removed from the queue after the consumer has successfully processed it, preventing data loss during crashes.
- **Prefetch count** (`basic_qos(prefetch_count=1)`) prevents a single consumer from hoarding all messages, enabling fair dispatch across multiple consumers.
- **Connection management** matters: heartbeats detect dead connections, blocked-connection timeouts handle flow control, and exponential-backoff retries make startup order irrelevant.
- **RabbitMQ Management API** (port 15672) is invaluable for monitoring queue depths, consumer counts, and exchange bindings programmatically -- the same data visible in the web UI is available as JSON.
- **YAML-driven configuration** keeps topology definitions (exchange names, routing keys, queue settings) out of application code and easy to review or change.

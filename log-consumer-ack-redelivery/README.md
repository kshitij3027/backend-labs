# Log Consumer Ack Redelivery

A RabbitMQ consumer wrapper that adds message acknowledgment tracking, exponential backoff redelivery, and dead letter queue handling to ensure at-least-once log processing even under failures and crashes.

## Tech Stack

- **Language**: Python 3.11+
- **Message Broker**: RabbitMQ (with management plugin)
- **Consumer Library**: pika
- **Web Dashboard**: FastAPI + Uvicorn
- **CLI Producer**: Click
- **Monitoring**: Prometheus metrics via prometheus-client
- **Logging**: structlog

## Architecture

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────────┐
│  CLI Producer │────>│    RabbitMQ        │────>│  Consumer Worker  │
│  (click)      │     │                   │     │  (pika)           │
└──────────────┘     │  ┌─────────────┐  │     │                  │
                     │  │ main queue  │  │     │  ack/nack logic  │
                     │  └─────────────┘  │     │  retry counter   │
                     │  ┌─────────────┐  │     │  backoff calc    │
                     │  │ retry queues│  │     └────────┬─────────┘
                     │  │ (per-queue  │  │              │
                     │  │  TTL-based) │  │              │
                     │  └─────────────┘  │     ┌────────▼─────────┐
                     │  ┌─────────────┐  │     │ In-Memory Tracker │
                     │  │ dead letter │  │     │ (ack state,       │
                     │  │ queue (DLQ) │  │     │  retries, stats)  │
                     │  └─────────────┘  │     └────────┬─────────┘
                     └───────────────────┘              │
                                                ┌───────▼─────────┐
                                                │  FastAPI Dashboard│
                                                │  (monitoring UI)  │
                                                └─────────────────┘
```

## Components

### 1. Consumer Worker
- Connects to RabbitMQ and consumes messages from the main queue
- Processes log messages with configurable handler
- Tracks acknowledgment state per message in an in-memory AckTracker
- On processing failure: acks the original message, then republishes a copy to the appropriate retry queue (ack-then-republish pattern)
- Moves messages to DLQ after max retry attempts exceeded
- Graceful shutdown with in-flight message completion

### 2. Redelivery Engine
- Exponential backoff using separate retry queues, each with a queue-level `x-message-ttl`
- Configurable retry delays via `RETRY_DELAYS` (e.g., `[1000, 2000, 4000, 8000]` ms)
- Uses per-queue TTL: each delay tier has its own retry queue with a fixed TTL
- When TTL expires, messages are dead-lettered back to the main exchange for another attempt
- Tracks retry count and history per message ID

### 3. Dead Letter Queue (DLQ) Handler
- Messages exceeding max retries are routed to the DLQ
- DLQ messages retain full retry history and failure reasons
- Dashboard exposes DLQ inspection

### 4. FastAPI Web Dashboard
- Real-time stats: messages processed, acked, nacked, retried, dead-lettered
- Per-message retry history and current state
- DLQ browser
- Health check endpoint for consumer liveness
- Auto-refreshing HTML UI

### 5. CLI Message Producer
- Send individual or batch log messages to the main queue
- Configurable message format (JSON log entries)
- Simulate failure scenarios (malformed messages, large payloads)
- Useful for testing and demos

## Configuration

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_HOST` | `rabbitmq` | RabbitMQ server host |
| `RABBITMQ_PORT` | `5672` | RabbitMQ AMQP port |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASS` | `guest` | RabbitMQ password |
| `MAIN_QUEUE` | `logs.incoming` | Main consumption queue |
| `MAIN_EXCHANGE` | `logs.main` | Main exchange name |
| `RETRY_EXCHANGE` | `logs.retry` | Retry exchange name |
| `RETRY_DELAYS` | `[1000, 2000, 4000, 8000]` | Retry queue TTLs in milliseconds |
| `DLQ_QUEUE` | `logs.dead_letter` | Dead letter queue name |
| `MAX_RETRIES` | `5` | Max retry attempts before DLQ |
| `ACK_TIMEOUT_SEC` | `30` | Seconds before an unacked message is considered timed out |
| `DASHBOARD_PORT` | `8000` | FastAPI dashboard port |
| `PREFETCH_COUNT` | `10` | RabbitMQ prefetch count |
| `FAILURE_RATE` | `0.2` | Simulated processing failure rate (0.0-1.0) |
| `TIMEOUT_RATE` | `0.1` | Simulated processing timeout rate (0.0-1.0) |

## How to Run

```bash
# Start all services
docker compose up --build -d

# Send test messages
docker compose exec app python -m src.message_producer send --count 20

# View dashboard
open http://localhost:8000

# Run unit tests
docker compose run --rm --build test

# Run E2E tests
docker compose --profile e2e up --build --abort-on-container-exit

# View logs
docker compose logs -f app

# Stop everything
docker compose down -v
```

## How It Runs

Long-lived process -- a consumer service that connects to RabbitMQ, continuously consumes messages, processes them, and manages the ack/nack/redelivery lifecycle. The consumer worker runs indefinitely until stopped (SIGINT/SIGTERM for graceful shutdown). A companion FastAPI server provides a real-time web dashboard for monitoring message flow, retry states, and DLQ contents. A CLI producer is included for testing.

## Message Flow

1. Producer publishes JSON log message to `logs.incoming` via the main exchange
2. Consumer picks up message, attempts processing
3. **Success**: message is acked, state recorded in the in-memory tracker
4. **Failure**: message is acked (ack-then-republish pattern), retry count incremented
5. **Retry**: a copy of the message is published to a per-TTL retry queue with the appropriate delay (exponential backoff)
6. **TTL expires**: message is dead-lettered back to `logs.incoming` for another attempt
7. **Max retries exceeded**: message routed to DLQ with full failure history

## What I Learned

- **Per-queue TTL vs per-message TTL**: RabbitMQ per-message TTL only expires messages at the HEAD of the queue -- messages behind a long-TTL message get stuck. Solution: separate retry queues each with queue-level `x-message-ttl`.
- **Pika thread safety**: pika's `BlockingConnection` is NOT thread-safe. Use `connection.add_callback_threadsafe()` to schedule operations from other threads. Signal handlers can only be registered from the main thread.
- **Ack-then-republish pattern**: For delayed retries, always `basic_ack` the original message then `basic_publish` a copy to the retry queue. Never use `basic_nack(requeue=True)` -- that causes immediate redelivery without backoff.
- **`process_data_events` loop**: Using `process_data_events(time_limit=1)` instead of `start_consuming()` allows checking a shutdown event each iteration for graceful shutdown.
- **Exponential backoff with reconnection**: Pika has no auto-reconnect. Wrap the consumer in an outer try/except loop with exponential backoff (1s to 30s) and re-declare all queues/exchanges after reconnecting.

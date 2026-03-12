# Log Consumer Ack Redelivery

A RabbitMQ consumer wrapper that adds message acknowledgment tracking, exponential backoff redelivery, and dead letter queue handling to ensure at-least-once log processing even under failures and crashes.

## Tech Stack

- **Language**: Python 3.11+
- **Message Broker**: RabbitMQ (with management plugin)
- **Consumer Library**: pika
- **Web Dashboard**: FastAPI + Uvicorn
- **CLI Producer**: Click
- **Monitoring**: Prometheus metrics via prometheus-client
- **Storage**: SQLite (ack tracking / redelivery state)

## Architecture

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────────┐
│  CLI Producer │────▶│    RabbitMQ        │────▶│  Consumer Worker  │
│  (click)      │     │                   │     │  (pika)           │
└──────────────┘     │  ┌─────────────┐  │     │                  │
                     │  │ main queue  │  │     │  ack/nack logic  │
                     │  └─────────────┘  │     │  retry counter   │
                     │  ┌─────────────┐  │     │  backoff calc    │
                     │  │ retry queue │  │     └────────┬─────────┘
                     │  │ (TTL-based) │  │              │
                     │  └─────────────┘  │              │
                     │  ┌─────────────┐  │     ┌────────▼─────────┐
                     │  │ dead letter │  │     │  SQLite Tracker   │
                     │  │ queue (DLQ) │  │     │  (ack state, retries)│
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
- Tracks acknowledgment state per message in SQLite
- On processing failure: nacks the message and schedules redelivery with exponential backoff
- Moves messages to DLQ after max retry attempts exceeded
- Graceful shutdown with in-flight message completion

### 2. Redelivery Engine
- Exponential backoff with jitter: `min(base_delay * 2^attempt + jitter, max_delay)`
- Configurable base delay, max delay, max retries, and jitter range
- Uses RabbitMQ TTL-based retry queues (per-message TTL)
- Tracks retry count and history per message ID

### 3. Dead Letter Queue (DLQ) Handler
- Messages exceeding max retries are routed to the DLQ
- DLQ messages retain full retry history and failure reasons
- Dashboard exposes DLQ inspection and manual requeue

### 4. FastAPI Web Dashboard
- Real-time stats: messages processed, acked, nacked, retried, dead-lettered
- Per-message retry history and current state
- DLQ browser with manual requeue/discard actions
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
| `RABBITMQ_HOST` | `localhost` | RabbitMQ server host |
| `RABBITMQ_PORT` | `5672` | RabbitMQ AMQP port |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASS` | `guest` | RabbitMQ password |
| `RABBITMQ_VHOST` | `/` | RabbitMQ virtual host |
| `MAIN_QUEUE` | `logs.incoming` | Main consumption queue |
| `RETRY_EXCHANGE` | `logs.retry` | Retry exchange name |
| `DLQ_QUEUE` | `logs.dead_letter` | Dead letter queue name |
| `MAX_RETRIES` | `5` | Max retry attempts before DLQ |
| `BASE_DELAY_SEC` | `1` | Base delay for exponential backoff |
| `MAX_DELAY_SEC` | `60` | Maximum backoff delay cap |
| `DASHBOARD_PORT` | `8000` | FastAPI dashboard port |
| `DB_PATH` | `./data/ack_tracker.db` | SQLite database path |
| `PREFETCH_COUNT` | `10` | RabbitMQ prefetch count |

## How to Run

```bash
# 1. Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the consumer worker
python -m consumer.worker

# 4. Start the dashboard (separate terminal)
python -m dashboard.app

# 5. Send test messages via CLI producer
python -m producer.cli send --count 20
python -m producer.cli send --simulate-failures --count 5
```

## How It Runs

Long-lived process — a consumer service that connects to RabbitMQ, continuously consumes messages, processes them, and manages the ack/nack/redelivery lifecycle. The consumer worker runs indefinitely until stopped (SIGINT/SIGTERM for graceful shutdown). A companion FastAPI server provides a real-time web dashboard for monitoring message flow, retry states, and DLQ contents. A CLI producer is included for testing.

## Message Flow

1. Producer publishes JSON log message to `logs.incoming`
2. Consumer picks up message, attempts processing
3. **Success**: message is acked, state recorded in SQLite
4. **Failure**: message is nacked, retry count incremented
5. **Retry**: message republished to retry queue with per-message TTL (exponential backoff)
6. **TTL expires**: message re-routed back to `logs.incoming` for another attempt
7. **Max retries exceeded**: message routed to DLQ with full failure history

## What I Learned

_(To be filled in after implementation)_

# Kafka Partitioning & Consumer Group

A multi-partition Kafka topic with a smart log producer and a coordinated consumer group system that processes logs in parallel with real-time monitoring.

## How It Runs

Long-lived process — Docker Compose brings up Kafka infrastructure, then a Python CLI launches the producer and consumer group. A web dashboard runs alongside for monitoring. Can also run a focused CLI-only mode.

## Tech Stack

- **Language:** Python 3.12
- **Message Broker:** Apache Kafka (via KRaft mode, no Zookeeper)
- **Kafka Client:** confluent-kafka
- **Web Dashboard:** FastAPI + WebSocket
- **Monitoring:** Rich (CLI), Chart.js (web)
- **Containerization:** Docker & Docker Compose

## Architecture

```
┌─────────────────┐
│  Smart Producer  │──── Partitioning strategy (round-robin / key-based / custom)
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│         Kafka Topic (N partitions)      │
│  ┌──────┐  ┌──────┐  ┌──────┐          │
│  │ P-0  │  │ P-1  │  │ P-2  │  ...     │
│  └──┬───┘  └──┬───┘  └──┬───┘          │
└─────┼────────┼────────┼────────────────┘
      │        │        │
      ▼        ▼        ▼
┌──────────────────────────────┐
│     Consumer Group           │
│  ┌────┐  ┌────┐  ┌────┐     │
│  │ C-0│  │ C-1│  │ C-2│     │
│  └────┘  └────┘  └────┘     │
│  (auto-rebalancing)          │
└──────────┬───────────────────┘
           │
           ▼
┌─────────────────────┐
│  Monitoring Layer   │
│  - CLI (Rich)       │
│  - Web (FastAPI)    │
└─────────────────────┘
```

## Core Features

### Smart Log Producer
- Generates structured log messages (JSON) with varying severity levels
- Configurable partitioning strategies:
  - **Round-robin** — even distribution across partitions
  - **Key-based** — logs from the same source always go to the same partition
  - **Custom** — partition by severity (ERROR/CRITICAL to dedicated partitions)
- Configurable throughput (messages per second)
- Batch production support

### Coordinated Consumer Group
- Multiple consumers in a single consumer group
- Automatic partition assignment and rebalancing
- Parallel log processing across partitions
- Offset management (auto-commit and manual commit modes)
- Graceful shutdown with offset commit on SIGTERM

### Real-Time Monitoring
- **CLI mode (Rich):** Live dashboard showing per-partition throughput, consumer lag, message counts
- **Web mode (FastAPI):** Browser-based dashboard with WebSocket updates, charts for throughput over time, partition distribution, consumer group health

### Observability
- Per-partition message counters and throughput rates
- Consumer lag tracking per partition
- Rebalance event logging
- Producer delivery confirmations and error rates

## Project Structure

```
kafka-partitioning-consumer-group/
├── README.md
├── requirements.txt
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── src/
│   ├── __init__.py
│   ├── producer/
│   │   ├── __init__.py
│   │   ├── smart_producer.py       # Log producer with partitioning strategies
│   │   └── log_generator.py        # Structured log message generator
│   ├── consumer/
│   │   ├── __init__.py
│   │   ├── consumer_group.py       # Consumer group coordinator
│   │   ├── log_processor.py        # Per-consumer log processing logic
│   │   └── rebalance_handler.py    # Partition rebalance callbacks
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── metrics.py              # Shared metrics collection
│   │   ├── cli_dashboard.py        # Rich-based CLI monitoring
│   │   └── web_dashboard.py        # FastAPI + WebSocket dashboard
│   ├── config.py                   # Centralized configuration
│   └── cli.py                      # CLI entry point (click)
├── static/
│   └── dashboard.html              # Web dashboard frontend
└── tests/
    ├── __init__.py
    ├── test_producer.py
    ├── test_consumer.py
    └── test_metrics.py
```

## How to Run

### Docker Compose (full stack)
```bash
docker-compose up --build
```

### CLI Mode (producer + consumers + CLI dashboard)
```bash
python -m src.cli run --mode cli --partitions 6 --consumers 3 --rate 100
```

### Web Dashboard Mode
```bash
python -m src.cli run --mode web --partitions 6 --consumers 3 --rate 100
# Open http://localhost:8000 in browser
```

### Producer Only
```bash
python -m src.cli producer --partitions 6 --strategy key-based --rate 50
```

### Consumer Group Only
```bash
python -m src.cli consumer --group-id log-processors --consumers 3
```

## Configuration

| Environment Variable         | Default         | Description                          |
|------------------------------|-----------------|--------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`    | `kafka:9092`    | Kafka broker addresses               |
| `KAFKA_TOPIC`               | `logs`          | Topic name                           |
| `KAFKA_NUM_PARTITIONS`      | `6`             | Number of topic partitions           |
| `KAFKA_REPLICATION_FACTOR`  | `1`             | Replication factor                   |
| `CONSUMER_GROUP_ID`         | `log-processors`| Consumer group ID                    |
| `PRODUCER_RATE`             | `100`           | Messages per second                  |
| `PARTITION_STRATEGY`        | `key-based`     | round-robin, key-based, or custom    |
| `WEB_DASHBOARD_PORT`        | `8000`          | Web dashboard port                   |

## What I Learned

- **Consumer group rebalancing** with the cooperative-sticky assignor minimizes partition revocations during scaling events. Unlike the eager (range/round-robin) assignors that revoke all partitions before reassigning, cooperative-sticky only moves the partitions that need to change, keeping the rest processing without interruption.
- **Manual partition assignment via CRC32 hashing** on partition keys (e.g., `service_name` or `user_id`) guarantees that related log entries always land on the same partition. This preserves per-key ordering without relying on Kafka's default partitioner, and makes the routing deterministic and testable.
- **Thread-safe metrics collection** across concurrent consumers requires careful locking. Using a single `MetricsCollector` shared by all consumer threads with a `threading.Lock` avoids race conditions while keeping the snapshot method consistent -- each snapshot is a frozen point-in-time view.
- **Real-time dashboards over WebSocket** are more efficient than polling. Broadcasting metrics snapshots at 1-second intervals via FastAPI's WebSocket support gives the browser dashboard live updates without the overhead of repeated HTTP requests.
- **Graceful shutdown with signal traps** (SIGINT, SIGTERM) plus a `threading.Event` is the cleanest pattern for coordinating multi-threaded producer/consumer shutdown. Each thread checks the event in its loop, and the main thread joins them with a timeout to avoid hanging.
- **BufferError handling in the producer** is essential for sustained high-throughput production. When the internal librdkafka buffer fills up, retrying after a `poll(1.0)` drains delivered messages and frees space, preventing message loss.
- **Manual offset commits** (vs auto-commit) give precise control over at-least-once delivery semantics. Committing every N messages balances between commit overhead and reprocessing risk on consumer restart.

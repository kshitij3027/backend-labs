# Kafka Log Streaming Cluster

A 3-broker Apache Kafka cluster with ZooKeeper coordination that ingests structured logs from multiple simulated services, replicates them for fault tolerance, and serves them to independent consumer groups including a real-time dashboard and error aggregator.

## Tech Stack

- **Language:** Python 3.12
- **Message Broker:** Apache Kafka (3-broker cluster)
- **Coordination:** Apache ZooKeeper
- **Kafka Client:** confluent-kafka / kafka-python
- **Dashboard:** FastAPI + Jinja2 templates
- **Structured Logging:** structlog + Pydantic models
- **Containerization:** Docker + Docker Compose
- **Testing:** pytest

## Architecture

```
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Auth Service   │  │  Payment Svc    │  │  Order Service  │
│  (Producer)     │  │  (Producer)     │  │  (Producer)     │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                    Kafka Cluster (3 Brokers)                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │ Broker 1 │  │ Broker 2 │  │ Broker 3 │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
│                                                              │
│  Topics: service-logs (partitioned, replicated)              │
│          error-logs   (filtered errors)                      │
│                                                              │
│  Coordination: ZooKeeper                                     │
└──────────────────┬───────────────────┬───────────────────────┘
                   │                   │
                   ▼                   ▼
        ┌──────────────────┐  ┌──────────────────┐
        │  Real-Time       │  │  Error            │
        │  Dashboard       │  │  Aggregator       │
        │  (Consumer Grp)  │  │  (Consumer Grp)   │
        │  FastAPI UI      │  │                    │
        └──────────────────┘  └──────────────────┘
```

### Components

1. **Log Producers** — Simulated microservices (auth, payment, order) that generate structured JSON logs and publish them to Kafka topics.
2. **Kafka Cluster** — 3 brokers with replication factor 3 for fault tolerance. Topics are partitioned for parallel consumption.
3. **ZooKeeper** — Manages broker coordination, leader election, and cluster metadata.
4. **Setup Script** — One-time topic creation and configuration (partition count, replication factor, retention).
5. **Real-Time Dashboard Consumer** — FastAPI app that consumes logs and serves a live monitoring UI.
6. **Error Aggregator Consumer** — Independent consumer group that filters and aggregates error-level logs.

### Key Concepts

- Multi-broker Kafka cluster with replication for fault tolerance
- Consumer groups for independent, parallel log processing
- Structured log schema with Pydantic validation
- Topic partitioning strategies for throughput
- Broker failure and recovery (graceful degradation)
- At-least-once delivery semantics

## How to Run

> Long-lived process — Docker Compose brings up the cluster infrastructure, then Python producer/consumer/dashboard processes run continuously against it.

```bash
# 1. Start the Kafka cluster (ZooKeeper + 3 brokers)
docker-compose up -d zookeeper kafka-1 kafka-2 kafka-3

# 2. Create topics (one-time setup)
docker-compose run --rm setup

# 3. Start producers (simulated services)
docker-compose up -d producer-auth producer-payment producer-order

# 4. Start consumers
docker-compose up -d dashboard error-aggregator

# 5. Open the dashboard
open http://localhost:8000

# Bring everything up at once
docker-compose up -d

# View logs
docker-compose logs -f producer-auth
docker-compose logs -f dashboard

# Shut down
docker-compose down -v
```

## Testing

```bash
# Run unit tests inside Docker
docker-compose run --rm test

# Run locally (requires Kafka cluster running)
pytest tests/ -v
```

## What I Learned

*(To be filled after implementation)*

## API / Dashboard

| Endpoint | Description |
|---|---|
| `GET /` | Live dashboard UI with log stream |
| `GET /api/logs` | Recent logs (JSON) |
| `GET /api/stats` | Aggregated log statistics |
| `GET /api/errors` | Recent error logs |
| `GET /health` | Health check |

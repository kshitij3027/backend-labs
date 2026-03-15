# Kafka Log Streaming Cluster

A 3-broker Apache Kafka cluster with ZooKeeper coordination that ingests structured logs from multiple simulated services, replicates them for fault tolerance, and serves them to independent consumer groups including a real-time dashboard and error aggregator.

## Tech Stack

- **Language:** Python 3.12
- **Message Broker:** Apache Kafka (3-broker cluster via Confluent images)
- **Coordination:** Apache ZooKeeper
- **Kafka Client:** confluent-kafka (librdkafka-based, high performance)
- **Dashboard:** FastAPI + Jinja2 templates + SSE (Server-Sent Events)
- **Structured Logging:** structlog + Pydantic models
- **Containerization:** Docker + Docker Compose
- **Testing:** pytest (149 unit tests) + custom E2E/fault/benchmark scripts
- **Monitoring:** Kafka UI (provectuslabs/kafka-ui)

## Architecture

```
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Web API Service │  │  User Service   │  │ Payment Service │
│  (Producer)      │  │  (Producer)     │  │  (Producer)     │
└────────┬─────────┘  └────────┬────────┘  └────────┬────────┘
         │                     │                     │
    web-api-logs       user-service-logs     payment-service-logs
         │                     │                     │
         ▼                     ▼                     ▼
┌──────────────────────────────────────────────────────────────┐
│                    Kafka Cluster (3 Brokers)                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │ Broker 1 │  │ Broker 2 │  │ Broker 3 │                   │
│  │ :9092    │  │ :9093    │  │ :9094    │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
│                                                              │
│  Topics: web-api-logs        (3 partitions, RF=3)            │
│          user-service-logs   (3 partitions, RF=3)            │
│          payment-service-logs(3 partitions, RF=3)            │
│          critical-logs       (1 partition,  RF=3)            │
│                                                              │
│  Coordination: ZooKeeper :2181                               │
└──────────────────┬───────────────────┬───────────────────────┘
                   │                   │
                   ▼                   ▼
        ┌──────────────────┐  ┌──────────────────┐
        │  Real-Time       │  │  Error            │
        │  Dashboard       │  │  Aggregator       │
        │  (Consumer Grp)  │  │  (Consumer Grp)   │
        │  FastAPI + SSE   │  │                    │
        │  :8000           │  │                    │
        └──────────────────┘  └──────────────────┘
```

### Components

1. **Service Simulators** (`src/producer.py`) -- Three simulated microservices (web-api, user-service, payment-service) run inside a single producer process. Each simulator generates realistic structured JSON logs with weighted distributions for status codes and log levels, then publishes to its dedicated Kafka topic. ERROR-level messages are also duplicated to the `critical-logs` topic.
2. **Kafka Cluster** -- 3 Confluent Kafka brokers with replication factor 3 and `min.insync.replicas=2`. Topics are partitioned (3 partitions each) using `user_id` as the partition key for consistent ordering per user.
3. **ZooKeeper** -- Manages broker coordination, leader election, and cluster metadata.
4. **Topic Init Script** (`scripts/create_topics.sh`) -- Runs once at startup to create all 4 topics with the correct partition count and replication factor, with retry logic until brokers are ready.
5. **Dashboard Consumer** (`src/consumer.py`) -- Consumes from all 3 service topics via the `dashboard-consumer` consumer group. Maintains a rolling buffer of recent messages and aggregated stats.
6. **Error Aggregator** (`src/error_aggregator.py`) -- Independent `error-aggregator-consumer` group that subscribes to `critical-logs` and all service topics, filtering for ERROR-level messages and tracking error counts/rates.
7. **Metrics Tracker** (`src/metrics.py`) -- Tracks throughput (msg/sec), consumer lag, and latency statistics. Fed by the dashboard's background sampling tasks.
8. **FastAPI Dashboard** (`src/dashboard.py`) -- Serves a real-time monitoring UI with SSE streaming, plus JSON API endpoints for logs, stats, errors, metrics, and ordering verification.
9. **Kafka UI** -- Web UI on port 8080 for inspecting topics, partitions, consumer groups, and broker state.

### Key Concepts

- Multi-broker Kafka cluster with replication for fault tolerance
- Dual-listener pattern (INTERNAL for Docker network, EXTERNAL for host access)
- Consumer groups for independent, parallel log processing
- Key-based partitioning (user_id) for per-user message ordering
- Structured log schema with Pydantic validation
- Server-Sent Events (SSE) for real-time browser streaming
- At-least-once delivery semantics with `acks=all`
- Broker failure tolerance with `min.insync.replicas=2`

## How to Run

> Long-lived process -- Docker Compose brings up the cluster infrastructure, then Python producer/consumer/dashboard processes run continuously against it.

```bash
# Start everything (Kafka cluster + producers + dashboard)
docker compose up -d --build

# Or step by step:
# 1. Start infrastructure (ZooKeeper, brokers, topic init, Kafka UI)
docker compose up -d zookeeper kafka-1 kafka-2 kafka-3 kafka-init kafka-ui

# 2. Start application services (producer + dashboard)
docker compose up -d producer dashboard

# 3. Open the live dashboard
open http://localhost:8000

# 4. Open Kafka UI for cluster inspection
open http://localhost:8080

# View logs
docker compose logs -f producer
docker compose logs -f dashboard

# Shut down
docker compose down -v
```

## Testing

```bash
# Unit tests (149 tests)
docker compose --profile test run --rm --build test

# E2E verification (43 checks)
docker compose up -d --build
# Wait for services to be healthy
docker compose --profile e2e run --rm --build e2e

# Fault tolerance test (stop a broker, verify cluster continues)
docker compose --profile fault-test run --rm --build fault-test

# Throughput benchmark (validates 100K+ msg/sec)
docker compose --profile benchmark run --rm --build benchmark

# Clean up
docker compose down -v
```

## Makefile Targets

| Target | Description |
|---|---|
| `make build` | Build all Docker images |
| `make up` | Start all services (build + detach) |
| `make down` | Stop all services |
| `make infra-up` | Start only Kafka infrastructure (ZooKeeper, brokers, init, UI) |
| `make infra-down` | Stop infrastructure |
| `make test` | Run unit tests (149 tests) |
| `make e2e` | Run full E2E verification (43 checks) |
| `make fault-test` | Run fault tolerance test |
| `make benchmark` | Run throughput benchmark |
| `make logs` | Tail all service logs |
| `make clean` | Full teardown (remove volumes and local images) |

## What I Learned

- **Dual-listener pattern:** Kafka brokers need separate INTERNAL (Docker network, port 29092) and EXTERNAL (host-mapped, ports 9092-9094) listeners for containers and host clients to coexist. The `KAFKA_ADVERTISED_LISTENERS` setting is critical -- containers must connect via the internal listener name (e.g., `kafka-1:29092`) while host tools use `localhost:9092`.

- **Consumer groups:** Independent consumer groups (`dashboard-consumer`, `error-aggregator-consumer`) read the same topics at their own pace without interference. Kafka tracks offsets per group, so adding a new consumer group replays from the beginning without affecting existing ones.

- **Replication factor vs min.insync.replicas:** RF=3 means 3 copies of every partition exist across brokers. `min.insync.replicas=2` means at least 2 replicas must acknowledge a write before it is considered committed. Combined with `acks=all`, this tolerates 1 broker failure without data loss or write disruption.

- **Key-based partitioning:** Using `user_id` as the partition key guarantees all events for a given user land in the same partition, preserving per-user ordering. This is essential for debugging user journeys across log entries.

- **ISR (In-Sync Replicas):** Kafka maintains an ISR set for each partition -- only replicas that are caught up with the leader participate in writes. When a broker falls behind or goes down, it drops out of the ISR. The cluster continues as long as `min.insync.replicas` are still in sync.

- **SSE (Server-Sent Events):** Simpler than WebSockets for one-way server-to-client streaming. Native browser support via `EventSource`, automatic reconnection, and no need for a separate protocol upgrade. Heartbeats (`: heartbeat\n\n`) keep connections alive through proxies.

- **confluent-kafka performance:** With proper batching (`batch.size=200KB`), compression (`lz4`), and tuning (`linger.ms=100`), the Python confluent-kafka library (backed by librdkafka) easily exceeds 100K+ msg/sec on a single producer thread.

## API / Dashboard

| Endpoint | Description |
|---|---|
| `GET /` | Live dashboard UI with real-time log stream via SSE |
| `GET /api/logs` | Recent logs (JSON array) |
| `GET /api/stats` | Aggregated log statistics (total, by_service, by_level, msg/sec) |
| `GET /api/errors` | Recent error logs + error counts + error rate |
| `GET /api/metrics` | Throughput history, consumer lag, latency stats |
| `GET /api/ordering` | Message ordering verification (per partition key) |
| `GET /api/stream` | SSE endpoint for real-time log streaming |
| `GET /health` | Health check with consumer status |

## Troubleshooting

| Issue | Solution |
|---|---|
| Port 8000/8080/9092 already in use | Stop other services occupying those ports, or change the port mapping in `docker-compose.yml` |
| Brokers slow to start | Normal -- brokers take 30-60 seconds to become healthy. Health checks and `depends_on` conditions handle this automatically |
| Consumer lag on first start | Expected behavior. Consumers start from the earliest offset and catch up quickly |
| `docker-compose` command not found | Use `docker compose` (v2 plugin syntax) instead of `docker-compose` (v1 standalone) |
| Kafka init fails | Ensure all 3 brokers are healthy before `kafka-init` runs. The script retries, but if brokers never start, check Docker resource limits |
| Dashboard shows no logs | Verify the producer is running (`docker compose logs producer`). Producers run for a configurable duration (default 60s) then exit |

# Exactly-Once Kafka Transaction Processor

A banking transaction processing system that guarantees exactly-once message processing semantics using Kafka's idempotent producers, transactional writes, and committed-read consumers — even under failures.

## Tech Stack

- **Language**: Python 3.12
- **Message Broker**: Apache Kafka (KRaft mode, 3-broker cluster)
- **Database**: PostgreSQL 16
- **Web Dashboard**: Flask + Socket.IO
- **Kafka Client**: confluent-kafka (librdkafka-based)
- **ORM / DB Access**: SQLAlchemy
- **Containerization**: Docker & Docker Compose

## Architecture

```
┌─────────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
│  Transactional   │────▶│     Kafka Cluster        │────▶│  Exactly-Once    │
│  Producer        │     │  (3 brokers, KRaft)      │     │  Consumer        │
│                  │     │                           │     │                  │
│ • Idempotent     │     │  Topics:                  │     │ • read_committed │
│ • Transactional  │     │  • transactions.pending   │     │ • Consumer group │
│ • Atomic writes  │     │  • transactions.completed │     │ • Offset commit  │
│   across topics  │     │  • transactions.dlq       │     │   within txn     │
└─────────────────┘     └─────────────────────────┘     └────────┬─────────┘
                                                                  │
                                                                  ▼
┌─────────────────┐                                      ┌──────────────────┐
│  Flask Dashboard │◀─────────────── reads ──────────────│   PostgreSQL     │
│                  │                                      │                  │
│ • Real-time stats│                                      │ • Accounts       │
│ • Txn history    │                                      │ • Transactions   │
│ • Consumer lag   │                                      │ • Idempotency    │
│ • Failure inject │                                      │   keys           │
└─────────────────┘                                      └──────────────────┘
        ▲
        │
┌───────┴─────────┐
│  Transaction     │
│  Monitor         │
│                  │
│ • Stuck txn      │
│   detection      │
│ • Consumer lag   │
│   tracking       │
│ • Alert on       │
│   anomalies      │
└─────────────────┘
```

### Processes

| Process | Role |
|---|---|
| **Transactional Producer** | Generates simulated banking transactions (transfers, deposits, withdrawals). Uses Kafka idempotent + transactional APIs to atomically write to `transactions.pending`. Retries on transient failures without duplicates. |
| **Exactly-Once Consumer** | Consumes from `transactions.pending` with `isolation.level=read_committed`. Processes each transaction (balance checks, DB writes) and commits offsets within the same Kafka transaction. Deduplicates via idempotency keys stored in PostgreSQL. |
| **Transaction Monitor** | Periodically checks for stuck/orphaned transactions, tracks consumer group lag, detects anomalies (double-spends, negative balances), and publishes alerts. |
| **Flask Dashboard** | Web UI showing real-time transaction throughput, success/failure rates, consumer lag, account balances, and a failure injection panel for testing exactly-once guarantees. |

## Key Concepts Demonstrated

- **Idempotent Producer**: `enable.idempotence=true` ensures no duplicate messages even on retries
- **Transactional Writes**: `init_transactions()` / `begin_transaction()` / `commit_transaction()` for atomic multi-topic writes
- **Read-Committed Consumer**: `isolation.level=read_committed` so consumers never see uncommitted (aborted) messages
- **Consume-Transform-Produce (EOS)**: Consumer offset commits and producer writes in the same transaction — the core exactly-once pattern
- **Idempotency Keys**: Application-level dedup in PostgreSQL as a second safety net
- **Dead Letter Queue**: Failed transactions routed to `transactions.dlq` for inspection
- **Failure Injection**: Simulate broker crashes, consumer restarts, and network partitions to prove exactly-once holds

## How to Run

```bash
# Build and start all services
docker-compose up --build -d

# Check all containers are healthy
docker-compose ps

# View producer logs
docker-compose logs -f producer

# View consumer logs
docker-compose logs -f consumer

# Open the dashboard
open http://localhost:5050

# Inject a failure (consumer crash + restart)
curl -X POST http://localhost:5050/api/inject-failure/consumer-crash

# Verify exactly-once: check for duplicate or missing transactions
curl http://localhost:5050/api/verify-eos

# Tear down
docker-compose down -v
```

## What I Learned

- How Kafka's idempotent producer uses sequence numbers and producer IDs to deduplicate at the broker level
- The difference between `read_uncommitted` and `read_committed` isolation levels in Kafka consumers
- How the consume-transform-produce pattern achieves end-to-end exactly-once semantics
- Why `transaction.timeout.ms` and `max.block.ms` tuning matters for transactional producers
- How application-level idempotency keys complement Kafka's exactly-once guarantees
- The failure modes that can break at-least-once but not exactly-once (and vice versa)
- How to use KRaft mode (no ZooKeeper) for a modern Kafka cluster setup

# Kafka Log Compaction State Manager

A system that uses Kafka's log compaction to efficiently manage user profile state by keeping only the latest value per key, with tombstone-based deletion and a real-time monitoring dashboard.

## Tech Stack

- **Language**: Python 3.11+
- **Message Broker**: Apache Kafka (with log compaction enabled)
- **Kafka Client**: confluent-kafka
- **Web Dashboard**: Flask + Chart.js
- **Containerization**: Docker & Docker Compose

## Architecture

```
┌──────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│  Profile Producer │────▶│   Kafka (Compacted   │────▶│  State Consumer  │
│  (state changes)  │     │      Topic)          │     │  (rebuilds state)│
└──────────────────┘     └─────────────────────┘     └────────┬─────────┘
                                                              │
                                                              ▼
                                                     ┌──────────────────┐
                                                     │  Web Dashboard   │
                                                     │  (Flask + metrics)│
                                                     └──────────────────┘
```

### Components

1. **Profile Producer** — Generates user profile state changes (create, update, delete). Sends keyed messages to a compacted Kafka topic where the key is the user ID. Deletions are sent as tombstone records (null value).

2. **Compacted Kafka Topic** — Configured with `cleanup.policy=compact` so Kafka retains only the latest value per key. Old superseded values are removed during log compaction, keeping the topic size bounded.

3. **State Consumer** — Reads the compacted topic from the beginning to rebuild the full current state of all user profiles into an in-memory store. Continues consuming new updates in real time.

4. **Web Dashboard** — Flask-based dashboard serving real-time metrics: total profiles, active vs deleted, compaction lag, messages per second, and state store contents.

## How It Runs

Long-lived process — a Docker Compose stack runs:
- **Kafka infrastructure** (Zookeeper + Kafka broker with compaction settings)
- **Profile Producer** generating continuous profile state changes
- **State Consumer** rebuilding and maintaining current state from compacted logs
- **Web Dashboard** serving real-time compaction metrics on a local port

## Key Concepts Explored

- **Log compaction** — How Kafka retains only the latest record per key
- **Tombstone records** — Sending null values to mark keys for deletion
- **State rebuilding** — Consuming from the beginning to reconstruct current state
- **Compaction monitoring** — Tracking compaction lag, dirty ratio, and cleaner metrics
- **Idempotent state management** — Replaying a compacted log produces the same state

## How to Run

```bash
# Start full stack (Kafka + app + dashboard)
make up

# View logs
make logs

# Run unit tests in Docker
make test

# Run E2E verification
make e2e

# Run performance benchmarks
make benchmark

# Stop and clean up
make down

# Full cleanup (remove images and volumes)
make clean
```

- Dashboard available at `http://localhost:5555`
- Kafka broker at `localhost:9092`

## What I Learned

- **Log compaction requires tuning segment.bytes**: Kafka's default 1GB segment size means compaction rarely triggers in a dev environment. Setting segment.bytes to 1MB forces frequent compaction, making the behavior observable.
- **Tombstones are just null-valued messages**: Producing a message with a key and null value is Kafka's native deletion mechanism. The log compactor retains tombstones for `delete.retention.ms` before removing them.
- **State rebuild from compacted topics is fast**: Reading a compacted topic from the beginning to reconstruct current state is efficient because compaction has already removed superseded records.
- **Idempotent producers prevent duplicates without transactions**: Setting `enable.idempotence=True` gives exactly-once semantics for single-partition writes via producer ID + sequence numbers, without the overhead of full transactions.
- **Consumer assign() vs subscribe()**: For state rebuild, `assign()` to specific partitions at offset 0 gives deterministic reads. For live consumption, `subscribe()` enables consumer group coordination.
- **Compaction monitoring requires watermark analysis**: The gap between high and low watermarks, combined with unique key count, reveals how effectively compaction is reducing storage.
- **min.cleanable.dirty.ratio controls compaction frequency**: A low ratio (0.1) makes the log cleaner more aggressive, which is essential for demos but would increase I/O in production.

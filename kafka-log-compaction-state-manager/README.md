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
docker-compose up --build
```

- Dashboard available at `http://localhost:5555`
- Kafka broker at `localhost:9092`

## What I Learned

_(To be filled after implementation)_

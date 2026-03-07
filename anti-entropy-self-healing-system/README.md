# Anti-Entropy Self-Healing System

A distributed system that detects and repairs data inconsistencies across 3 storage node containers using Merkle trees, scheduled scans, and read repair.

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** Flask + Flask-SocketIO
- **Scheduling:** APScheduler (background anti-entropy scans)
- **HTTP Client:** httpx (node communication)
- **Monitoring:** Prometheus client
- **Logging:** structlog
- **Testing:** pytest
- **Infrastructure:** Docker, Docker Compose

## Architecture

```
                    +----------------------------------+
                    |     Coordinator (port 5050)       |
                    |  Scanner | Repair | Read Repair   |
                    |  Dashboard | Metrics | API        |
                    +----------------+-----------------+
                                     | httpx
                    +----------------+----------------+
                    |                |                 |
              +-----+----+   +------+---+   +--------+-+
              | node-a   |   | node-b   |   | node-c   |
              | port 8001|   | port 8002|   | port 8003|
              | Flask    |   | Flask    |   | Flask    |
              | Store    |   | Store    |   | Store    |
              | Merkle   |   | Merkle   |   | Merkle   |
              +----------+   +----------+   +----------+
```

## How to Run

```bash
# Build and start the system (3 nodes + coordinator)
make run

# View logs
make logs

# Stop the system
make stop
```

The dashboard is available at **http://localhost:5050/**

## How to Test

```bash
# Run unit tests in Docker
make test

# Run full E2E verification
make e2e

# Seed test data
make seed
```

## API Endpoints

### Coordinator (port 5050)

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Dashboard UI |
| `/health` | GET | Coordinator health |
| `/api/status` | GET | System health + node connectivity |
| `/api/data/<key>` | GET | Read with read repair |
| `/api/data/<key>` | PUT | Write to all nodes |
| `/api/scan/trigger` | POST | Trigger immediate anti-entropy scan |
| `/api/metrics` | GET | Consistency metrics (JSON) |
| `/api/replicas` | GET | Node health statuses |
| `/api/inject` | POST | Inject inconsistency (testing) |
| `/metrics` | GET | Prometheus metrics |

### Storage Nodes (ports 8001-8003)

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Node health |
| `/data/<key>` | GET/PUT | Read/write entry |
| `/data` | GET | All entries |
| `/keys` | GET | List keys |
| `/merkle/root` | GET | Merkle tree root hash |
| `/merkle/leaves` | GET | Merkle tree leaf hashes |

## What I Learned

- **Merkle trees** enable efficient consistency checking -- comparing root hashes lets you skip detailed comparison when nodes agree, and leaf-level diffing pinpoints exactly which keys diverge
- **Anti-entropy scans** as a background process ensure eventual consistency even without client-driven repair
- **Read repair** catches inconsistencies inline during normal reads, complementing scheduled scans
- **Conflict resolution strategies** (latest-write-wins, highest-version) determine which value "wins" -- the choice depends on the application's consistency requirements
- **Multi-container Docker architecture** with health checks and dependency ordering ensures services start in the right order
- **Prometheus metrics** provide observability into the self-healing process -- tracking comparisons, inconsistencies, and repair rates

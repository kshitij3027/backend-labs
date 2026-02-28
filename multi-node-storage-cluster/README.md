# Multi-Node Storage Cluster

A 3-node distributed storage cluster for log files with automatic replication, health monitoring, quorum-based writes, and failure handling. Built from scratch in Python to explore the fundamentals of distributed storage systems.

---

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** Flask (REST API per node + dashboard)
- **CLI:** Click
- **Inter-node Communication:** HTTP via `requests` library
- **Storage:** Local filesystem (JSON files, one directory per node)
- **Concurrency:** Threading for background replication, health checks, and read-repair
- **Infrastructure:** Docker, Docker Compose

---

## Architecture

```
                        +----------------+
                        |  Web Dashboard |
                        |  (Flask UI)    |
                        |  :8080         |
                        +-------+--------+
                                |
                        +-------+--------+
                        | Cluster Manager|
                        | (health, ring) |
                        +--+----+----+---+
                           |    |    |
              +------------+    |    +------------+
              v                 v                 v
      +-------------+  +-------------+  +-------------+
      |   Node 1    |<->|   Node 2    |<->|   Node 3    |
      |  Flask API  |  |  Flask API  |  |  Flask API  |
      |  :5001      |  |  :5002      |  |  :5003      |
      | +---------+ |  | +---------+ |  | +---------+ |
      | |FileStore| |  | |FileStore| |  | |FileStore| |
      | +---------+ |  | +---------+ |  | +---------+ |
      +-------------+  +-------------+  +-------------+
```

### Components

| Component | Description |
|---|---|
| **Storage Node** (x3) | Flask HTTP server that stores log files on local disk. Exposes REST APIs for read, write, list, replication, and cluster status. |
| **FileStore** | Thread-safe file-based storage engine. Handles writes, reads, replica ingestion, file rotation, and storage stats. |
| **Cluster Manager** | Tracks node membership, runs background health checks, detects failures, and maintains the consistent hash ring. |
| **Replication Manager** | Async replication with configurable quorum. Implements hinted handoff for temporarily failed nodes and background read-repair. |
| **Consistent Hash Ring** | Maps files to primary nodes using consistent hashing with virtual nodes for even distribution. |
| **Version Manager** | Tracks file versions per node for conflict detection during replication and read-repair. |
| **Dashboard** | Real-time web UI showing cluster health, node status, file counts, and providing a write interface. |
| **Cluster Client** | Python client library for programmatic interaction with the cluster (write, read, health). |
| **CLI** | Click-based command-line tool for writing logs, reading files, listing files, and checking cluster status. |

---

## Key Features

- **Leaderless replication** -- any node accepts writes, no single point of failure for ingestion
- **Consistent hashing** for file-to-node mapping with virtual nodes for even data distribution
- **Async replication** with hinted handoff for temporarily unavailable nodes
- **Quorum-based writes** -- 2 of 3 nodes must acknowledge before success is returned
- **Background read-repair** -- stale or missing replicas are repaired on read
- **Health monitoring** with periodic heartbeats and automatic failure detection
- **Real-time web dashboard** for cluster visibility
- **File rotation** and storage management to keep disk usage under control
- **Load testing** with configurable concurrency via ThreadPoolExecutor

---

## How to Run

```bash
# Start the full cluster (3 nodes + dashboard)
docker compose up --build -d

# Run unit tests in Docker
make test

# Run end-to-end tests (includes degraded mode and recovery)
make e2e

# Run load tests (100 requests, 10 concurrent workers)
make loadtest

# View dashboard
open http://localhost:8080

# Write a log entry
curl -X POST http://localhost:5001/write \
  -H "Content-Type: application/json" \
  -d '{"message":"hello","level":"info"}'

# Read a file
curl http://localhost:5001/read/<file_path>

# List files on a node
curl http://localhost:5001/files

# Check cluster status
curl http://localhost:5001/cluster/status

# Check cluster status via dashboard
curl http://localhost:8080/api/cluster

# Stop the cluster
make stop
```

---

## API Reference

### Storage Node Endpoints (ports 5001-5003)

| Method | Endpoint | Description |
|--------|---|---|
| `GET` | `/health` | Node health check |
| `POST` | `/write` | Store a new log entry (JSON body) |
| `GET` | `/read/<file_path>` | Read a stored file by path |
| `GET` | `/files` | List all stored files on this node |
| `POST` | `/replicate` | Receive a replicated file from another node |
| `GET` | `/stats` | Node operation stats (writes, reads, replications) |
| `GET` | `/replication/status` | Replication manager status and pending hints |
| `GET` | `/cluster/status` | Cluster-wide status from this node's perspective |
| `GET` | `/cluster/nodes` | List all known nodes and their health |

### Dashboard Endpoints (port 8080)

| Method | Endpoint | Description |
|--------|---|---|
| `GET` | `/` | Web dashboard UI |
| `GET` | `/api/cluster` | Aggregated cluster status (all nodes) |
| `GET` | `/api/files` | Files across all nodes |
| `GET` | `/api/health` | Dashboard health check |
| `POST` | `/api/write` | Write a log entry (round-robin to nodes) |

---

## Project Structure

```
multi-node-storage-cluster/
├── README.md
├── Makefile
├── Dockerfile                    # Production image for storage nodes + dashboard
├── Dockerfile.test               # Test image (pytest + load test runner)
├── docker-compose.yml            # 3 nodes, dashboard, test, load-test services
├── requirements.txt
├── .env.example
├── .gitignore
├── scripts/
│   └── load_test.py              # ThreadPoolExecutor-based load tester
├── src/
│   ├── __init__.py
│   ├── __main__.py               # Entry point (node or dashboard mode)
│   ├── cli.py                    # Click CLI for cluster interaction
│   ├── cluster_client.py         # Python client library
│   ├── cluster_manager.py        # Node membership, health checks, hash ring
│   ├── config.py                 # Cluster configuration from env vars
│   ├── consistent_hash.py        # Consistent hashing ring implementation
│   ├── dashboard.py              # Flask web dashboard
│   ├── file_store.py             # File-based storage engine with rotation
│   ├── replication.py            # Async replication, quorum, hinted handoff
│   ├── storage_node.py           # Flask REST API for a single node
│   ├── versioning.py             # Version tracking for conflict resolution
│   └── templates/
│       └── dashboard.html        # Dashboard web UI template
└── tests/
    ├── __init__.py
    ├── conftest.py               # Shared pytest fixtures
    ├── test_cluster_client.py
    ├── test_cluster_manager.py
    ├── test_config.py
    ├── test_consistent_hash.py
    ├── test_dashboard.py
    ├── test_file_store.py
    ├── test_replication.py
    ├── test_storage_node.py
    └── test_versioning.py
```

---

## What I Learned

- **Leaderless replication tradeoffs:** Leaderless designs are simpler to implement (no leader election, no failover logic) but make consistency harder. Every node must coordinate independently, and conflicting writes need version-based resolution.

- **Consistent hashing for data distribution:** A hash ring with virtual nodes distributes files evenly across the cluster. Adding or removing a node only remaps a fraction of keys, unlike naive modulo-based hashing which reshuffles everything.

- **Hinted handoff for temporary failures:** When a target replica node is down, the writing node stores a "hint" and replays it when the node recovers. This avoids data loss during transient failures without requiring immediate re-replication to other nodes.

- **Quorum writes (W=2, N=3):** Requiring 2 of 3 nodes to acknowledge a write balances availability and durability. A single node failure does not block writes, but two simultaneous failures will.

- **Read-repair for eventual consistency:** Checking replicas on read and repairing stale copies in the background is a lightweight way to converge toward consistency without a dedicated anti-entropy process.

- **Threading challenges:** Background replication, health checks, and read-repair all run in daemon threads. Thread-safe counters, proper locking around shared state, and careful shutdown ordering were essential to avoid race conditions.

- **Docker networking for multi-container clusters:** Docker Compose networks let containers resolve each other by service name. Volume mounts persist storage across restarts. Profiles keep test and load-test services from starting by default.

---

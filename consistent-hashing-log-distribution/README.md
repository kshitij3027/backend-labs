# Consistent Hashing Log Distribution

A consistent hashing system that distributes log entries across multiple storage nodes using a virtual-node-based hash ring, supporting dynamic cluster scaling with minimal data movement.

## Tech Stack

- **Language**: Python 3.12+
- **Hashing**: hashlib SHA-1 (160-bit hash ring placement)
- **Web Dashboard**: Flask with Jinja2 templates (real-time monitoring UI)
- **Concurrency**: threading with RLock (node management, background tasks)
- **Configuration**: YAML-based cluster config with env var overrides
- **Containerization**: Docker + Docker Compose
- **Testing**: pytest (72 unit tests + E2E integration tests)
- **Serialization**: JSON (log entries, cluster state, API responses)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Cluster Coordinator                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  Hash Ring   │  │  Node Manager │  │  Metrics   │ │
│  │  (vnodes)    │  │  (add/remove) │  │  Collector │ │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                │                 │        │
│         ▼                ▼                 ▼        │
│  ┌─────────────────────────────────────────────────┐│
│  │              Log Distribution Engine             ││
│  └─────────────────────────────────────────────────┘│
└────────┬──────────────┬───────────────┬─────────────┘
         │              │               │
    ┌────▼────┐   ┌────▼────┐    ┌────▼────┐
    │ Node A  │   │ Node B  │    │ Node C  │
    │(vnodes) │   │(vnodes) │    │(vnodes) │
    │ [logs]  │   │ [logs]  │    │ [logs]  │
    └─────────┘   └─────────┘    └─────────┘

    ┌─────────────────────────────────┐
    │   Web Dashboard (Flask HTTP)    │
    │  - Ring visualization           │
    │  - Node stats & health          │
    │  - Log distribution metrics     │
    │  - Dynamic scaling controls     │
    └─────────────────────────────────┘
```

## How It Runs

**Long-lived process** — the cluster coordinator starts multiple storage nodes, accepts log entries for storage, supports dynamic node addition/removal, and exposes cluster metrics. A web dashboard served over HTTP provides real-time monitoring.

### Key Components

1. **Hash Ring** — Virtual-node-based consistent hash ring for even distribution
2. **Storage Nodes** — In-memory log storage with per-node capacity tracking
3. **Cluster Coordinator** — Manages the ring, routes logs, handles scaling events
4. **Data Migration** — Minimal-movement rebalancing when nodes join/leave
5. **Web Dashboard** — Flask-based HTTP UI for monitoring ring state and metrics

## How to Run

### Prerequisites

- Docker and Docker Compose

### Commands

```bash
# Build Docker images
make build

# Run 72 unit tests in Docker
make test

# Start the dashboard (accessible at http://localhost:8080)
make run

# Stop the application
make stop

# Run full end-to-end tests (health, log storage, scaling, ring ops)
make e2e

# Run benchmark demo with success criteria validation (10K logs, 3 nodes)
make demo

# Cleanup containers, volumes, and images
make clean
```

The dashboard port can be changed with `HOST_PORT=9090 make run`.

## API Docs

All endpoints return JSON.

### GET /health

Returns cluster health status.

```json
{ "status": "healthy", "cluster_name": "log-cluster", "node_count": 3, "total_logs": 150 }
```

### POST /api/logs

Store a single log entry or a batch of entries. Each entry requires `source`, `level`, and `message` fields.

```bash
# Single entry
curl -X POST http://localhost:8080/api/logs \
  -H "Content-Type: application/json" \
  -d '{"source":"web-server","level":"info","message":"request completed"}'

# Batch
curl -X POST http://localhost:8080/api/logs \
  -H "Content-Type: application/json" \
  -d '[{"source":"db","level":"error","message":"timeout"},{"source":"auth","level":"warn","message":"retry"}]'
```

Returns: `{ "stored": 2, "details": [...] }`

### GET /api/stats

Returns per-node distribution statistics and cluster-wide metrics.

```json
{
  "cluster_name": "log-cluster",
  "node_count": 3,
  "total_logs": 150,
  "nodes": {
    "node1": { "log_count": 48, "percentage": 32.0, "vnodes": 150 },
    "node2": { "log_count": 52, "percentage": 34.7, "vnodes": 150 },
    "node3": { "log_count": 50, "percentage": 33.3, "vnodes": 150 }
  }
}
```

### POST /api/nodes

Add a new storage node to the cluster. Triggers data migration from existing nodes.

```bash
curl -X POST http://localhost:8080/api/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node4"}'
```

Returns: `{ "status": "added", "node_id": "node4", "migrated_logs": 38 }`

### DELETE /api/nodes/{id}

Remove a node from the cluster. Its logs are redistributed to remaining nodes.

```bash
curl -X DELETE http://localhost:8080/api/nodes/node4
```

Returns: `{ "status": "removed", "node_id": "node4", "redistributed_logs": 38 }`

### GET /api/ring

Returns hash ring visualization data including virtual node positions and node color mappings.

```json
{
  "vnodes": [{ "position": 0.0234, "node_id": "node1" }, ...],
  "node_colors": { "node1": "#e74c3c", "node2": "#2ecc71", "node3": "#3498db" }
}
```

### POST /api/simulate

Generate random log entries for testing.

```bash
curl -X POST http://localhost:8080/api/simulate \
  -H "Content-Type: application/json" \
  -d '{"count":100}'
```

Returns: `{ "generated": 100, "distribution": { "node1": 33, "node2": 34, "node3": 33 } }`

## Success Criteria

The benchmark demo (`make demo`) validates these performance targets:

| Metric | Target |
|--------|--------|
| 100K hash lookups | < 2 seconds |
| 3-node distribution | Within +/-5% of 33.3% each |
| Adding a 4th node | ~25% data movement (1/n) |
| Ring update latency | < 50ms for add/remove |
| Data loss during scaling | Zero |

## What I Learned

- **Virtual nodes are essential for balance.** Without them, a small number of physical nodes creates hotspots on the ring. Using 150 virtual nodes per physical node produces distributions consistently within +/-5% of ideal.

- **SHA-1 provides better uniformity than MD5.** SHA-1's 160-bit output space distributes virtual nodes more evenly across the ring, reducing clustering artifacts visible with shorter hashes.

- **Minimal data movement is the core advantage.** When adding a node to an n-node cluster, only ~1/n of the data needs to migrate. This is what makes consistent hashing superior to modular hashing (where nearly all keys would remap).

- **RLock is needed for thread safety.** The hash ring is read during log routing and written during node add/remove. A reentrant lock (RLock) is necessary because some operations (like migration) need to call other ring methods while already holding the lock.

- **Arc-length estimation approximates load.** Each node's expected load can be estimated by the total arc length it covers on the ring (the sum of gaps between its virtual nodes and the preceding ones). This gives a O(1) load estimate without scanning all stored logs.

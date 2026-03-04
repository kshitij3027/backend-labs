# Self-Healing Cluster Membership

A distributed cluster membership system with gossip-based state dissemination, phi accrual failure detection, deterministic leader election, and network partition tolerance. Built from scratch in Python using only aiohttp for HTTP communication and orjson for serialization.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ClusterMember (node.py)               │
│  Orchestrates all components for a single cluster node   │
├──────────┬──────────┬───────────┬────────────┬──────────┤
│  HTTP    │  Gossip  │  Health   │  Failure   │  Leader  │
│  Server  │ Protocol │  Monitor  │  Detector  │ Election │
│          │          │           │  (Phi)     │          │
├──────────┴──────────┴───────────┴────────────┴──────────┤
│              MembershipRegistry (shared state)           │
└─────────────────────────────────────────────────────────┘

Node-to-Node Communication (HTTP):
  POST /gossip     - Gossip digest exchange
  POST /heartbeat  - Heartbeat signals
  POST /join       - Join request with cluster digest response
  GET  /health     - Node health status
  GET  /membership - Full cluster membership view
```

### Data Flow

```
                ┌──────────┐
   heartbeat    │  Node A  │    gossip digest
   ──────────►  │          │  ◄──────────────
                │ Registry │
   /join req    │  Health  │    phi accrual
   ──────────►  │  Gossip  │  ──────────────►
                │ Election │     detect fail
                └──────────┘
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
    ┌─────────┐ ┌─────────┐ ┌─────────┐
    │ Node B  │ │ Node C  │ │ Node D  │
    └─────────┘ └─────────┘ └─────────┘
```

## Tech Stack

- **Language**: Python 3.12
- **HTTP**: aiohttp (async HTTP client/server)
- **Serialization**: orjson (fast JSON)
- **Container**: Docker + Docker Compose
- **Testing**: pytest + pytest-asyncio + aioresponses

## How to Run

### Prerequisites

- Docker and Docker Compose

### Start the 5-Node Cluster

```bash
# Build and start all 5 nodes
make run

# View logs
make logs

# Stop the cluster
make stop
```

### Run Unit Tests (in Docker)

```bash
make test
```

### Run E2E Tests

```bash
make e2e
```

This starts the cluster, waits for stabilization, then runs the full E2E verification suite (cluster formation, leader election, failure detection, leader re-election, node rejoin, gossip convergence, and network partition handling).

### Manual Inspection

Once the cluster is running (`make run`), query any node:

```bash
# Check node health
curl http://localhost:5001/health

# View cluster membership from node-1's perspective
curl http://localhost:5001/membership

# View from node-3
curl http://localhost:5003/membership
```

## API Endpoints

Each node exposes the following HTTP endpoints on its mapped port:

| Method | Path          | Description                              |
|--------|---------------|------------------------------------------|
| GET    | `/health`     | Node status, role, incarnation, heartbeats |
| GET    | `/membership` | All known cluster members and statuses   |
| POST   | `/gossip`     | Receive gossip digest from a peer        |
| POST   | `/heartbeat`  | Receive heartbeat from a peer            |
| POST   | `/join`       | Join request; returns cluster digest     |

**Port mapping** (host:container): node-1=5001:5000, node-2=5002:5000, ..., node-5=5005:5000

## Configuration

All configuration is via environment variables:

| Variable                           | Default   | Description                                      |
|------------------------------------|-----------|--------------------------------------------------|
| `NODE_ID`                          | `node-1`  | Unique identifier for this node                  |
| `ADDRESS`                          | `0.0.0.0` | Listen address                                   |
| `PORT`                             | `5000`    | Listen port                                      |
| `ROLE`                             | `worker`  | Initial role (`leader` or `worker`)              |
| `SEED_NODES`                       | (empty)   | Comma-separated `host:port` list of seed nodes   |
| `GOSSIP_INTERVAL`                  | `2.0`     | Seconds between gossip rounds                    |
| `HEALTH_CHECK_INTERVAL`            | `1.0`     | Seconds between health checks                    |
| `PHI_THRESHOLD`                    | `8.0`     | Phi value above which a node is declared FAILED   |
| `GOSSIP_FANOUT`                    | `3`       | Number of peers to gossip with each round        |
| `SUSPECTED_HEALTH_CHECK_MULTIPLIER`| `0.5`     | Health check interval multiplier for suspected nodes |
| `HEARTBEAT_WINDOW_SIZE`            | `20`      | Sliding window size for phi calculation           |
| `CLEANUP_INTERVAL`                 | `30.0`    | Seconds between cleanup cycles                   |

See `.env.example` for a sample configuration.

## Key Algorithms

### Phi Accrual Failure Detection

Instead of a fixed heartbeat timeout, each node tracks the inter-arrival times of heartbeats from every peer in a sliding window. The phi value is computed as:

```
phi = time_since_last_heartbeat / mean_inter_arrival_interval
```

- phi < 1.0: normal operation
- 1.0 <= phi < threshold: node is SUSPECTED (checked more frequently)
- phi >= threshold (default 8.0): node is declared FAILED

This adapts automatically to network conditions -- a node on a slow link gets more lenient treatment.

### SWIM-Style Gossip

Each node periodically selects up to `gossip_fanout` random peers and sends its full membership digest. Incoming digests are merged using SWIM rules:

1. **Higher incarnation always wins** -- a node that has bumped its incarnation overrides any stale state.
2. **At the same incarnation, worse status wins** -- FAILED > SUSPECTED > HEALTHY. This ensures failure information propagates reliably.
3. **Unknown nodes are added** -- new members discovered through gossip are immediately registered.

### Incarnation-Based Refutation

When a node receives gossip claiming it is SUSPECTED or FAILED, it refutes the claim by incrementing its incarnation number and re-announcing itself as HEALTHY. The higher incarnation ensures this refutation propagates through the cluster and overrides the stale suspicion.

### Deterministic Leader Election

Leader election uses a simple deterministic rule: the healthy node with the highest node ID wins. Since gossip ensures eventual consistency of membership views, all nodes converge on the same leader without needing a voting protocol. The majority check ensures only the majority partition can elect a leader during a network split.

### Network Partition Tolerance

Two majority checks prevent split-brain scenarios:

1. **Health Monitor**: Before marking a node as FAILED, the monitor checks whether it can reach a majority of known nodes (reachable > total/2). Minority partitions cannot mark nodes as failed.
2. **Leader Election**: Before electing a leader, the system verifies that healthy nodes form a majority of the total cluster. This prevents both sides of a partition from independently electing a leader.

## Test Commands

```bash
# Unit tests in Docker (120+ tests)
make test

# Full E2E suite (7 tests: formation, election, failure detection,
# re-election, rejoin, convergence, partition)
make e2e

# Clean up all containers and images
make clean
```

## What I Learned

- **Phi accrual detection** is much more practical than fixed timeouts for real networks with varying latency. The sliding window of inter-arrival times lets each node adapt to its own network conditions.
- **Incarnation numbers** are the key mechanism that makes gossip-based membership work correctly -- without them, a rejoining node's HEALTHY status would lose to stale FAILED gossip at the same incarnation.
- **Majority quorum** is essential for partition tolerance. Without it, both sides of a network split will independently mark the other side as failed and elect their own leaders, causing split-brain.
- **SWIM merge rules** (higher incarnation wins, worse status wins at same incarnation) are elegant because they guarantee convergence while ensuring failure information is never accidentally overridden by stale health reports.
- **Adaptive health checking** (checking suspected nodes more frequently) improves detection speed without adding unnecessary load during normal operation.
- **Docker network disconnect/connect** is a practical way to simulate network partitions in E2E tests without modifying application code.

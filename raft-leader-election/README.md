# Raft Leader Election

A multi-node cluster system that implements the Raft consensus algorithm to elect a single leader among nodes, with automatic failover when the leader fails.

## Tech Stack

- **Language**: Python 3.12
- **RPC**: gRPC (inter-node communication)
- **Web UI**: Flask + Socket.IO (real-time visualization)
- **Serialization**: Protocol Buffers
- **Testing**: pytest
- **Containerization**: Docker Compose (multi-node orchestration)

## Architecture

The system implements the core Raft leader election protocol:

- **Node States**: Each node operates as a Follower, Candidate, or Leader
- **Election Timeout**: Followers that don't hear from a leader start an election
- **Vote Requests**: Candidates request votes from all other nodes
- **Majority Wins**: First candidate to receive a majority of votes becomes leader
- **Heartbeats**: The leader sends periodic heartbeats to maintain authority
- **Term Numbers**: Monotonically increasing terms prevent stale leaders

### Components

```
raft-leader-election/
├── src/
│   ├── node.py              # Core Raft node (state machine, timers)
│   ├── rpc_server.py        # gRPC server (handles incoming RPCs)
│   ├── rpc_client.py        # gRPC client (sends RPCs to peers)
│   ├── election.py          # Election logic (timeouts, vote counting)
│   ├── heartbeat.py         # Leader heartbeat mechanism
│   ├── config.py            # Cluster configuration loader
│   └── proto/
│       └── raft.proto       # Protocol Buffer definitions
├── web/
│   ├── app.py               # Flask + Socket.IO web server
│   ├── templates/
│   │   └── index.html       # Real-time cluster visualization
│   └── static/
│       ├── css/
│       └── js/
├── tests/
│   ├── test_config.py       # Configuration tests
│   ├── test_election.py     # Election logic tests
│   ├── test_heartbeat.py    # Heartbeat mechanism tests
│   ├── test_node.py         # Node state machine tests
│   ├── test_partition.py    # Network partition tests
│   ├── test_priority.py     # Priority election & pre-vote tests
│   └── test_rpc.py          # gRPC round-trip tests
├── scripts/
│   └── verify_cluster.py    # E2E cluster verification
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .gitignore
├── .env.example
└── README.md
```

### Data Flow

```
┌─────────────────────────────────────────────────────┐
│                   Web Dashboard                      │
│          (Flask + Socket.IO, port 8080)              │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│   │ Node 1  │  │ Node 2  │  │ Node 3  │  ... N     │
│   │ [state] │  │ [state] │  │ [state] │            │
│   └─────────┘  └─────────┘  └─────────┘            │
└─────────────────────────────────────────────────────┘
        │                │               │
        ▼                ▼               ▼
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node 1  │────▶│  Node 2  │────▶│  Node 3  │
│ (gRPC)   │◀────│ (gRPC)   │◀────│ (gRPC)   │
│ port 5001│     │ port 5002│     │ port 5003│
└──────────┘     └──────────┘     └──────────┘
     │                                  │
     └──── RequestVote / AppendEntries ─┘
           (heartbeats & elections)
```

## How to Run

### Start the cluster

```bash
make run        # Builds and starts 5 Raft nodes + web dashboard
```

- Dashboard: http://localhost:8080
- Nodes: ports 5001-5005 (gRPC)

### Run tests

```bash
make test       # Unit tests (97 tests) inside Docker
make e2e        # End-to-end: spins up cluster, verifies election + failure recovery
```

### Other commands

```bash
make logs       # Stream cluster logs
make stop       # Tear down all containers
make clean      # Remove images and build artifacts
```

### Dashboard features

- Real-time node state visualization (Leader=green, Follower=blue, Candidate=orange, Stopped=gray)
- **Kill Leader** button: stops the current leader and triggers automatic re-election
- **Partition [1,2] | [3,4,5]** button: simulates a network partition
- **Heal Partition** button: restores full connectivity
- Scrolling election log with timestamps

## Key Scenarios

1. **Initial Election**: All nodes start as followers; the first to timeout triggers an election
2. **Leader Failure**: When the leader stops sending heartbeats, followers detect the timeout and start a new election
3. **Split Vote**: If no candidate gets a majority, a new election begins with randomized timeouts
4. **Network Partition**: Nodes in the minority partition cannot elect a leader (no majority)
5. **Leader Rejoins**: A stale leader discovers a higher term and steps down to follower

## What I Learned

- **Randomized timeouts are the key insight** of Raft. By giving each node a different election timeout (150-300ms), split votes are rare without any coordination.
- **Term numbers act as a logical clock**. Every RPC carries a term; any node discovering a higher term immediately steps down. This single rule prevents stale leaders.
- **Pre-vote prevents unnecessary term inflation**. Without pre-vote, a partitioned node would keep incrementing its term. Pre-vote checks if an election could succeed before incrementing.
- **asyncio.Event is perfect for resettable timers**. Using `asyncio.wait_for(event.wait(), timeout=T)` gives a clean pattern: the timer fires on timeout, but resets instantly when the event is set.
- **gRPC async (grpc.aio) works well for this use case**. One event loop runs the server, election timer, and heartbeat loop via `asyncio.gather()` with no threading needed.
- **Strict majority = `(N // 2) + 1`**. Off-by-one errors here would break the single-leader guarantee. For 5 nodes, you need 3 votes, not 2.

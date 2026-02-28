# Multi-Node Storage Cluster

A 3-node distributed storage cluster that stores log files across multiple nodes, automatically replicates data between them, and handles node failures gracefully.

---

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** Flask (REST API per node)
- **Cluster Manager:** Custom Python orchestrator
- **Web Dashboard:** Flask + Jinja2 HTML templates
- **Inter-node Communication:** HTTP (requests library)
- **Storage:** Local filesystem (one directory per node)
- **Concurrency:** Threading for background replication and health checks

---

## Architecture

```
                        ┌──────────────┐
                        │  Web Dashboard│
                        │  (Flask UI)   │
                        └──────┬───────┘
                               │
                        ┌──────┴───────┐
                        │   Cluster    │
                        │   Manager    │
                        └──┬───┬───┬───┘
                           │   │   │
              ┌────────────┘   │   └────────────┐
              ▼                ▼                ▼
      ┌───────────┐    ┌───────────┐    ┌───────────┐
      │  Node 1   │◄──►│  Node 2   │◄──►│  Node 3   │
      │ Flask API │    │ Flask API │    │ Flask API │
      │ :5001     │    │ :5002     │    │ :5003     │
      └───────────┘    └───────────┘    └───────────┘
```

### Components

- **Storage Node (x3):** Each node is a Flask HTTP server that stores log files on its local filesystem and exposes REST APIs for reading, writing, listing, and deleting logs.
- **Cluster Manager:** Orchestrates the 3 nodes — tracks membership, monitors health via periodic heartbeats, triggers replication, and handles failover when a node goes down.
- **Web Dashboard:** A Flask-based UI that shows cluster health, node status, stored files, replication state, and allows manual file upload/download.

### Key Features

- **Automatic Replication:** When a log file is written to one node, it is automatically replicated to at least one other node (configurable replication factor).
- **Node Failure Handling:** If a node becomes unreachable, the cluster manager detects it via missed heartbeats and re-replicates its data to surviving nodes.
- **Consistent Hashing:** Files are assigned to primary nodes using consistent hashing, distributing data evenly across the cluster.
- **Read Repair:** On read, if a replica is found to be stale or missing, the system repairs it in the background.
- **Health Checks:** Each node exposes a `/health` endpoint; the cluster manager polls these periodically.

---

## How It Runs

Each storage node runs as a **long-lived Flask HTTP server**. The cluster manager starts all three nodes, monitors their health, and coordinates replication. The web dashboard runs as a separate Flask app for visibility.

### REST API (per node)

| Method | Endpoint             | Description                     |
|--------|----------------------|---------------------------------|
| POST   | `/logs`              | Store a new log file            |
| GET    | `/logs/<filename>`   | Retrieve a log file             |
| GET    | `/logs`              | List all stored log files       |
| DELETE | `/logs/<filename>`   | Delete a log file               |
| GET    | `/health`            | Node health check               |
| GET    | `/status`            | Node status (stored files, etc) |
| POST   | `/replicate`         | Receive a replicated file       |

### Cluster Manager API

| Method | Endpoint             | Description                     |
|--------|----------------------|---------------------------------|
| GET    | `/cluster/status`    | Overall cluster health          |
| GET    | `/cluster/nodes`     | List all nodes and their state  |
| POST   | `/cluster/rebalance` | Trigger manual rebalance        |

---

## How to Run

```bash
# 1. Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the cluster (starts 3 nodes + cluster manager + dashboard)
python cluster_manager.py

# 4. Open the dashboard
# Visit http://localhost:5000 in your browser
```

### Docker (coming soon)

```bash
docker-compose up --build
```

---

## Project Structure

```
multi-node-storage-cluster/
├── README.md
├── requirements.txt
├── .gitignore
├── cluster_manager.py        # Orchestrates nodes, health checks, replication
├── storage_node.py           # Flask server for a single storage node
├── replication.py            # Replication logic and consistency
├── consistent_hash.py        # Consistent hashing ring implementation
├── dashboard.py              # Web dashboard Flask app
├── templates/
│   └── dashboard.html        # Dashboard UI template
├── config.py                 # Cluster configuration (ports, replication factor)
├── tests/
│   ├── test_storage_node.py
│   ├── test_replication.py
│   ├── test_consistent_hash.py
│   └── test_cluster_manager.py
├── Dockerfile
└── docker-compose.yml
```

---

## What I Learned

_To be filled in after implementation._

---

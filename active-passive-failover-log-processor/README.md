# active-passive-failover-log-processor

A multi-node log processing service where one primary node handles all traffic while standby nodes monitor its health and automatically take over within 10 seconds if it fails.

## Tech Stack
- **Language:** Python 3.11+
- **HTTP framework:** FastAPI + Uvicorn
- **Coordination / shared state:** Redis (leader lock, heartbeats)
- **Inter-node health checks:** httpx
- **Orchestration:** Docker Compose (primary path) or K3d (alt)
- **Tests:** pytest + pytest-asyncio

## Architecture

```
                  ┌──────────────┐
        traffic → │   primary    │  (active — accepts log writes / queries)
                  │  node-1:8001 │
                  └──────┬───────┘
                         │ heartbeat + leader lock
                         ▼
                   ┌───────────┐
                   │   Redis   │  (lock key + last-heartbeat)
                   └─────┬─────┘
              ┌──────────┴──────────┐
              ▼                     ▼
       ┌──────────────┐      ┌──────────────┐
       │  standby-2   │      │  standby-3   │
       │  node-2:8002 │      │  node-3:8003 │
       └──────────────┘      └──────────────┘
              (idle, watching health; promote on missed heartbeats)
```

### Roles
- **Primary (active):** Holds the leader lock in Redis. Accepts all log ingest + query traffic. Renews its lock TTL every ~2s.
- **Standby (passive):** Polls Redis for the leader lock and the primary's `/health` endpoint. If both indicate failure for ≥10s, the first standby to grab the lock promotes itself.

### Failover sequence (target: <10s)
1. Primary stops renewing its Redis lock (crash / kill / network partition).
2. Lock TTL expires (e.g., 5s).
3. Standbys race to `SET NX` the lock; one wins.
4. Winner flips internal role to `primary`, starts accepting traffic.
5. Total target: end-to-end p95 promotion ≤ 10s.

### HTTP surface (planned, per node)
- `GET /health` — liveness + current role (`primary` / `standby`)
- `GET /role` — current role + lock holder
- `POST /logs` — ingest a log line (rejected with 503 if not primary)
- `GET /logs` — query recent logs (primary only)
- `GET /metrics` — prometheus-style counters (heartbeats, promotions, rejected writes)

## HTTP Endpoints

| Method | Path                            | Status code(s)               | Description                                                                       |
|--------|---------------------------------|------------------------------|-----------------------------------------------------------------------------------|
| GET    | `/health`                       | 200 (PRIMARY) / 503 (other)  | Liveness probe — only PRIMARY returns 200.                                        |
| GET    | `/role`                         | 200                          | Returns `{node_id, state, role, lock_holder, known_winner, term}`.                |
| GET    | `/metrics`                      | 200                          | Prometheus exposition text (counters + `node_state` gauge).                       |
| POST   | `/logs`                         | 201 (PRIMARY) / 503 (other)  | Body: `{message, level?, log_id?}`. Idempotent on client-supplied `log_id`.       |
| GET    | `/logs?limit=N`                 | 200 (PRIMARY) / 503 (other)  | Returns `{logs, count, last_log_id}` for the most recent `limit` entries.         |
| POST   | `/admin/trigger-failover`       | 202 (PRIMARY) / 503 (other)  | Releases the lock and self-demotes; standbys promote within ~6s.                  |
| POST   | `/heartbeat`                    | 200 / 400                    | Debug ping — accepts a `HeartbeatMessage` JSON body (real heartbeat goes via Redis). |
| POST   | `/election/candidacy`           | 200 / 400                    | Internal — receives `ElectionMessage` from peers during elections.                |
| POST   | `/election/result`              | 200 / 400                    | Internal — receives `ElectionResult` from peers; updates `known_winner`.          |

## How to Run
> _To be filled in once the implementation lands. Will be a single `docker compose up` that spins up Redis + 3 nodes (1 primary, 2 standby) on ports 8001-8003._

Once Commit 4a lands, `docker compose up --build` will spin up Redis + 3 nodes on ports 8001-8003. (Commit 1 ships only the scaffold; the node service entrypoint is added in Commit 4a.)

## What I Learned
<!-- Filled in as the project evolves -->
- _Why Redis SET NX + TTL is a viable poor-man's leader election (and where it falls short vs. Raft / etcd)._
- _How to size lock TTL vs. heartbeat interval to balance false-failover risk against detection latency._
- _Split-brain windows: what happens during the gap between "primary loses network" and "lock TTL expires."_

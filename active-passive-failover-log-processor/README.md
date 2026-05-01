# active-passive-failover-log-processor

A 3-node Python/FastAPI log-processing cluster (1 primary + 2 standby + Redis) that fails over to a fresh primary in **under 10 seconds** when the active primary dies. Built end-to-end as a learning lab in active-passive coordination, hybrid leader election, and resilience patterns (circuit breaker, bulkhead, chaos testing).

## Tech Stack

- **Language:** Python 3.11+ (asyncio)
- **HTTP:** FastAPI + Uvicorn
- **Inter-node calls:** httpx (async)
- **Coordination / shared state:** Redis 7 (`SET NX EX` lock + heartbeat key + state snapshot)
- **Serialization:** orjson
- **Container:** Docker + Docker Compose (4 services: redis + 3 nodes + dashboard)
- **Tests:** pytest + pytest-asyncio + fakeredis

## Architecture

```
                        ┌─────────────────────────┐
       client traffic → │  primary (node-1:8001)  │  ← /logs, /admin/trigger-failover
                        │  state = PRIMARY        │
                        └────────────┬────────────┘
                                     │ leader:lock + heartbeat:primary + state:snapshot
                                     ▼
                              ┌──────────────┐
                              │    Redis     │
                              └──────┬───────┘
                       ┌─────────────┴─────────────┐
                       ▼                           ▼
            ┌─────────────────────┐      ┌─────────────────────┐
            │ standby (node-2)    │      │ standby (node-3)    │
            │ state = STANDBY     │      │ state = STANDBY     │
            │ polls heartbeat     │      │ polls heartbeat     │
            └─────────────────────┘      └─────────────────────┘

                    ┌─────────────────────────┐
                    │ dashboard (port 8080)   │  ← own container, polls all 3 nodes
                    │ FastAPI + WS + static   │
                    └─────────────────────────┘
```

### State machine

```
       INACTIVE ──IS_PRIMARY=true──► PRIMARY ◄─────────────┐
          │                            │                   │
          └──IS_PRIMARY=false──► STANDBY                    │
                                  │                         │
                  heartbeat_miss  │                         │ won_lock
                                  ▼                         │
                              ELECTION ─────────────────────┘
                                  │
                                  └─lost_lock──► STANDBY
                                  │
                                  └─lock_lost (PRIMARY only)──► STANDBY (self-demote)
```

## Failover semantics

Total failover budget: **<10 seconds** end-to-end.

| Stage                     | Budget    | Source                          |
|---------------------------|-----------|---------------------------------|
| Heartbeat-miss detection  | 6s        | `HEARTBEAT_TIMEOUT` (3 × 2s)    |
| Election jitter           | <100ms    | `priority * 0.0001s`            |
| `SET NX EX` lock race     | <50ms     | one Redis RTT                   |
| Snapshot load             | <100ms    | `GET state:snapshot`            |
| Role flip + warm-up       | ~500ms    | bind PRIMARY routes             |
| **Total**                 | **~7s**   | observed: SIGKILL ~7.5s, SIGTERM ~6.4s, manual ~6.5s |

### Continuity guarantees

- **Counters survive promotion.** The primary writes a counter snapshot (`_next_id`, `_seen_ids`, `last_log_id`, `log_count`) to Redis every `STATE_SYNC_INTERVAL` (default 5s). The new primary loads it before binding `/logs`, so `last_log_id` keeps strictly increasing across a failover.
- **Bounded loss on uncoordinated kill.** Up to **~5 seconds of writes** can be lost on `SIGKILL` / network partition (one snapshot interval). The lost piece is the *idempotency-dedup capability* and *exact pre-failover `log_count`* — the allocator stays monotonic regardless.
- **Idempotent ingest.** Clients may supply `log_id`; retries against a different primary post-failover are deduplicated by id within the snapshot window.
- **Payloads are NOT replicated.** This lab persists *counters*, not log bodies. Cross-node payload replication is a separate problem (Kafka / Raft / cross-DC).

## How to Run

Prerequisites: Docker + Docker Compose.

```bash
# Bring up Redis + 3 nodes + dashboard
make run

# Tail logs from every service
make logs

# Run unit tests in Docker (224 tests, all green at last build)
make test

# Run the full E2E driver (9 steps, ~3 min)
make e2e

# Stop and remove everything
make stop
```

Once `make run` is up, visit **http://localhost:8080** for the live dashboard.

```bash
# Quick smoke test
curl http://localhost:8001/health           # 200 OK   (primary)
curl http://localhost:8002/health           # 503      (standby)
curl -X POST http://localhost:8001/logs \
     -H 'Content-Type: application/json' \
     -d '{"message":"hello","level":"INFO"}'
# → {"status":"accepted","log_id":1}
```

## HTTP Endpoints

Each node exposes these on its host port (8001 / 8002 / 8003).

| Method | Path                            | Status code(s)              | Description                                                                       |
|--------|---------------------------------|-----------------------------|-----------------------------------------------------------------------------------|
| GET    | `/health`                       | 200 (PRIMARY) / 503 (other) | Liveness probe — only PRIMARY returns 200.                                        |
| GET    | `/role`                         | 200                         | `{node_id, state, role, lock_holder, known_winner, term}`.                        |
| GET    | `/metrics`                      | 200                         | Prometheus exposition text (counters + `node_state` gauge).                       |
| POST   | `/logs`                         | 201 (PRIMARY) / 503 (other) | Body `{message, level?, log_id?}`. Idempotent on client-supplied `log_id`.        |
| GET    | `/logs?limit=N`                 | 200 (PRIMARY) / 503 (other) | `{logs, count, last_log_id}` for the most recent `limit` entries.                 |
| POST   | `/admin/trigger-failover`       | 202 (PRIMARY) / 503 (other) | Releases the lock and self-demotes; standbys promote within ~6s.                  |
| POST   | `/heartbeat`                    | 200 / 400                   | Debug ping (real heartbeat goes via Redis).                                       |
| POST   | `/election/candidacy`           | 200 / 400                   | Internal — receives `ElectionMessage` from peers during elections.                |
| POST   | `/election/result`              | 200 / 400                   | Internal — receives `ElectionResult`; updates `known_winner`.                     |

## Configurable Parameters

All configuration is via environment variables (per node). See `.env.example` for an annotated copy.

| Variable                  | Default     | Description                                                                  |
|---------------------------|-------------|------------------------------------------------------------------------------|
| `NODE_ID`                 | (required)  | Unique cluster identifier; seeds election priority via MD5.                  |
| `IS_PRIMARY`              | `false`     | Bootstrap as PRIMARY. Set on exactly one node.                               |
| `PORT`                    | `8001`      | HTTP listen port.                                                            |
| `REDIS_HOST`              | `redis`     | Redis hostname.                                                              |
| `REDIS_PORT`              | `6379`      | Redis port.                                                                  |
| `HEARTBEAT_INTERVAL`      | `2.0`       | Seconds between heartbeat writes + lock renewals (PRIMARY only).             |
| `HEARTBEAT_TIMEOUT`       | `6.0`       | Seconds without heartbeat before STANDBY triggers an election (3 × 2s).      |
| `ELECTION_TIMEOUT`        | `10.0`      | Maximum total time allowed for an election to complete.                      |
| `STATE_SYNC_INTERVAL`     | `5.0`       | Seconds between state snapshots written to Redis (PRIMARY only).             |
| `LOCK_TTL`                | `3`         | TTL on `leader:lock` in Redis. Renewed every `HEARTBEAT_INTERVAL`.           |
| `PEER_NODES`              | `""`        | CSV `host:port` list of peers (excludes self) for inter-node election RPCs.  |
| `DASHBOARD_POLL_INTERVAL` | `1.0`       | Dashboard-only — how often the dashboard server polls every node.            |

## Election protocol (hybrid: priority + Redis lock)

A standby that misses 3 consecutive heartbeats kicks off an election:

1. **Detect.** `STANDBY → ELECTION` after `HEARTBEAT_TIMEOUT` of silence (~6s).
2. **Jitter.** Wait `priority * 0.0001s`, where `priority = md5(node_id)[:4] mod 1000`. Lowest priority races first; collisions break by lex order.
3. **Broadcast candidacy.** Send `ElectionMessage` to every peer via `/election/candidacy` in parallel. Failures are logged but not fatal — Redis is the source of truth.
4. **Race for the lock.** Attempt `SET leader:lock <node_id> NX EX <LOCK_TTL>` in Redis. **Redis is the tie-breaker — there is no quorum vote.**
5. **Winner.** Loads the state snapshot, broadcasts `ElectionResult{winner=self}`, transitions `ELECTION → PRIMARY`, starts heartbeat emission + lock renewal.
6. **Loser.** Reads the lock value, broadcasts `ElectionResult{winner=<lock_holder>}`, transitions `ELECTION → STANDBY`.
7. **Self-demote.** A PRIMARY whose `renew_lock` returns 0 (lock stolen / expired) immediately transitions `PRIMARY → STANDBY`. This catches the network-partition case where Redis kept moving while the primary was severed.

### Why hybrid?

Pure Redis `SET NX` would work, but priority-based jitter gives a deterministic ordering that minimises wasted RPCs during the common case (one standby clearly should win). The Redis lock is the *correctness guarantee*; the broadcast is *informational* — every node updates its `known_winner` view but doesn't gate any decision on it.

## Resilience patterns

### Circuit breaker (per peer, DIY)

Each `(host, port)` peer gets its own `CircuitBreaker` (no `pybreaker` dependency).

| State        | Behaviour                                                                          |
|--------------|------------------------------------------------------------------------------------|
| **CLOSED**   | Calls pass through. 5 consecutive failures (`fail_max=5`) → OPEN.                  |
| **OPEN**     | All calls rejected immediately with `CircuitBreakerOpen`. Stays open for 30s.      |
| **HALF_OPEN**| Exactly one trial call. Success → CLOSED, failure → OPEN (cooldown clock resets).  |

Counters surface at `/metrics`:

- `circuit_breaker_failures_total` — sum across every peer breaker.
- `circuit_breaker_opens_total` — sum of times any breaker has opened.

### Bulkhead (per call type)

Two independent `asyncio.Semaphore` budgets in `InterNodeClient`:

- `send_candidacy` — **3 concurrent** calls.
- `send_election_result` — **3 concurrent** calls.

Three slots is comfortable headroom for a 3-node cluster (each election fans out to 2 peers) while preventing runaway parallelism if something upstream retries aggressively. The two budgets are separate so a candidacy storm cannot starve the result broadcast.

Heartbeat traffic flows through Redis (not httpx) so it has no breaker / semaphore — the resilience pattern there is "Redis is up or the cluster is down".

### Dashboard as its own container

The dashboard runs in a fourth container on port 8080. It does **not** live on any cluster node — that would defeat the purpose, since the moment a primary dies you'd lose visibility into the failover you're trying to observe. By polling all three nodes from outside, the dashboard tolerates any single-node failure cleanly.

The "Trigger Failover" button proxies through the dashboard server, which reads `leader:lock` from Redis and forwards to whichever node currently holds it. The browser never has to discover the live primary.

## Chaos testing

`scripts/chaos.py` is a standalone driver run against a *running* cluster. It does NOT bring the cluster up or tear it down — `make run` first, then disturb.

```bash
# 60 s of every-30s SIGKILL on a random node; assert exactly one PRIMARY at all samples
python3 scripts/chaos.py --scenario random_kill --duration 60

# Disconnect the primary from the network, expect a standby to promote, reconnect,
# expect the original primary to rejoin as STANDBY
python3 scripts/chaos.py --scenario partition --duration 30

# 50 logs/sec for 120s while random_kill runs in parallel; assert duplicate-id ratio < 5%
python3 scripts/chaos.py --scenario sustained_load --duration 120 --rate 50
```

The `tests/test_chaos.py` pytest module wraps the partition scenario as an integration test. It is **skipped by default**; enable with:

```bash
RUN_CHAOS_TESTS=1 pytest tests/test_chaos.py
```

The full E2E driver `scripts/verify_failover.py` (run by `make e2e`) also exercises a partition + heal cycle as Step 9.

## What I Learned

- **Redis `SET NX EX` is a viable poor-man's leader lock.** Kleppmann's *How to do distributed locking* critique (Redlock isn't safe under arbitrary clock skew) is real but largely orthogonal for *availability* locks like this one — antirez's response (it's fine for "efficiency" use cases where occasional double-acquisition is recoverable) is the right framing for an active-passive failover where the new primary always idempotently picks up where the old one left off.
- **Lock TTL must be shorter than the heartbeat-miss timeout.** With `LOCK_TTL=3` and `HEARTBEAT_TIMEOUT=6`, the lock has expired by the time standbys finish their failure-detection countdown — the winning standby's first `SET NX EX` succeeds rather than contending with a stale lease. Setting them equal would create a race.
- **TTL/3 renewal cadence (Heartbeats).** Renewing every 1/3 of TTL tolerates two missed renewals before the lock expires. Heartbeats every 2s with TTL=6s is actually closer to TTL/2 here — but that's a deliberate trade for the 6s detection window required by the spec.
- **Hybrid election > pure jitter.** Priority gives ordering without a coordination round; Redis `SET NX` gives mutual exclusion. The broadcast is informational. This avoids the "everyone races, one wins arbitrarily" pattern of pure SET-NX-only and the "extra round-trip per candidate" cost of a vote-based protocol.
- **Idempotent log writes are non-negotiable for failover continuity.** Without dedup on `log_id`, a client retry post-failover would create a phantom duplicate. With it, the new primary can pick up a snapshot whose `_next_id` is past every id the client has ever seen.
- **Single-Redis is a documented SPOF.** Sentinel / Redlock / Cluster would close it but add operational weight that doesn't pay back for a 3-node lab. Documented in *Known limitations*.
- **The dashboard hosting model matters.** Hosting it on the primary defeats the point. Hosting it on a standby creates a chicken-and-egg ("which standby?"). Its own container, polling all three, is the only configuration that survives the failure mode the system is built to recover from.
- **Snapshot-only state replication has a clear failure-loss bound** (`STATE_SYNC_INTERVAL`). It's the right level of effort for "the cluster's view of itself stays continuous" — but explicitly NOT the right tool for "no log entry is ever lost".

## Known limitations

- **Single-Redis SPOF.** No Sentinel / Cluster / Redlock. If Redis dies, the cluster loses both the lock and the state snapshot. Acceptable for a learning lab; would replace with Sentinel quorum in production.
- **No fencing tokens.** A "stuck" old primary could in principle write past the moment its lock was stolen if Redis lost the lock for a beat. We rely on lock-renewal failure → self-demote to close this; a real system would attach a monotonically-increasing token to every write and reject stale tokens at the storage layer.
- **Snapshot-only state replay (~5s window).** Up to 5s of un-snapshotted writes can be lost on uncoordinated primary kill. Tunable via `STATE_SYNC_INTERVAL`; lower it for fresher snapshots at the cost of Redis write pressure.
- **No AuthN/Z.** `/admin/trigger-failover` is wide open. Fine on `localhost`-bound docker-compose; not fine in production.
- **Log payloads are not replicated** between nodes — only counters. A real system would put Kafka or a Raft log between the client and the cluster.
- **Manual failover is no fresher than the last scheduled snapshot.** The release-lock path inside `stop()` does not force an extra snapshot. If you want zero-loss failover you'd add `snapshot_now()` immediately before releasing the lock.

## Project layout

```
src/
  config.py             # NodeConfig (env vars)
  models.py             # NodeState enum + dataclasses + orjson helpers
  redis_client.py       # async wrapper: lock acquire/renew/release (Lua), heartbeat, snapshot
  state_machine.py      # 5-state transition table with validation + callbacks
  heartbeat.py          # HeartbeatEmitter (PRIMARY) + HeartbeatMonitor (STANDBY)
  election.py           # ElectionCoordinator: jitter, broadcast, lock race
  log_processor.py      # in-memory log store + idempotent ingest
  state_persistence.py  # snapshot every 5s; load_into() on PRIMARY transition
  http_server.py        # FastAPI app factory (/health, /logs, /role, /metrics, ...)
  circuit_breaker.py    # DIY 3-state breaker
  inter_node_client.py  # PeerClient + breaker + bulkhead semaphores
  peer_client.py        # PeerClient Protocol + plain httpx impl
  dashboard.py          # FastAPI + WebSocket dashboard (separate container)
  node.py               # FailoverNode — wires everything
  __main__.py           # entry point: load config, run node

scripts/
  verify_failover.py    # 9-step E2E driver invoked by `make e2e`
  chaos.py              # standalone chaos driver (random_kill / partition / sustained_load)

tests/                  # 224 unit tests + 1 opt-in chaos integration test
```

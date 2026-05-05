# Multi-Region Log Replication Engine

A simulated distributed log store across three regions (`us-east`, `europe`,
`asia`) with **vector clocks** for causal ordering, **deterministic
conflict resolution** under concurrent writes, and **automatic primary
failover** with a <5s recovery budget. Built as a learning lab for
backend internals — eventual consistency, replication-lag telemetry,
and one-way failover semantics.

## Tech Stack

- **Python 3.11**
- **FastAPI** + **Uvicorn** (HTTP + WebSocket)
- **Pydantic v2** (data models)
- **pytest** + **pytest-asyncio** + **httpx** (testing)
- **Vue 3** + **Tailwind** + **Chart.js** (CDN, raw HTML — no bundler)
- **Docker** + **Docker Compose**

## Architecture

```
                     ┌──────────────────────────────────────────────┐
   client traffic →  │        FastAPI app (single process)          │
                     │                                              │
                     │  POST /api/logs ──► ReplicationController    │
                     │                       │                      │
                     │                       ▼                      │
                     │            ┌────────────────────┐            │
                     │            │ Region(us-east)    │ PRIMARY    │
                     │            │  log_store         │            │
                     │            │  vector_clock      │            │
                     │            │  logical_ts        │            │
                     │            └─────────┬──────────┘            │
                     │      replicate (in-process asyncio.gather)   │
                     │             │                  │             │
                     │             ▼                  ▼             │
                     │   ┌──────────────────┐  ┌──────────────────┐ │
                     │   │ Region(europe)   │  │ Region(asia)     │ │
                     │   │  log_store       │  │  log_store       │ │
                     │   │  vector_clock    │  │  vector_clock    │ │
                     │   │  logical_ts      │  │  logical_ts      │ │
                     │   └──────────────────┘  └──────────────────┘ │
                     │                                              │
                     │  HealthMonitor ── per-region lag p50/p95/p99 │
                     │                ── replication success rate   │
                     │                ── primary failure detection  │
                     │                ── triggers failover (<5s)    │
                     │                                              │
                     │  WS /ws ── pushes snapshot every 5s          │
                     │  GET /  ── serves Vue dashboard (raw HTML)   │
                     └──────────────────────────────────────────────┘
```

Three `Region` objects live in-process inside a single FastAPI app. The
`ReplicationController` elects a primary deterministically from
`PRIMARY_PREFERENCE`, routes every write through it (`local_write`),
and fans out to secondaries with `asyncio.gather(...,
return_exceptions=True)`. The `HealthMonitor` runs as an asyncio
background task, snapshots cluster state every
`HEALTH_CHECK_INTERVAL_SEC`, and triggers re-election when the elected
primary reports `is_healthy=False` for **two consecutive ticks**.

## Vector Clock Semantics

A `VectorClock` is simply `dict[region_id, int]` — one logical-time
counter per region.

```python
def vector_clock_compare(a, b):
    """
    -1   : a < b   (a happens-before b)
     1   : a > b   (b happens-before a)
     0   : a == b  (identical)
     None: concurrent / incomparable
    """
    keys = set(a) | set(b)
    a_le_b = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    b_le_a = all(b.get(k, 0) <= a.get(k, 0) for k in keys)
    if a_le_b and b_le_a: return 0
    if a_le_b:            return -1
    if b_le_a:            return 1
    return None
```

`merge(local, incoming)` element-wise maxes the two clocks; the local
region's slot is then incremented by 1 on receive (per the project
spec).

## Conflict Resolution Rules

When a secondary receives an entry that already exists in its
`log_store`, conflict resolution runs deterministically:

| `vector_clock_compare(existing, incoming)` | Action |
|---|---|
| `-1` (existing happens-before incoming) | Keep **incoming** |
| `1` (incoming happens-before existing) | Keep **existing** |
| `0` (identical) | Keep **incoming** (idempotent) |
| `None` (concurrent / incomparable) | **LWW** on `(logical_ts, created_at, region, log_id)` — larger lex tuple wins |

The tiebreaker tuple uses fields every region sees identically, so
every region converges on the same resolution without coordination —
this is the simple-and-deterministic flavour of LWW (not full CRDT).

## HTTP Endpoints

| Method | Path | Description |
|---|---|---|
| GET  | `/`                              | Vue 3 + Tailwind dashboard (raw HTML) |
| GET  | `/api/health`                    | `HealthSnapshot` JSON |
| GET  | `/api/status`                    | Alias for `/api/health` (dashboard polling fallback) |
| POST | `/api/logs`                      | Write a log. Body: `{"message", "level", "service"}` |
| GET  | `/api/logs?limit=N`              | Read recent entries from the **primary** |
| POST | `/api/carts/{cart_id}`           | Cart write (consistent-hashed to home region; routed via primary) |
| POST | `/api/regions/{id}/kill`         | Mark region offline (gated by `ALLOW_KILL_ENDPOINT`) |
| POST | `/api/regions/{id}/heal`         | Mark region online (does NOT auto-promote) |
| GET  | `/api/regions/{id}/logs?limit=N` | Read logs from a specific region's local store |
| WS   | `/ws`                            | `HealthSnapshot` pushed every `WEBSOCKET_PUSH_INTERVAL_SEC` |

## WebSocket Message Shape

Every push (and the initial post-connect message) is a `HealthSnapshot`
serialised to JSON:

```json
{
  "overall_status": "healthy",
  "current_primary": "us-east",
  "taken_at": 1714836200.123,
  "total_writes": 142,
  "regions": [
    {
      "region_id": "us-east",
      "is_primary": true,
      "is_healthy": true,
      "log_count": 142,
      "vector_clock": {"us-east": 142, "europe": 90, "asia": 90},
      "logical_ts": 142,
      "replication_lag_ms": 0.0,
      "replication_success_rate": null
    },
    {
      "region_id": "europe",
      "is_primary": false,
      "is_healthy": true,
      "log_count": 142,
      "vector_clock": {"us-east": 142, "europe": 142, "asia": 0},
      "logical_ts": 142,
      "replication_lag_ms": 12.4,
      "replication_success_rate": 1.0
    }
  ],
  "recent_failovers": [
    {"at": 1714836180.0, "old_primary": "us-east", "new_primary": "europe", "elapsed_ms": 0.42}
  ]
}
```

`total_writes` is sourced from the **current primary's** `log_count` —
not summed across regions, which would triple-count under a 3-region
cluster.

## Failover Semantics

- **Detection**: 2 consecutive unhealthy ticks. Default tick =
  `HEALTH_CHECK_INTERVAL_SEC` = 1s, so detection lands within ~2s.
- **Recovery target**: <5s end-to-end. Observed in `make e2e`: ~1-2s
  (detection dominates; election is microseconds).
- **One-way**: healing the original primary via
  `POST /api/regions/{id}/heal` does **not** auto-promote. The new
  primary keeps the role until it itself becomes unhealthy.
- **Manual reset**: to re-elect from the head of `PRIMARY_PREFERENCE`,
  kill the *current* primary (the next deterministic candidate
  becomes primary).
- **History**: the most recent ten failover events are kept in a
  bounded `collections.deque` and surfaced on every `HealthSnapshot`
  via `recent_failovers`.

## Run

```bash
make run     # docker compose up --build -d
make logs    # follow app logs
make stop    # docker compose down

make test    # full unit-test suite, in Docker
make e2e     # multi-region E2E: write 50, kill primary, failover, heal, assert
make demo    # cart-update demo: consistent hashing + concurrent conflict resolution
```

The chaos scenario can be run against the live stack:

```bash
make run
python3 scripts/chaos.py
make stop
```

## What I Learned

- **Vector clocks vs Lamport clocks** — Lamport gives total order at
  the cost of losing concurrency information. Vector clocks preserve
  "concurrent" as a distinct relation, which is exactly what conflict
  resolution needs to know.
- **Deterministic LWW vs CRDT** — Last-write-wins on a stable
  tiebreaker tuple is simple and converges, but it can drop one of two
  concurrent writes. CRDTs (e.g. an OR-Set) converge without dropping,
  but require richer per-field types. For a log of opaque
  `{message, level, service}` records, deterministic LWW is the right
  trade-off.
- **Why one-way failover** — a healed primary that auto-promotes can
  flap. Network instability + auto-promote = thrash. Operator-driven
  re-promotion is the correct posture for the simulated environment.
- **CDN Vue + raw HTML** — keeps the dashboard self-contained at the
  cost of a slower first paint. For a learning project the trade-off
  is right; bundling Vue would balloon the repo's setup surface for
  no real production benefit here.
- **`asyncio.gather(..., return_exceptions=True)`** — the trick that
  lets one secondary's failure surface as a stats-tracker datum
  without failing the whole write. Without it, a single misbehaving
  secondary takes the primary's write down with it.

## Cleanup

```bash
./cleanup.sh   # docker compose down -v + image prune (does NOT touch .venv)
```

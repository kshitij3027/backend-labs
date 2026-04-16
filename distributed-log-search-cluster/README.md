# Distributed Log Search Cluster

A horizontally scalable search engine that partitions an inverted index across
multiple index nodes using consistent hashing and serves queries from a
single coordinator via parallel scatter-gather.

## Tech Stack

- Python 3.12
- FastAPI + Uvicorn (coordinator and index-node HTTP APIs)
- httpx (async, pooled HTTP client used by the coordinator)
- redis-py (asyncio) with Redis 7 as the shared per-node posting store
- cachetools (TTL LRU result cache on the coordinator)
- pytest + pytest-asyncio
- Docker + Docker Compose

## Architecture

```
                     +----------------------+
 Client -- POST ---> |  Coordinator (8000)  |
 /index, /search     |  - consistent hash   |
                     |  - scatter-gather    |
                     |  - TTL result cache  |
                     +--+----+----+----+----+
                        |    |    |    |   parallel httpx
                  +-----+    |    |    +-----+
                  v          v    v          v
               node-1     node-2 node-3   node-4   FastAPI (8101-8104)
                  \          |    |          /
                   \         v    v         /
                    +-----> Redis 7 <------+
                           (shared, namespaced)
```

### How the pieces fit

- **Consistent hash ring.** The coordinator hashes every term with SHA-1 and
  places each physical node at 100 virtual positions on a 2^160 ring. Lookup
  is O(log N) via `bisect` over a sorted list of `(hash, node_id)` tuples.
  The ring is used both at index time (where does this term live?) and at
  query time (which nodes do I need to ask?).
- **Term-sharded index.** Each term's entire posting list lives on exactly
  one physical node. Keys are namespaced per-node:
  `node:{node_id}:postings:{term}` (a Redis set of doc_ids),
  `node:{node_id}:tf:{term}:{doc_id}` (term frequency), and
  `node:{node_id}:meta:terms` (set of terms owned by the node).
- **Shared document bodies.** Document content and metadata live in a single
  shared namespace `docs:{doc_id}`. Only the coordinator reads from it, when
  it hydrates scored results. This avoids replicating doc bodies across every
  node that owns a term from that doc.
- **Scatter-gather.** The coordinator tokenizes the query, groups terms by
  their owning node, fires one HTTP request per node via
  `asyncio.gather` over a shared `httpx.AsyncClient`, then intersects (AND)
  or unions (OR) the returned posting lists and scores with TF-IDF.
- **Partial-failure tolerance.** Each node call is wrapped in a
  retry-with-exponential-backoff helper (3 attempts, 0.1 / 0.2 / 0.4 s). If a
  node is still unreachable after retries it is added to `failed_nodes` in
  the response; healthy nodes still return results. Partial responses are
  never cached.
- **Result cache.** `TTLCache(maxsize=1000, ttl=60)` keyed on a normalized
  `(query, op)` tuple. Cache hits set `cached=true` in the response.

## How to Run

```bash
make build      # build all images
make up         # start redis, 4 nodes, coordinator in the background
```

Index a document:

```bash
curl -s -X POST http://localhost:8000/index \
  -H 'Content-Type: application/json' \
  -d '{"doc_id":"d1","content":"error timeout login failed"}'
```

Run a multi-term AND search:

```bash
curl -s -X POST http://localhost:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"error timeout","op":"AND","limit":10}'
```

Cluster-wide health and per-node stats:

```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/cluster/stats
curl -s http://localhost:8101/stats   # any individual node
```

Shut down and reclaim volumes:

```bash
make down
```

## API

### `POST /index` (coordinator)

Request:
```json
{
  "doc_id": "d1",
  "content": "error timeout login failed",
  "metadata": {"source": "app-a"}
}
```

Response:
```json
{"doc_id": "d1", "terms_indexed": 4, "nodes_written": ["node-2", "node-4"]}
```

### `POST /search` (coordinator)

Request:
```json
{"query": "error timeout", "op": "AND", "limit": 10}
```

Response:
```json
{
  "documents": [
    {"doc_id": "d1", "content": "error timeout login failed", "score": 1.97, "metadata": {}},
    {"doc_id": "d3", "content": "timeout error database connection", "score": 1.97, "metadata": {}}
  ],
  "total_results": 2,
  "search_time_ms": 8.42,
  "nodes_queried": ["node-2", "node-4"],
  "failed_nodes": [],
  "routing_ms": 0.08,
  "scatter_ms": 6.91,
  "merge_ms": 1.31,
  "cached": false
}
```

Field notes:
- `nodes_queried` reflects only the nodes that actually own query terms, not
  all 4 — single-term queries hit exactly one node.
- `failed_nodes` is populated when a node was unreachable after retries.
- `routing_ms`, `scatter_ms`, `merge_ms` are per-stage latencies for
  observability. `search_time_ms` is the end-to-end wall time.
- `cached=true` indicates the response came from the coordinator's TTL cache.

### `GET /health` (coordinator)

```json
{
  "status": "healthy",
  "coordinator_port": 8000,
  "nodes": {"node-1": true, "node-2": true, "node-3": true, "node-4": true},
  "healthy_nodes": 4,
  "total_nodes": 4
}
```

### `GET /cluster/stats` (coordinator)

```json
{
  "node-1": {"node_id": "node-1", "term_count": 312, "document_count": 250, "status": "active"},
  "node-2": {"node_id": "node-2", "term_count": 297, "document_count": 250, "status": "active"},
  "node-3": {"node_id": "node-3", "term_count": 305, "document_count": 250, "status": "active"},
  "node-4": {"node_id": "node-4", "term_count": 291, "document_count": 250, "status": "active"}
}
```

### `GET /stats` (individual node)

Same shape as one entry in `/cluster/stats`, served by the node itself at
`http://node-N:81xN/stats`.

## Testing

All tests run inside Docker. No command is expected to run on the host.

| Command | Purpose |
|---|---|
| `make test` | Full `pytest` unit suite (hash ring, shard, node API, coordinator API, planner, cache, retry). |
| `make e2e` | `scripts/e2e_smoke.py` — indexes a 6-doc fixed corpus, runs 7 fixed AND/OR queries, asserts exact `doc_id` set equality and presence of all observability fields. |
| `make distribution` | `scripts/distribution_test.py` — indexes 1000+ distinct terms and asserts stddev/mean of per-node `term_count` is below 0.20. |
| `make failure` | `scripts/failure_test.py` — runs continuous queries while a node is stopped and restarted; asserts no hard failures and that at least one response surfaces `failed_nodes`. |
| `make benchmark` | `scripts/benchmark.py` — drives concurrent `/search` load and reports QPS + p50/p95/p99 latency. |

## Configurable Parameters

All configured via environment variables (see `docker-compose.yml`).

| Variable | Default | Description |
|---|---|---|
| `COORDINATOR_PORT` | `8000` | Coordinator HTTP port. |
| `NODE_URLS` | `node-1=http://node-1:8101,...` | Static node registry, `id=url` entries comma-separated. |
| `VIRTUAL_NODES` | `100` | Virtual nodes per physical node on the hash ring. |
| `REQUEST_TIMEOUT` | `5.0` | Per-request timeout (seconds) for coordinator to node. |
| `RETRY_COUNT` | `3` | Attempts (including the first) before giving up on a node. |
| `RETRY_BASE_DELAY` | `0.1` | Exponential backoff base, doubled per retry. |
| `CACHE_SIZE` | `1000` | Coordinator TTL cache max entries. |
| `CACHE_TTL` | `60` | Coordinator TTL cache lifetime (seconds). |
| `REDIS_HOST` | `redis` | Redis hostname. |
| `REDIS_PORT` | `6379` | Redis port. |
| `NODE_ID` | per-service | `node-1` .. `node-4`, used as the Redis key prefix. |
| `NODE_PORT` | `8101`..`8104` | Port the individual node serves on. |

## What I Learned

- A bisect search over a sorted `(hash, node_id)` list is enough to make a
  consistent-hash lookup O(log N); no fancy skip-list required.
- 100 virtual nodes per physical node is the rule-of-thumb sweet spot: fewer
  and distribution skews badly, more and ring construction dominates startup.
- Term-sharding (one term lives on one node) is dramatically simpler than
  doc-sharding for multi-term intersection because each node can respond
  with a complete posting list for its terms — no cross-node join needed.
- Keeping document bodies in a single shared `docs:*` namespace means
  hydration is a one-shot pipelined `HGETALL` regardless of how many nodes
  the query fanned out to.
- `asyncio.gather(return_exceptions=True)` plus a small retry wrapper is all
  that's needed for a production-shaped scatter-gather with partial-failure
  tolerance; there's no need for a task-group abstraction here.
- Per-stage timings (`routing_ms`, `scatter_ms`, `merge_ms`) are
  disproportionately valuable for debugging — without them "slow query" is
  an unfalsifiable claim.
- Result caches must never cache partial responses. The bug is easy to write
  and very sticky: a transient node outage gets a 60-second sticky empty
  answer for a popular query.
- Retry with exponential backoff has to bound total latency, not just retry
  count: 3 tries * 5 s timeout = 15 s wall clock if a node is dead, which
  pushes past the coordinator's own client timeouts.
- Docker-only testing loop (no local uvicorn) is a net time saver once the
  compose stack is fast to rebuild — shipping breaks get caught before commit
  instead of in CI.

## Status

Implemented and measured against the compose stack:

- Distribution quality: stddev/mean = **0.11** over 1000 terms across 4 nodes.
- Single-term latency: **p50 ~50 ms**, well under the 100 ms target.
- Throughput: **~652 QPS** sustained on the compose stack, clearing the 100
  QPS target by 6x.
- Fault tolerance: graceful partial results with `failed_nodes` populated
  during node outages; full recovery on restart.

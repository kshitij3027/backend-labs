# Distributed SQL-Like Log Query Engine

A SQL-like query language and distributed execution engine for log data — hand-written tokenizer and recursive-descent parser, a cost-annotated planner that does partition pruning, predicate pushdown and two-phase aggregation distribution, and a scatter-gather coordinator that fans queries out across three partition nodes with per-partition timeouts, retries, and partial-failure tolerance.

## Tech Stack

- **Language / runtime**: Python 3.12
- **Web framework**: FastAPI 0.115 on Uvicorn (ASGI)
- **HTTP client**: `httpx` async client with shared connection pool
- **Data validation**: Pydantic v2
- **Templating / UI**: Jinja2 + vanilla JavaScript (no HTMX, no React)
- **CLI**: Click 8 (for `scripts/demo.py` and `scripts/load_test.py`)
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker + Docker Compose

## Architecture

```
                        +--------------------------------+
 Client -- POST SQL --> |   Coordinator (:8000)          |
 Web UI / WebSocket     |   parse -> plan -> execute     |
                        |   partial-failure tolerance    |
                        |   /api/query  /api/explain     |
                        |   /api/partitions  /api/health |
                        |   /  (UI)  /ws/query/{id}      |
                        +--+----------+----------+-------+
                parallel httpx scatter-gather
                           |          |          |
                           v          v          v
                    partition-1  partition-2  partition-3
                     (:8101)      (:8102)      (:8103)
                     2026-04-01   2026-04-08   2026-04-15
                     ..04-07      ..04-14      ..04-21
                     + indexes    + indexes    + indexes
```

- **Coordinator** parses SQL into a typed AST, builds a cost-annotated execution plan, prunes partitions whose metadata can't satisfy the `WHERE`, pushes the remaining predicate down to each surviving partition, scatters parallel `POST /execute` calls, and merges partial results into a globally-correct response.
- **Partition nodes** are independent FastAPI services. Each owns a deterministic synthetic time-range shard held in memory with indexes on `level`, `service`, and `timestamp`. Each applies the pushed-down filter locally and returns either raw rows or a partial aggregate bucket.
- **Partial failure** — per-partition 5 s timeout, exponential-backoff retries. A dead partition surfaces in the `failed_partitions` list on the response envelope; surviving partitions' results still come back with `partial_results: true`.

## Quick Start

```bash
# Bring everything up (coordinator + 3 partitions).
docker compose up -d
# Or equivalently, using the helper script (also waits for healthchecks):
./start.sh

# Open the web UI.
open http://localhost:8000/

# Shut everything down.
./stop.sh
# Or: docker compose down -v
```

Once up, the coordinator listens on `http://localhost:8000` and the partitions on `http://localhost:8101`, `:8102`, `:8103`. API docs are auto-generated at `http://localhost:8000/docs`.

## API Examples

```bash
# Health — returns coordinator + per-partition status
curl -s http://localhost:8000/api/health

# Partition inventory — id, url, time range, indexed fields, healthy flag
curl -s http://localhost:8000/api/partitions

# Run a query
curl -s -X POST http://localhost:8000/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, COUNT(*) AS cnt FROM logs GROUP BY service ORDER BY cnt DESC LIMIT 5"}'

# EXPLAIN — build the plan without executing
curl -s -X POST http://localhost:8000/api/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM logs WHERE level = '\''ERROR'\'' LIMIT 10"}'

# Stream a query over WebSocket (first, POST to get a query_id)
curl -s -X POST http://localhost:8000/api/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM logs LIMIT 5"}'
# Then open:  ws://localhost:8000/ws/query/<query_id>
```

## Sample Queries

```sql
-- Temporal (partition pruning by time range)
SELECT * FROM logs
WHERE level = 'ERROR'
  AND timestamp BETWEEN '2026-04-08' AND '2026-04-14'
LIMIT 10;

-- Text search (CONTAINS across indexed fields)
SELECT * FROM logs
WHERE message CONTAINS 'timeout'
LIMIT 10;

-- Analytical (two-phase GROUP BY aggregation)
SELECT service, COUNT(*) AS cnt
FROM logs
GROUP BY service
ORDER BY cnt DESC
LIMIT 5;
```

## Configuration

All configuration is environment-driven — see `.env.example` for the full set. The values below are already baked into `docker-compose.yml`; override them via an `.env` file or shell env if you want different defaults.

| Variable | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Root log level for coordinator and partition services. |
| `COORDINATOR_PORT` | `8000` | Port the coordinator FastAPI app binds to. |
| `PARTITION_URLS` | `partition-1=http://partition-1:8101,...` | Comma-separated `id=url` list of partition endpoints for the coordinator to scatter to. |
| `REQUEST_TIMEOUT` | `5.0` | Per-partition HTTP timeout (seconds) for the scatter-gather client. |
| `DEFAULT_LIMIT` | `1000` | Row cap applied when a query omits an explicit `LIMIT`. |
| `MAX_CONCURRENT_QUERIES` | `100` | Soft ceiling on simultaneous in-flight queries at the coordinator. |
| `QUERY_TIMEOUT` | `30.0` | Total wall-clock timeout (seconds) for a single end-to-end query. |
| `PARTITION_ID` | `partition-1` | Identifier this partition reports in its `/metadata` response. |
| `PARTITION_PORT` | `8101` | Port the partition FastAPI app binds to. |
| `PARTITION_TIME_START` | `2026-04-01T00:00:00` | Inclusive lower bound of this partition's time-range shard. |
| `PARTITION_TIME_END` | `2026-04-07T23:59:59` | Inclusive upper bound of this partition's time-range shard. |
| `INDEXED_FIELDS` | `level,service,timestamp` | Fields this partition maintains indexes on for faster filtering. |
| `LOG_SAMPLE_COUNT` | `5000` | Number of deterministic synthetic log rows generated at startup. |

## Running Tests

Unit + integration suite inside Docker:

```bash
docker compose run --rm test pytest -v
```

Or via the Makefile:

```bash
make test
```

## Running the Demo

Runs three representative queries (temporal, text-search, analytical) against a live coordinator and prints the formatted output block from §8 of the project requirements.

```bash
docker compose up -d
docker compose run --rm test python scripts/demo.py
# Or: make demo
```

## Running the Load Test

Drives `--concurrent-users` asyncio workers against `POST /api/query` for `--duration` seconds and reports QPS, p50/p95/p99 latency, error rate, and partial-failure rate. Exits 0 when sustained QPS >= 100 (the spec target), otherwise 1.

```bash
docker compose up -d
docker compose run --rm test python scripts/load_test.py \
    --concurrent-users 20 --duration 30
# Or: make load
```

Sample output:

```
==============================================================
  Distributed SQL-Like Log Query Engine — Load Test Report
==============================================================
  Target URL          : http://coordinator:8000
  Concurrent users    : 20
  Query mix           : all
  Warmup (sec)        : 3.0
  Measured duration   : 30.02 sec
  Total duration      : 33.00 sec
--------------------------------------------------------------
  Total requests      : 4821
  Successful          : 4821
  Failed              : 0
  Error rate          : 0.00%
  Partial-failure rate: 0.00% (of successful responses)
--------------------------------------------------------------
  Throughput (QPS)    : 160.59 requests/sec
  Latency p50         : 112.34 ms
  Latency p95         : 248.77 ms
  Latency p99         : 312.05 ms
==============================================================
PASS: QPS target met: 160.59 requests/sec
```

## Makefile Targets

| Target | Purpose |
|---|---|
| `make build` | `docker compose build` all images. |
| `make up` | Start the 3 partitions + coordinator detached. |
| `make down` | Tear down the stack and remove volumes. |
| `make test` | Run the full pytest suite inside the `test` service. |
| `make e2e` | Run the demo end-to-end: `start.sh` -> `demo.py` -> `stop.sh`. |
| `make demo` | Run `scripts/demo.py` against a running coordinator. |
| `make load` | Run `scripts/load_test.py` at 20 users x 30s. |
| `make ui` | Open `http://localhost:8000/` in the browser. |
| `make logs` | Tail logs for every service. |
| `make clean` | Tear down + remove locally built images. |

## What I Learned

- **Hand-writing a SQL parser forces precise grammar design.** A tokenizer plus recursive-descent parser with precedence climbing for `WHERE` expressions is a compact way to get `AND` / `OR` / `NOT` / `IN` / `BETWEEN` / `CONTAINS` right without pulling in `sqlglot`.
- **Partition pruning before scatter is the single biggest latency lever.** Pruning is cheap metadata math at the coordinator — every partition skipped is an entire network round-trip saved and an entire shard un-scanned.
- **Two-phase aggregation (partial -> global) composes trivially.** Each partition returns `{count, sum, min, max, groups}`; the coordinator's only job is to merge buckets. `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `GROUP BY` all fall out of the same merge primitive.
- **Partial-failure handling has to be baked in from the start.** `asyncio.gather(..., return_exceptions=True)` plus a retry decorator lets a dead partition surface as a `failed_partitions` entry on the response envelope instead of crashing the whole query — worth the minor envelope complexity.
- **WebSocket streaming drops in cleanly when the executor has a callback hook.** Passing an optional `progress_callback` into `QueryExecutor.run` kept the UI progress feature additive, not invasive — the executor stays identical for non-streaming callers.

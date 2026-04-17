# SQL-Like Log Query Engine

A SQL-like query language and execution engine that parses queries, optimizes them with partition pruning, predicate pushdown, and aggregation distribution, then executes them in coordinated fashion across multiple distributed log storage nodes.

## Tech Stack

- **Language**: Python 3.12
- **Framework**: FastAPI + Uvicorn (REST API on port 8000)
- **SQL Parser**: sqlglot
- **HTTP Client**: httpx (async node coordination)
- **Validation**: Pydantic v2
- **Web UI**: Jinja2 templates
- **CLI**: Click
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker + Docker Compose (multi-node simulation)

## What It Does

This project implements a distributed query engine for log data with four layered responsibilities:

1. **Parse** — Accept SQL-like queries (`SELECT ... FROM logs WHERE ... GROUP BY ... ORDER BY ...`) and turn them into a typed AST.
2. **Plan & Optimize** — Apply classic query-optimization techniques adapted for partitioned log storage:
   - **Partition pruning** — skip nodes whose partition metadata cannot match the `WHERE` clause (time range, hash key, etc.).
   - **Predicate pushdown** — send filters down to each node so only matching rows cross the wire.
   - **Aggregation distribution** — split aggregates into partial (per-node) and final (coordinator) stages so `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY` scale across nodes.
3. **Execute** — Coordinate a scatter/gather across the surviving nodes, stream partial results back, and merge them into a single globally-correct response.
4. **Serve** — Expose the engine over a REST API (FastAPI on port 8000) and a lightweight web interface where users can submit queries and inspect the generated plan.

## How It Runs

- **Single-process mode**: one FastAPI server on port 8000 with simulated in-memory partition nodes — good for development and unit tests.
- **Docker Compose mode**: one coordinator container plus N partition-node containers, each serving its own shard of log data over HTTP — used for end-to-end testing of the real scatter/gather path.

## How to Run

_To be filled in as development progresses._

## Query Language (planned)

```sql
SELECT level, COUNT(*) AS n
FROM logs
WHERE ts BETWEEN '2026-04-01' AND '2026-04-17'
  AND service = 'api'
GROUP BY level
ORDER BY n DESC
LIMIT 10;
```

Supported (target) surface:
- Projection: `SELECT col, ...` and `SELECT *`
- Filtering: `WHERE` with `AND` / `OR`, comparisons, `BETWEEN`, `IN`, `LIKE`
- Aggregation: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY`
- Ordering & paging: `ORDER BY`, `LIMIT`, `OFFSET`

## Architecture (planned)

```
            +--------------------------+
Client  --> |  Coordinator  (:8000)    |
  UI    --> |  parse -> plan -> exec   |
            +------------+-------------+
                         |
         +---------------+---------------+
         v               v               v
    Node-1 (:9001)  Node-2 (:9002)  Node-N (:900N)
    partition A     partition B     partition C
```

- **Coordinator**: parses SQL, builds an optimized plan, prunes partitions, pushes predicates, scatters sub-queries, merges results.
- **Partition Nodes**: each owns a shard of log data, applies pushed-down predicates locally, returns partial rows or partial aggregates.

## API (planned)

- `POST /query` — submit a SQL-like query, get back rows plus the chosen execution plan.
- `GET /plan?sql=...` — dry-run: return the optimized plan without executing.
- `GET /nodes` — list registered partition nodes and their health.
- `GET /` — web UI for query submission and plan inspection.

## What I Learned

_To be filled in as the project evolves._

# MapReduce Log Processor

A distributed MapReduce framework that processes batches of log events through a map-shuffle-reduce pipeline with fault-tolerant coordination, implemented in Python with Docker-based workers.

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST API for job submission and monitoring)
- **Database**: PostgreSQL 16 (job/task state, final results)
- **Cache/Shuffle Store**: Redis 7 (intermediate key-value pairs via msgpack)
- **Task Coordination**: Custom coordinator with heartbeat-based fault detection
- **Serialization**: msgpack (shuffle data), JSON (API and log input)
- **Containerization**: Docker, Docker Compose

## Architecture

```
                          ┌──────────────────────────┐
                          │      Coordinator          │
  ┌──────────┐   REST     │      (FastAPI)            │
  │  Client   │──────────▶│                           │
  │  (curl)   │◀──────────│  - Job management         │
  └──────────┘            │  - Task scheduling        │
                          │  - Heartbeat monitoring    │
                          │  - Crash recovery          │
                          └─────────┬────────────────┘
                                    │ pull model
                      ┌─────────────┼─────────────┐
                      ▼             ▼             ▼
               ┌──────────┐  ┌──────────┐  ┌──────────┐
               │ Worker 1  │  │ Worker 2  │  │ Worker N  │
               │           │  │           │  │           │
               │ MAP phase │  │ MAP phase │  │ MAP phase │
               │  ↓ combine│  │  ↓ combine│  │  ↓ combine│
               │  ↓ shuffle│  │  ↓ shuffle│  │  ↓ shuffle│
               │ REDUCE    │  │ REDUCE    │  │ REDUCE    │
               └──────────┘  └──────────┘  └──────────┘
                      │             │             │
                      ▼             ▼             ▼
               ┌──────────┐  ┌──────────┐  ┌──────────┐
               │  Redis    │  │PostgreSQL│  │Filesystem│
               │ (shuffle) │  │ (results)│  │  (logs)  │
               └──────────┘  └──────────┘  └──────────┘
```

### Pipeline Stages

1. **Map** -- Each worker reads an assigned chunk of the log file, applies a user-defined map function (e.g. `word_count`), and emits key-value pairs. A local combiner pre-aggregates output to reduce shuffle volume.
2. **Shuffle** -- Key-value pairs are hash-partitioned by key into reducer buckets and written to Redis as msgpack-encoded lists.
3. **Reduce** -- Each worker reads its assigned partition from Redis, groups by key, applies the reduce function (e.g. `sum`), and writes final results to PostgreSQL.

### Task Distribution (Pull Model)

Workers poll the coordinator for tasks via `GET /tasks/next`. The coordinator uses `SELECT FOR UPDATE SKIP LOCKED` to atomically assign the next pending task, preventing double-assignment without blocking.

### Fault Tolerance

- Workers send periodic heartbeats to the coordinator.
- If a worker misses heartbeats beyond the configured timeout, it is marked dead and its in-progress tasks are reset to `PENDING` for reassignment.
- Idempotent task execution ensures correctness on retry: map tasks delete existing Redis keys before writing, reduce tasks delete existing results before inserting.
- Crash recovery on coordinator startup re-checks incomplete jobs.

## How to Run

```bash
# Build and start all services (coordinator, 2 workers, postgres, redis)
docker compose up --build -d

# Check logs
docker compose logs -f coordinator
docker compose logs -f worker

# Submit a word count job
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "input_path": "/data/sample-logs.jsonl",
    "map_fn": "word_count",
    "reduce_fn": "sum",
    "num_mappers": 4,
    "num_reducers": 2
  }' | python3 -m json.tool

# Check job status (replace <job_id> with the id from the response)
curl -s http://localhost:8000/jobs/<job_id> | python3 -m json.tool

# Get results once status is COMPLETED
curl -s http://localhost:8000/jobs/<job_id>/result | python3 -m json.tool

# Run unit tests
make test

# Run full E2E verification (starts services, submits jobs, validates results)
make e2e

# Tear down
docker compose down -v
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Submit a new MapReduce job |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{id}` | Get job status and details |
| `GET` | `/jobs/{id}/result` | Retrieve final key-value results |
| `DELETE` | `/jobs/{id}` | Cancel a running job |
| `GET` | `/workers` | List registered workers and status |
| `GET` | `/health` | Coordinator health check |
| `GET` | `/stats` | System stats (jobs, tasks, workers) |
| `GET` | `/metrics` | In-memory metrics (durations, shuffle volume) |

### Submit Job Request Body

```json
{
  "input_path": "/data/sample-logs.jsonl",
  "map_fn": "word_count",
  "reduce_fn": "sum",
  "num_mappers": 4,
  "num_reducers": 2
}
```

**Available map functions**: `word_count`, `error_code`, `url_path`
**Available reduce functions**: `sum`, `count`, `collect`

## Configuration

All settings are controlled via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_URL` | `postgresql+asyncpg://...` | Async PostgreSQL connection string |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `HEARTBEAT_INTERVAL` | `5` | Seconds between heartbeat checks |
| `HEARTBEAT_TIMEOUT` | `15` | Seconds before a worker is marked dead |
| `MAX_RETRIES` | `3` | Max retry attempts per failed task |
| `REDIS_TTL` | `3600` | TTL for intermediate Redis keys (seconds) |
| `MAX_CONCURRENT_TASKS` | `8` | Backpressure limit on running tasks |

## What I Learned

- **Pull model vs push model** for task distribution: having workers poll for tasks (`GET /tasks/next`) is simpler and naturally handles backpressure, compared to a coordinator pushing tasks to workers.
- **`SELECT FOR UPDATE SKIP LOCKED`** provides atomic, non-blocking task assignment in PostgreSQL. Multiple workers can race to claim tasks without deadlocking or double-assigning.
- **Combiner optimization** dramatically reduces shuffle volume. Pre-aggregating word counts on the mapper side before writing to Redis can reduce the number of key-value pairs by 97%+ (e.g. thousands of `("the", 1)` pairs become a single `("the", "4200")`).
- **Heartbeat-based failure detection** is a pragmatic pattern: workers periodically POST a heartbeat, and a background task on the coordinator marks workers as dead if they miss the timeout. Dead workers' tasks are reset for reassignment.
- **Idempotent task execution** is essential for safe retries. Map tasks delete their Redis output keys before writing; reduce tasks delete existing result rows before inserting. This means re-executing a task produces the same result without duplication.
- **msgpack vs JSON** for intermediate serialization: msgpack is more compact and faster to encode/decode than JSON for the high-volume shuffle data between mappers and reducers.
- **Data skew detection**: monitoring partition sizes before the reduce phase helps identify when hash partitioning produces uneven workloads, enabling future optimizations like dynamic repartitioning.

# MapReduce Log Processor

A distributed MapReduce framework that processes batches of log events through a map-shuffle-reduce pipeline with fault-tolerant coordination, implemented in Python with Docker-based workers.

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST API for job submission and monitoring)
- **Task Coordination**: Custom coordinator with heartbeat-based fault detection
- **Serialization**: JSON / MessagePack
- **Storage**: Local filesystem (intermediate shuffle data + final output)
- **Containerization**: Docker, Docker Compose
- **Optional Scaling**: K3d (Kubernetes-based multi-node deployment)

## Architecture

```
┌─────────────┐       ┌──────────────────┐
│   Client     │──────▶│   Coordinator    │
│  (REST API)  │◀──────│   (FastAPI)      │
└─────────────┘       └────────┬─────────┘
                               │ assigns tasks
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
          ┌──────────┐  ┌──────────┐  ┌──────────┐
          │ Worker 1  │  │ Worker 2  │  │ Worker 3  │
          │ (mapper/  │  │ (mapper/  │  │ (mapper/  │
          │  reducer) │  │  reducer) │  │  reducer) │
          └──────────┘  └──────────┘  └──────────┘
```

### Pipeline Stages

1. **Map** — Each worker receives a chunk of log data, applies a user-defined map function, and emits key-value pairs.
2. **Shuffle** — Intermediate key-value pairs are partitioned by key and distributed to the appropriate reducer.
3. **Reduce** — Each worker aggregates values for its assigned keys using a user-defined reduce function.

### Fault Tolerance

- Workers send periodic heartbeats to the coordinator.
- If a worker misses heartbeats, its in-progress tasks are reassigned to healthy workers.
- Idempotent task execution ensures correctness on retry.

## How to Run

```bash
# Build and start all services
docker compose up --build

# Submit a job via the REST API
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"input_path": "/data/sample-logs.jsonl", "map_fn": "word_count", "reduce_fn": "sum"}'

# Check job status
curl http://localhost:8000/jobs/<job_id>

# View results
curl http://localhost:8000/jobs/<job_id>/result
```

## API Endpoints

| Method | Path                     | Description                |
|--------|--------------------------|----------------------------|
| POST   | `/jobs`                  | Submit a new MapReduce job |
| GET    | `/jobs`                  | List all jobs              |
| GET    | `/jobs/{id}`             | Get job status and details |
| GET    | `/jobs/{id}/result`      | Retrieve job output        |
| DELETE | `/jobs/{id}`             | Cancel a running job       |
| GET    | `/workers`               | List registered workers    |
| GET    | `/health`                | Coordinator health check   |

## What I Learned

*(To be filled in as the project progresses.)*

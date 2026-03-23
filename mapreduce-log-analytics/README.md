# MapReduce Log Analytics

A custom distributed batch-processing engine that splits large log files into chunks, processes them in parallel via user-defined map and reduce functions, and serves results through a REST API with a real-time web dashboard.

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST API + WebSocket)
- **Task Processing**: multiprocessing / concurrent.futures
- **Dashboard**: HTML/JS served via FastAPI static files, WebSocket for real-time updates
- **Storage**: File-based (chunked log files + JSON results)
- **Testing**: pytest
- **Containerization**: Docker + Docker Compose

## Architecture

```
┌─────────────┐       ┌──────────────┐       ┌──────────────┐
│  Web UI /    │──────▶│   REST API   │──────▶│  Job Manager │
│  Dashboard   │◀──ws──│  (FastAPI)   │       │              │
└─────────────┘       └──────────────┘       └──────┬───────┘
                                                     │
                                              ┌──────▼───────┐
                                              │   Splitter    │
                                              │  (chunking)   │
                                              └──────┬───────┘
                                                     │
                                    ┌────────────────┼────────────────┐
                                    │                │                │
                              ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
                              │  Mapper 1  │   │  Mapper 2  │   │  Mapper N  │
                              │ + Combiner │   │ + Combiner │   │ + Combiner │
                              └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                                    │                │                │
                              ┌─────▼────────────────▼────────────────▼─────┐
                              │              Shuffle & Sort                  │
                              └─────────────────────┬──────────────────────┘
                                                     │
                                    ┌────────────────┼────────────────┐
                                    │                │                │
                              ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
                              │ Reducer 1  │   │ Reducer 2  │   │ Reducer N  │
                              └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                                    │                │                │
                              ┌─────▼────────────────▼────────────────▼─────┐
                              │             Result Aggregator                │
                              └─────────────────────────────────────────────┘
```

**Combiner optimization**: Each mapper pre-aggregates its output locally before shuffle, reducing `(word, 1), (word, 1), (word, 1)` into `(word, 3)`. This dramatically cuts the volume of intermediate data.

## How to Run

```bash
# Build and start the server
docker compose up --build -d

# Generate sample log data
docker compose run --rm generate-logs

# Server is now at http://localhost:8080
# Dashboard at http://localhost:8080/dashboard
# API docs at http://localhost:8080/docs
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/jobs/submit` | Submit a new MapReduce job |
| GET | `/api/jobs` | List all jobs |
| GET | `/api/jobs/{id}` | Get job status and results |
| GET | `/api/functions` | List available analysis functions |
| WS | `/ws` | Real-time job progress updates |
| GET | `/dashboard` | Web dashboard UI |

### Example: Submit a job

```bash
# Generate logs first
docker compose run --rm generate-logs

# Submit a word count job
curl -X POST http://localhost:8080/api/jobs/submit \
  -H "Content-Type: application/json" \
  -d '{"input_files": ["/data/json-logs.jsonl"], "map_fn": "word_count", "reduce_fn": "word_count"}'

# Check job status
curl http://localhost:8080/api/jobs/{job_id}

# List available analysis functions
curl http://localhost:8080/api/functions

# List all jobs
curl http://localhost:8080/api/jobs
```

### Available Analyzers

- **word_count** — Counts word frequency across all log messages
- **pattern_frequency** — Extracts IP addresses, HTTP status codes, and error patterns
- **service_distribution** — Breaks down logs by service name and severity level
- **security** — Identifies top IPs, 404 paths, peak hours, and user agents

## Running Tests

```bash
# Unit + integration tests in Docker
docker compose run --rm --build test

# E2E tests (starts server, runs verification)
docker compose --profile e2e up --build --abort-on-container-exit

# Performance benchmarks
docker compose run --rm test python scripts/benchmark.py
```

## Performance

Benchmarked on 10K JSON log entries (4 workers, 64MB chunks):

| Analyzer | Time | Result Keys |
|----------|------|-------------|
| word_count | <2s | ~200+ |
| pattern_frequency | <2s | ~50+ |
| service_distribution | <2s | ~10 |
| security | <2s | 4 |

The combiner optimization reduces shuffle volume by 10-100x depending on key cardinality, making the engine efficient even at higher log volumes.

## Features

- **File Chunking**: Splits large log files into configurable chunks for parallel processing
- **Parallel Map Phase**: Runs user-defined map functions across chunks using a worker pool
- **Combiner Optimization**: Pre-aggregates map output within each chunk before shuffle
- **Shuffle & Sort**: Groups intermediate key-value pairs by key
- **Parallel Reduce Phase**: Runs user-defined reduce functions across grouped data
- **Built-in Analyzers**: Word count, pattern frequency, service distribution, security analysis
- **REST API**: Submit jobs, check status, retrieve results
- **WebSocket Dashboard**: Real-time job progress and result visualization
- **Job Management**: Queue and track multiple concurrent jobs
- **Graceful Error Handling**: Timeout support, pool failure recovery, malformed line skipping

## What I Learned

- Implementing a MapReduce execution model from scratch (split -> map -> combine -> shuffle -> reduce)
- The combiner pattern and its dramatic impact on shuffle volume
- Managing parallel worker pools with proper error handling and timeouts
- Real-time progress reporting via WebSocket
- Building a job queue with state machine transitions
- Chunking strategies for large file processing with boundary alignment
- End-to-end testing of data processing pipelines

# MapReduce Log Analytics

A custom distributed batch-processing engine that splits large log files into chunks, processes them in parallel via user-defined map and reduce functions, and serves results through a REST API with a real-time web dashboard.

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST API + WebSocket)
- **Task Processing**: multiprocessing / concurrent.futures
- **Dashboard**: HTML/JS served via FastAPI static files, WebSocket for real-time updates
- **Storage**: File-based (chunked log files + JSON results)
- **Testing**: pytest

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

## How It Runs

This is a **long-lived server** with a REST API and WebSocket endpoint:

1. Start the server
2. Submit MapReduce jobs via API or web UI (upload log files, select map/reduce functions)
3. Monitor job progress in real time via the web dashboard
4. Retrieve results through the API or dashboard when complete

## Features

- **File Chunking**: Splits large log files into configurable chunks for parallel processing
- **Parallel Map Phase**: Runs user-defined map functions across chunks using a worker pool
- **Shuffle & Sort**: Groups intermediate key-value pairs by key
- **Parallel Reduce Phase**: Runs user-defined reduce functions across grouped data
- **Built-in Map/Reduce Functions**: Word count, log-level aggregation, IP frequency, error pattern extraction
- **REST API**: Submit jobs, check status, retrieve results
- **WebSocket Dashboard**: Real-time job progress, worker status, and result visualization
- **Job Management**: Queue, cancel, and track multiple concurrent jobs

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/jobs` | Submit a new MapReduce job |
| GET | `/api/jobs` | List all jobs |
| GET | `/api/jobs/{id}` | Get job status and results |
| DELETE | `/api/jobs/{id}` | Cancel a running job |
| GET | `/api/functions` | List available map/reduce functions |
| POST | `/api/upload` | Upload log files |
| WS | `/ws/dashboard` | Real-time job progress updates |

## How to Run

```bash
# Build and run with Docker
docker-compose up --build

# Or run locally
pip install -r requirements.txt
python -m mapreduce_log_analytics.main

# Server starts at http://localhost:8000
# Dashboard at http://localhost:8000/dashboard
# API docs at http://localhost:8000/docs
```

## Running Tests

```bash
# In Docker
docker-compose run --rm app pytest -v

# Locally
pytest -v
```

## What I Learned

- Implementing a MapReduce execution model from scratch (split → map → shuffle → reduce)
- Managing parallel worker pools and handling stragglers/failures
- Real-time progress reporting via WebSocket
- Building a job queue with state machine transitions
- Chunking strategies for large file processing

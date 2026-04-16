# Inverted Index Log Search Engine

A high-performance inverted index system with specialized log tokenization, a RESTful search API, and a React-based search interface that enables sub-100ms full-text search across log entries.

## Tech Stack

- **Backend**: Python 3.12, FastAPI, Uvicorn
- **Frontend**: React 18, Vite, TypeScript
- **Search Engine**: Custom inverted index with log-aware tokenization
- **Containerization**: Docker, Docker Compose
- **Testing**: pytest, httpx (async), Docker-based E2E and load tests

## Architecture

```
┌─────────────────┐       ┌─────────────────────────────────────┐
│  React Frontend │──────>│         FastAPI Backend              │
│   (port 3000)   │  API  │          (port 8000)                │
└─────────────────┘       │                                     │
                          │  ┌─────────────┐  ┌──────────────┐  │
                          │  │  Tokenizer   │  │   Inverted   │  │
                          │  │  (log-aware) │─>│    Index     │  │
                          │  └─────────────┘  └──────────────┘  │
                          │                                     │
                          │  ┌─────────────────────────────┐    │
                          │  │   Search API (REST)          │    │
                          │  │   - Full-text search         │    │
                          │  │   - Filtered queries         │    │
                          │  │   - Index management         │    │
                          │  └─────────────────────────────┘    │
                          │                                     │
                          │  ┌─────────────────────────────┐    │
                          │  │   Persistence Layer          │    │
                          │  │   - Periodic flush to disk   │    │
                          │  │   - Load on startup          │    │
                          │  └─────────────────────────────┘    │
                          └─────────────────────────────────────┘
```

## Features

- **Log-Aware Tokenization**: Parses timestamps, log levels, IP addresses, paths, and error codes as distinct tokens
- **Inverted Index**: In-memory inverted index with positional data for phrase queries
- **Sub-100ms Search**: Optimized posting list intersection for fast full-text search across 100K+ documents
- **Result Highlighting**: Matched terms are wrapped in `<mark>` tags in search results
- **Autocomplete Suggestions**: Prefix-based term suggestions from the index vocabulary
- **RESTful API**: Endpoints for indexing, searching, stats, and suggestions
- **React Search UI**: Real-time search interface with syntax highlighting and faceted filtering
- **Bulk Indexing**: Batch import of log entries with progress tracking
- **Persistence**: Automatic periodic flush to disk with restore on startup

## API Endpoints

| Method | Endpoint            | Description                     | Example                                          |
|--------|---------------------|---------------------------------|--------------------------------------------------|
| GET    | `/health`           | Health check with index stats   | `curl localhost:8000/health`                     |
| GET    | `/api/search`       | Full-text search                | `curl "localhost:8000/api/search?q=error&limit=10"` |
| GET    | `/api/stats`        | Index statistics                | `curl localhost:8000/api/stats`                  |
| GET    | `/api/suggestions`  | Autocomplete term suggestions   | `curl "localhost:8000/api/suggestions?prefix=err"` |
| POST   | `/api/index`        | Index a single log entry        | See below                                        |
| POST   | `/api/index/bulk`   | Bulk index multiple log entries | See below                                        |

**Index a single document:**

```bash
curl -X POST http://localhost:8000/api/index \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Authentication failed for user admin@corp.com from 192.168.1.100",
    "timestamp": 1713200000.0,
    "service": "auth-service",
    "level": "ERROR"
  }'
```

**Bulk index documents:**

```bash
curl -X POST http://localhost:8000/api/index/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {"message": "Request completed in 42ms", "timestamp": 1713200000.0, "service": "api-gateway", "level": "INFO"},
      {"message": "Cache miss for key session:abc123", "timestamp": 1713200001.0, "service": "cache-service", "level": "WARN"}
    ]
  }'
```

## How to Run

### With Docker Compose (recommended)

```bash
# Start backend + frontend
docker-compose up --build

# Run unit tests
docker-compose run --rm test

# Run E2E verification
docker-compose --profile e2e up --build --abort-on-container-exit e2e

# Run load test (100K documents)
docker-compose --profile loadtest up --build --abort-on-container-exit loadtest
```

- Backend: http://localhost:8000
- Frontend: http://localhost:3000
- API Docs (Swagger): http://localhost:8000/docs

### Without Docker

**Backend:**

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Frontend:**

```bash
cd frontend
npm install
npm run dev
```

## Performance Benchmarks

Targets verified by the automated load test (`scripts/load_test.py`):

| Metric                         | Target           |
|--------------------------------|------------------|
| Index 100K documents           | < 4 minutes      |
| Indexing throughput             | > 400 docs/sec   |
| Search P95 latency (100K docs) | < 50ms           |
| Concurrent operations          | 15+ with 0 errors|
| Data integrity                 | Zero data loss   |

Run the load test to verify:

```bash
docker-compose --profile loadtest up --build --abort-on-container-exit loadtest
```

## What I Learned

- Building an inverted index from scratch with positional indexing for phrase queries
- Log-specific tokenization strategies (timestamps, IPs, log levels, paths, stack traces)
- Posting list intersection algorithms and their performance characteristics
- Optimizing search latency to sub-100ms for datasets of 100K+ log entries
- Connecting a FastAPI backend to a React frontend with Vite proxy and Nginx reverse proxy
- Docker multi-stage builds and Compose profiles for test/e2e/loadtest isolation
- Designing async load tests with httpx.AsyncClient for concurrent benchmarking

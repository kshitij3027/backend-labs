# Distributed Log Query System Across Partitions

A query coordination system that intelligently searches log data distributed across multiple partitions using the scatter-gather pattern, merging results into coherent, globally-ordered responses.

## Tech Stack

- **Language**: Python 3.12
- **Framework**: FastAPI + Uvicorn
- **HTTP Client**: httpx (async scatter-gather)
- **Validation**: Pydantic v2
- **CLI**: Click
- **Templating**: Jinja2
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker + Docker Compose

## Architecture

```
Client / Web UI  -->  Query Coordinator (:8080)
                          |
                  +-------+-------+
                  v               v
           Partition-1 (:8081)  Partition-2 (:8082)
```

**Query Coordinator** (port 8080): Receives client queries, fans them out (scatter) to all partition servers in parallel, collects partial results, merges and globally sorts them (gather), and returns a unified response.

**Partition Servers** (ports 8081, 8082): Each holds a subset of log data with in-memory indexes. Responds to query requests by searching its local partition and returning matching log entries.

**Key Components:**
- **Partition Map**: Smart routing based on time-range overlap and partition health
- **Scatter-Gather**: Async parallel fan-out with fault tolerance
- **Result Merger**: Heap-based O(n log k) merge-sort with early termination
- **Query Cache**: LRU cache (OrderedDict) for repeated query optimization

## How to Run

### Docker Compose (recommended)
```bash
# Start all services
make run

# Run unit tests
make test

# Run end-to-end tests
make e2e

# View logs
make logs

# Stop all services
make stop

# Clean up
make clean
```

### Manual (development)
```bash
pip install -r requirements.txt

# Terminal 1: Partition 1
PORT=8081 PARTITION_ID=partition_1 python -m src partition

# Terminal 2: Partition 2
PORT=8082 PARTITION_ID=partition_2 python -m src partition

# Terminal 3: Coordinator
PORT=8080 PARTITION_URLS=http://localhost:8081,http://localhost:8082 python -m src coordinator
```

### Demo Script
```bash
make run
python scripts/demo.py
```

## API Endpoints

### Coordinator (port 8080)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI |
| GET | `/health` | Health status with partition and cache info |
| GET | `/stats` | Detailed system statistics |
| POST | `/query` | Execute a distributed query |

### Partition Server (ports 8081, 8082)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Partition health and metadata |
| POST | `/query` | Execute local query |

### Query Format

```json
{
  "time_range": {
    "start": "2026-03-01T00:00:00Z",
    "end": "2026-03-04T23:59:59Z"
  },
  "filters": [
    {"field": "level", "operator": "eq", "value": "ERROR"},
    {"field": "service", "operator": "contains", "value": "auth"}
  ],
  "sort_field": "timestamp",
  "sort_order": "desc",
  "limit": 50
}
```

### Response Format

```json
{
  "query_id": "abc-123",
  "total_results": 50,
  "partitions_queried": 2,
  "partitions_successful": 2,
  "total_execution_time_ms": 23.5,
  "results": [...],
  "cached": false
}
```

## What I Learned

- **Scatter-Gather Pattern**: Fan out queries to multiple partitions in parallel, collect results with fault tolerance, and merge into a unified response
- **Heap-Based Merge Sort**: O(n log k) algorithm for merging k sorted streams using Python's `heapq`
- **Smart Query Routing**: Partition map tracks time ranges to skip irrelevant partitions
- **LRU Caching**: OrderedDict-based cache with configurable eviction for query deduplication
- **Fault Tolerance**: Graceful degradation returning partial results when partitions are unavailable
- **FastAPI Lifespan**: Managing shared resources (httpx client, caches) across request lifecycle

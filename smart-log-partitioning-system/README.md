# Smart Log Partitioning System

A log routing and storage system that partitions log entries across multiple storage nodes using source-based, time-based, and hybrid strategies. A query optimizer prunes irrelevant partitions for 3x+ speedup. Flask web dashboard visualizes partition distribution in real-time.

## Tech Stack

- **Language**: Python 3.12
- **Framework**: Flask 3.1
- **CLI**: Click 8.1
- **Testing**: pytest 8.3
- **Containerization**: Docker / Docker Compose

## Architecture

```
POST /api/ingest  -->  PartitionRouter.route(entry)  -->  PartitionManager.store(pid, entry)
GET  /api/query   -->  QueryOptimizer.optimize(query) -->  PartitionManager.query(pids, filters)
GET  /api/stats   -->  PartitionManager.get_stats() + QueryOptimizer.get_efficiency_metrics()
GET  /             -->  Dashboard HTML (polls /api/stats via JS every 3s)
```

**Core components:**
- **PartitionRouter** — MD5 hash modulo for source routing, hourly buckets for time routing, compound key for hybrid
- **PartitionManager** — `defaultdict(list)` in-memory + JSONL file persistence per partition, with bloom filters for source membership testing
- **QueryOptimizer** — partition pruning (source -> single partition, time -> bucket range), tracks efficiency metrics
- **Web Dashboard** — dark-themed single-page app with auto-refreshing stats, partition bars, query efficiency panel

## How to Run

### Quick Start (Docker)
```bash
make run          # Start app on http://localhost:5050
make stop         # Stop the app
```

### Run Tests
```bash
make test         # Unit + integration tests in Docker (52 tests)
make e2e          # Full end-to-end test (health, ingest, query, stats, dashboard)
```

### Run Demo/Benchmark
```bash
make demo         # Benchmark: 1000 logs, ingestion rate, query speedup
```

### Manual Docker Commands
```bash
docker compose up -d app                                    # Start app
docker compose run --rm app python -m src demo --count 1000 # Run demo
docker compose down                                         # Stop
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check with strategy info |
| `/api/ingest` | POST | Ingest single or batch log entries |
| `/api/query` | GET | Query logs with source/level/time filters + optimization info |
| `/api/stats` | GET | Partition stats, variance, hotspots, query efficiency |
| `/` | GET | Web dashboard |

### Ingest Example
```bash
curl -X POST http://localhost:5050/api/ingest \
  -H "Content-Type: application/json" \
  -d '{"source":"web_server","level":"error","message":"Connection timeout"}'
```

### Query Example
```bash
# Query by source (optimized: scans only 1 of 3 partitions)
curl "http://localhost:5050/api/query?source=web_server"

# Query by level
curl "http://localhost:5050/api/query?level=error"

# Query by time range
curl "http://localhost:5050/api/query?start=2026-02-28T10:00:00&end=2026-02-28T12:00:00"
```

## Partition Strategies

| Strategy | Partition Key | Example ID | Pruning |
|---|---|---|---|
| `source` | MD5(source) % num_nodes | `"1"` | Source query -> 1/N partitions |
| `time` | Hourly time buckets | `"20260228_14"` | Time range -> bucket subset |
| `hybrid` | Source + time compound | `"1_20260228_14"` | Both source + time pruning |

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `PARTITION_STRATEGY` | `source` | Routing strategy (source/time/hybrid) |
| `PARTITION_NUM_NODES` | `3` | Number of source partition nodes |
| `PARTITION_TIME_BUCKET_HOURS` | `1` | Hours per time bucket |
| `PARTITION_DATA_DIR` | `data` | JSONL persistence directory |
| `HOST_PORT` | `5050` | Host port mapping |

## Project Structure

```
smart-log-partitioning-system/
├── Dockerfile / Dockerfile.test / docker-compose.yml / Makefile
├── requirements.txt
├── src/
│   ├── config.py          # PartitionConfig dataclass + env var loading
│   ├── router.py          # PartitionRouter (source/time/hybrid strategies)
│   ├── manager.py         # PartitionManager + BloomFilter
│   ├── optimizer.py       # QueryOptimizer (partition pruning + metrics)
│   ├── app.py             # Flask app factory
│   ├── cli.py             # Click CLI (serve, demo)
│   └── templates/
│       └── dashboard.html # Single-page dark-themed dashboard
├── tests/                 # 52 tests (router, manager, optimizer, API, bloom)
└── scripts/
    └── demo.py            # Benchmark: 1000 logs, ingestion rate, query speedup
```

## What I Learned

- **Partition pruning** is the core optimization — by routing logs deterministically (MD5 hash mod N), queries for a specific source only need to scan 1 out of N partitions, giving an immediate Nx improvement
- **Bloom filters** provide a lightweight probabilistic check for source membership per partition, avoiding unnecessary full scans with zero false negatives
- **Trade-off between partition count and variance** — with few sources and many nodes, MD5 hash distribution can be uneven; choosing sources that distribute evenly across nodes is important for balanced load
- **JSONL is surprisingly effective** for append-heavy log workloads — one file per partition keeps things simple while still allowing recovery from disk
- **Hybrid partitioning** (source + time compound keys) enables two-dimensional pruning but creates more partitions, which increases the benefit of bloom filter pre-filtering

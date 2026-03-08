# Cluster Performance Monitoring

A multi-node performance monitoring system that collects, aggregates, and analyzes real-time metrics from a simulated distributed log storage cluster, with alerting and a web dashboard.

## What It Does

- **Metric Collection**: Gathers CPU, memory, disk I/O, network, throughput, and latency metrics from 3 simulated cluster nodes (1 primary, 2 replicas) via async background loops
- **Aggregation**: Computes per-node stats (min/max/avg/p95/p99) and cluster-wide totals over a configurable time window
- **Analysis & Alerting**: Evaluates metrics against configurable thresholds, generates warning/critical alerts, computes a 0-100 performance score
- **JSON Reports**: Generates and persists comprehensive performance reports to disk
- **Web Dashboard**: Real-time Chart.js dashboard with WebSocket updates, health badges, per-node detail cards, and alert panels
- **REST API**: Query metrics, nodes, alerts, and reports via JSON endpoints

## Tech Stack

- **Language**: Python 3.12
- **Web Framework**: FastAPI (REST + WebSocket + Jinja2 templates)
- **Dashboard**: Chart.js (CDN) + vanilla JavaScript
- **Metric Storage**: In-memory ring buffers (`collections.deque`)
- **Testing**: pytest + pytest-asyncio + httpx
- **Containerization**: Docker + Docker Compose

## How to Run

### Docker (Recommended)

```bash
# Build images
make build

# Start the monitoring server
make run

# View logs
make logs

# Open dashboard
open http://localhost:8080/dashboard

# Stop
make stop
```

### Run Tests

```bash
# Unit tests (60 tests)
make test

# End-to-end verification
make e2e

# Load/performance test
make load-test
```

### Local Development

```bash
pip install -r requirements.txt
python -m uvicorn src.server:app --host 0.0.0.0 --port 8080
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/metrics` | Cluster-wide metric totals |
| `GET` | `/api/nodes` | List monitored nodes |
| `GET` | `/api/nodes/{id}/metrics` | Per-node aggregated metrics |
| `GET` | `/api/alerts` | Current threshold-breach alerts |
| `GET` | `/api/report` | Latest performance report |
| `POST` | `/api/report/generate` | Generate a new report |
| `POST` | `/api/simulate/degrade` | Inject degradation scenario |
| `POST` | `/api/simulate/recover` | Clear degradation |
| `WS` | `/ws` | Real-time metric stream |
| `GET` | `/dashboard` | Web dashboard UI |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `8080` | Server port |
| `NUM_NODES` | `3` | Number of simulated nodes |
| `COLLECTION_INTERVAL` | `5` | Metric collection interval (seconds) |
| `RETENTION_SECONDS` | `86400` | Metric retention period |
| `CPU_WARNING` | `70` | CPU warning threshold (%) |
| `CPU_CRITICAL` | `90` | CPU critical threshold (%) |
| `MEMORY_WARNING` | `80` | Memory warning threshold (%) |
| `MEMORY_CRITICAL` | `95` | Memory critical threshold (%) |
| `LATENCY_WARNING` | `100` | Latency warning threshold (ms) |
| `LATENCY_CRITICAL` | `500` | Latency critical threshold (ms) |

## Architecture

```
NodeSimulator -> MetricCollector -> MetricStore (ring buffer)
                                       |
                          +------------++-----------+
                          v             v            v
                   MetricAggregator  WebSocket    REST API
                          |          broadcast   /api/metrics
                          v                        |
                  PerformanceAnalyzer              |
                     |        |                    |
                  Alerts   Reporter -> data/*.json |
                     |                             |
                     +----------+------------------+
                                v
                          Dashboard (Chart.js + WS)
```

## What I Learned

- **FastAPI lifespan pattern** for managing startup/shutdown of async background tasks
- **In-memory ring buffers** using `collections.deque(maxlen=N)` for time-series data -- simple, fast, and memory-bounded
- **WebSocket broadcasting** from async background tasks to update dashboards in real-time
- **Percentile computation** with NumPy for p95/p99 metrics aggregation
- **Threshold-based alerting** with score computation and actionable recommendations
- **Chart.js + WebSocket integration** for real-time browser charts without any build step

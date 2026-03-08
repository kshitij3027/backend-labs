# Cluster Performance Monitoring

A multi-node performance monitoring system that collects, aggregates, and analyzes real-time metrics from a simulated distributed log storage cluster, with alerting and a web dashboard.

## What It Does

- **Metric Collection**: Gathers CPU, memory, disk I/O, network, throughput, and latency metrics from simulated cluster nodes via background async loops
- **Aggregation & Analysis**: Computes cluster-wide aggregates (min/max/avg/p95/p99), detects anomalies, and tracks trends over configurable time windows
- **Alerting**: Threshold-based and anomaly-based alerts with configurable rules, cooldowns, and notification channels
- **Web Dashboard**: Real-time WebSocket-powered dashboard showing per-node and cluster-wide metrics, historical charts, and alert status
- **REST API**: Query current and historical metrics, manage alert rules, trigger one-shot performance reports
- **CLI/API Reports**: Generate point-in-time performance reports summarizing cluster health, bottlenecks, and recommendations

## How It Runs

Long-lived server process with a REST API + WebSocket dashboard, backed by background async metric collection loops. Can also generate one-shot performance reports via CLI/API call.

## Tech Stack

- **Language**: Python 3.11+
- **Async Framework**: asyncio
- **Web Framework**: FastAPI (REST API + WebSocket support)
- **Dashboard**: Jinja2 templates + Chart.js (served by FastAPI)
- **Metric Storage**: In-memory time-series ring buffers (with optional SQLite persistence)
- **Alerting**: Custom rule engine with configurable thresholds
- **Simulated Nodes**: Async tasks generating realistic metric streams
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker + docker-compose

## How to Run

> Docker instructions will be added once the project is built.

### Local Development

```bash
cd cluster-performance-monitoring
pip install -r requirements.txt
python -m monitor.main
```

The server starts on `http://localhost:8080` by default.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `8080` | Server port |
| `NUM_NODES` | `5` | Number of simulated cluster nodes |
| `COLLECTION_INTERVAL` | `2` | Metric collection interval (seconds) |
| `RETENTION_SECONDS` | `3600` | How long to keep metric history |
| `ALERT_COOLDOWN` | `60` | Minimum seconds between repeated alerts |
| `DB_PATH` | `:memory:` | SQLite path (`:memory:` for in-memory only) |

### API Endpoints (Planned)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/nodes` | List all monitored nodes |
| `GET` | `/api/nodes/{id}/metrics` | Current metrics for a node |
| `GET` | `/api/cluster/metrics` | Aggregated cluster metrics |
| `GET` | `/api/metrics/history` | Historical metrics with time range |
| `GET` | `/api/alerts` | List active and recent alerts |
| `POST` | `/api/alerts/rules` | Create/update alert rules |
| `POST` | `/api/reports/generate` | Generate a one-shot performance report |
| `WS` | `/ws/metrics` | Real-time metric stream |
| `GET` | `/dashboard` | Web dashboard UI |

## Architecture (Planned)

```
┌─────────────────────────────────────────────────────┐
│                    FastAPI Server                     │
│                                                       │
│  ┌─────────┐  ┌──────────┐  ┌───────────────────┐   │
│  │ REST API │  │WebSocket │  │ Dashboard (Jinja2)│   │
│  └────┬─────┘  └────┬─────┘  └────────┬──────────┘   │
│       │              │                 │               │
│  ┌────▼──────────────▼─────────────────▼──────────┐   │
│  │              Metric Aggregator                  │   │
│  │  (cluster stats, percentiles, trend detection)  │   │
│  └────────────────────┬───────────────────────────┘   │
│                       │                               │
│  ┌────────────────────▼───────────────────────────┐   │
│  │           Time-Series Storage                   │   │
│  │    (ring buffers + optional SQLite persist)     │   │
│  └────────────────────┬───────────────────────────┘   │
│                       │                               │
│  ┌────────────────────▼───────────────────────────┐   │
│  │         Alert Engine (rule evaluation)          │   │
│  └────────────────────────────────────────────────┘   │
│                                                       │
│  ┌────────────────────────────────────────────────┐   │
│  │       Async Metric Collectors (per node)        │   │
│  │  [Node-1] [Node-2] [Node-3] [Node-4] [Node-5]  │   │
│  └────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

## What I Learned

> To be filled in as the project progresses.

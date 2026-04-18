# Faceted Log Search Engine

A multi-dimensional faceted search system that lets users filter structured logs across several attributes (service, level, region, response time, time-of-day) simultaneously, with real-time facet counts updated on every query.

---

## Tech Stack

- **Language:** Python 3.11+
- **Web Framework:** FastAPI (REST API)
- **Cache / Facet Counters:** Redis (hot facet counts, query result cache)
- **Primary Store:** SQLite (persistent structured log store with indexed columns)
- **Frontend:** Interactive dashboard (HTML + vanilla JS or lightweight framework, served by FastAPI)
- **Containerization:** Docker & Docker Compose
- **Testing:** pytest, httpx

---

## What This Project Does

Traditional log search returns a flat list. **Faceted search** lets a user progressively narrow results by clicking facet values — e.g., start with *all logs*, then filter to `service=auth`, then `level=ERROR`, then `region=us-east-1`, while the UI continuously shows how many matching logs fall into each remaining facet bucket.

Every filter combination is answered in real time, and each response carries the full facet breakdown so the UI can re-render counts alongside results.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                           │
│                                                              │
│  ┌──────────────────────┐    ┌────────────────────────────┐ │
│  │    FastAPI Backend    │    │           Redis            │ │
│  │                       │◄──►│  - Hot facet counts         │ │
│  │  ┌─────────────────┐ │    │  - Query result cache       │ │
│  │  │  Ingest API      │ │    │  - Top-N value sets         │ │
│  │  └─────────────────┘ │    └────────────────────────────┘ │
│  │  ┌─────────────────┐ │                                    │
│  │  │  Search API      │ │    ┌────────────────────────────┐ │
│  │  │  (facet filters) │ │◄──►│          SQLite            │ │
│  │  └─────────────────┘ │    │  - Persistent log store     │ │
│  │  ┌─────────────────┐ │    │  - Indexed facet columns    │ │
│  │  │  Facet Counter   │ │    │  - Range queries (latency, │ │
│  │  │  (aggregations)  │ │    │    timestamp)              │ │
│  │  └─────────────────┘ │    └────────────────────────────┘ │
│  │  ┌─────────────────┐ │                                    │
│  │  │  Static UI Host  │ │  GET /  (dashboard)               │
│  │  └─────────────────┘ │                                    │
│  └──────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Facet Dimensions

| Facet | Type | Example Values |
|-------|------|----------------|
| `service` | categorical | `auth`, `payments`, `api-gateway`, `cache` |
| `level` | categorical | `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` |
| `region` | categorical | `us-east-1`, `us-west-2`, `eu-west-1`, `ap-south-1` |
| `response_time_ms` | numeric range | bucketed: `<50`, `50-200`, `200-1000`, `>1000` |
| `time_of_day` | temporal bucket | `00-06`, `06-12`, `12-18`, `18-24` |

Any combination of filters can be applied simultaneously. The response always contains:
- **Matching log entries** (paginated)
- **Facet counts** for every dimension, reflecting the current filter set

---

## How It Runs

1. **Long-lived backend process** — FastAPI server runs continuously.
2. **Redis and SQLite are always up** — Redis holds hot facet counts and query cache, SQLite holds the full structured log store.
3. **Interactive dashboard** served at `/` — users click facet values to filter, results and counts update in real time.
4. **REST API** at `/api/*` — programmatic access to the same search & facet capability.
5. Data flow: logs are ingested via REST → written to SQLite → facet counters updated in Redis → available for faceted search queries.

---

## API Endpoints (planned)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/logs` | Ingest a single log entry or batch |
| GET | `/api/search` | Faceted search — supports multiple filter params, returns results + facet counts |
| GET | `/api/facets` | Get facet counts only (no result rows) |
| GET | `/api/logs/{id}` | Get a single log entry by id |
| GET | `/api/stats` | Total log count, index stats, facet cardinality |
| GET | `/` | Interactive dashboard (HTML) |
| GET | `/health` | Health check |

### Example search request

```
GET /api/search?service=auth&level=ERROR&region=us-east-1&response_time_bucket=200-1000&limit=50
```

### Example response shape

```json
{
  "total": 1247,
  "results": [ { "id": "...", "timestamp": "...", "service": "auth", "level": "ERROR", "...": "..." } ],
  "facets": {
    "service":    { "auth": 1247, "payments": 0, "api-gateway": 0 },
    "level":      { "ERROR": 1247, "WARN": 0, "INFO": 0 },
    "region":     { "us-east-1": 1247, "us-west-2": 0 },
    "response_time_bucket": { "<50": 0, "50-200": 0, "200-1000": 1247, ">1000": 0 },
    "time_of_day": { "00-06": 312, "06-12": 401, "12-18": 380, "18-24": 154 }
  }
}
```

---

## How to Run

> Docker setup and run instructions will be added once the project is implemented.

---

## What I Learned

<!-- To be filled in as the project progresses -->

- Faceted search data modeling (denormalized columns vs. inverted indexes)
- Real-time facet-count maintenance with Redis
- SQLite multi-column indexing trade-offs for mixed categorical + range queries
- Designing an interactive filter UI that stays in sync with server-side counts
- Query-result caching strategies when facet combinations are sparse vs. dense

---

## Project Structure (planned)

```
faceted-log-search-engine/
├── README.md
├── requirements.txt
├── .gitignore
├── (Dockerfile, docker-compose.yml — to be added)
├── src/
│   ├── main.py
│   ├── config.py
│   ├── models.py
│   ├── storage/
│   │   ├── sqlite_store.py
│   │   └── redis_facets.py
│   ├── search/
│   │   ├── query_builder.py
│   │   └── facet_counter.py
│   ├── api/
│   │   ├── logs.py
│   │   ├── search.py
│   │   └── facets.py
│   └── ui/
│       ├── index.html
│       └── static/
└── tests/
    ├── conftest.py
    ├── test_search.py
    ├── test_facets.py
    ├── test_api.py
    └── test_e2e.py
```

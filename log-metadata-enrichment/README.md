# Log Metadata Enrichment

A service that transforms bare-bones log entries into rich, contextual records by attaching system metadata, environment info, and performance metrics. Exposes an HTTP API, a Click CLI, and a web UI for manual testing.

## Tech Stack

- **Language:** Python 3.12
- **Web Framework:** Flask
- **CLI:** Click
- **System Metrics:** psutil
- **Validation:** Pydantic v2
- **Configuration:** PyYAML, python-dotenv
- **Testing:** pytest
- **Containerization:** Docker, docker-compose

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────────────┐     ┌───────────┐
│ HTTP API /   │────▶│  Rule Engine  │────▶│ Collector Registry │────▶│ Formatter │
│ CLI Input    │     │ (YAML rules) │     │  ┌─ SystemInfo    │     │ (JSON out)│
└─────────────┘     └──────────────┘     │  ├─ Environment   │     └───────────┘
                          │              │  └─ Performance   │
                          ▼              └───────────────────┘
                    ┌──────────┐
                    │ Enricher │  ← orchestrator, never raises
                    │ + Stats  │
                    └──────────┘
```

Three layers: **Collectors** (pluggable metadata sources with caching) → **Rule Engine** (YAML-configured, decides which collectors to invoke per log) → **Enricher** (orchestrator that merges original log + metadata, tracks stats).

## How to Run

### Docker (Recommended)

```bash
cd log-metadata-enrichment

# Run the server
make run
# → Server at http://localhost:8080

# Run tests
make test

# Run E2E validation
make e2e

# Run in-container verification
make verify
```

### CLI

```bash
# Enrich a single log
python -m src enrich "ERROR: Database connection failed"

# Enrich with custom source
python -m src enrich "INFO: App started" --source my-app

# Batch enrich from file
python -m src batch logs.txt --output enriched.json

# Start the server
python -m src serve --host 0.0.0.0 --port 8080
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Web UI for manual testing |
| GET | `/health` | Health check |
| POST | `/api/enrich` | Enrich a single log entry |
| GET | `/api/stats` | Pipeline statistics |
| GET | `/api/sample-logs` | 5 sample log messages |

### Example Request

```bash
curl -X POST http://localhost:8080/api/enrich \
  -H "Content-Type: application/json" \
  -d '{"log_message": "ERROR: Database connection failed", "source": "e2e-test"}'
```

### Example Response

```json
{
    "message": "ERROR: Database connection failed",
    "source": "e2e-test",
    "timestamp": "2026-02-27T03:10:27.450975+00:00",
    "hostname": "9ec185afffbf",
    "os_info": "Linux 6.12.67-linuxkit",
    "python_version": "3.12.12",
    "service_name": "log-enrichment",
    "environment": "development",
    "version": "1.0.0",
    "region": "local",
    "cpu_percent": 0.0,
    "memory_percent": 12.8,
    "disk_percent": 2.1,
    "enrichment_duration_ms": 0.32,
    "collectors_applied": ["system_info", "environment", "performance"],
    "enrichment_errors": []
}
```

## Enrichment Rules

Rules are configured in `config/enrichment_rules.yaml`:

- **ERROR/CRITICAL/FATAL** logs → all collectors (system, environment, performance)
- **WARNING** logs → all collectors
- **Default** (any log) → system_info + environment only

## What I Learned

- **Pluggable collector pattern**: Using an abstract base class + registry makes it easy to add new metadata sources without modifying the enricher
- **Never-raise orchestrator**: Two-layer exception handling (collector-level + enricher-level) ensures original logs are never lost even when enrichment fails
- **TTL-based caching**: psutil calls are expensive — caching performance metrics with a 5-second TTL provides fast responses without stale data
- **Priming psutil**: `cpu_percent(interval=0.1)` must be called once at init to prime the counter, then `interval=None` for non-blocking reads
- **Thread-safe stats**: Using `threading.Lock` protects counters since Flask may serve requests across threads
- **YAML rules with fallback**: Service works even without a config file thanks to hardcoded default rules

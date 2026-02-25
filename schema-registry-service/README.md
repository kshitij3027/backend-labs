# Schema Registry Service

A centralized REST API that stores, versions, and validates log message schemas. Producers and consumers in a log pipeline register JSON Schema or Avro schemas under named subjects, with automatic versioning, SHA-256 deduplication, compiled validator caching, and backward compatibility checking.

## Tech Stack

- **Language**: Python 3.12
- **Framework**: Flask
- **Validation**: jsonschema (Draft 7), fastavro
- **Storage**: Atomic JSON file persistence
- **Containerization**: Docker, Docker Compose

## Architecture

```
Client (curl / Web UI / other services)
    |
    v
Flask REST API  (src/app.py)
    |
    |-- SchemaRegistry   (src/registry.py)     -- register, version, dedup, retrieve
    |       |
    |       +-- FileStorage  (src/storage.py)  -- JSON file + atomic writes
    |
    |-- ValidatorManager (src/validators.py)   -- compiled JSON Schema + Avro validators, cached by ID
    |
    |-- CompatChecker    (src/compatibility.py) -- backward compat rules for JSON Schema + Avro
    |
    +-- MetricsTracker   (src/metrics.py)      -- in-memory validation counters
```

**Key design decisions:**
- `create_app(storage_path=None)` factory for testability — no module-level singletons
- Thread-safe storage with `threading.Lock` and atomic disk writes (`tempfile.mkstemp` + `os.replace`)
- Validators compiled on registration and cached in memory — dict lookup on validation
- SHA-256 deduplication: `json.dumps(schema, separators=(',',':'), sort_keys=True)` hashed to detect identical schemas

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Status + schema/subject counts |
| POST | `/schemas` | Register schema `{subject, schema, schema_type?}` |
| GET | `/schemas/subjects` | List all subject names |
| GET | `/schemas/subjects/<subject>` | Latest schema for subject |
| GET | `/schemas/subjects/<subject>/versions` | List version numbers |
| GET | `/schemas/subjects/<subject>/versions/<v>` | Specific version |
| POST | `/validate` | Validate `{subject, data, version?}` |
| POST | `/compatibility/subjects/<subject>` | Check compat `{schema, schema_type?}` |
| GET | `/metrics` | Validation counters + success rate |
| GET | `/` | Web UI dashboard |

### Examples

**Register a schema:**
```bash
curl -X POST http://localhost:8080/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "subject": "user-events",
    "schema": {
      "type": "object",
      "properties": {
        "user_id": {"type": "string"},
        "event_type": {"type": "string"}
      },
      "required": ["user_id", "event_type"]
    }
  }'
```

**Validate data:**
```bash
curl -X POST http://localhost:8080/validate \
  -H "Content-Type: application/json" \
  -d '{
    "subject": "user-events",
    "data": {"user_id": "u123", "event_type": "click"}
  }'
```

**Check compatibility before evolving:**
```bash
curl -X POST http://localhost:8080/compatibility/subjects/user-events \
  -H "Content-Type: application/json" \
  -d '{
    "schema": {
      "type": "object",
      "properties": {
        "user_id": {"type": "string"},
        "event_type": {"type": "string"},
        "metadata": {"type": "object"}
      },
      "required": ["user_id", "event_type"]
    }
  }'
```

## How to Run

```bash
# Build and start the service
make run
# Service available at http://localhost:8080

# Run unit + integration tests in Docker
make test

# Run E2E tests (curl-based against running container)
make e2e

# Clean up
make clean
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_PATH` | `data/registry.json` | Path to the JSON storage file |

Data persists across container restarts via Docker named volumes.

## What I Learned

- **App factory pattern**: Using `create_app(storage_path)` instead of module-level singletons makes testing dramatically simpler — each test gets an isolated app instance with temp storage, no `importlib.reload` hacks.
- **Atomic file writes**: Writing to a temp file then calling `os.replace()` prevents data corruption if the process crashes mid-write. This is the same pattern databases use for write-ahead logs.
- **Schema deduplication via content hashing**: Canonicalizing JSON (`sort_keys=True`, compact separators) before SHA-256 hashing catches identical schemas regardless of key order or whitespace differences.
- **Compiled validator caching**: jsonschema's `Draft7Validator(schema)` compiles the schema into an internal representation. Caching this by schema_id avoids re-compilation on every validation — critical for high-throughput pipelines.
- **Backward compatibility rules**: JSON Schema and Avro have different compatibility semantics. For JSON Schema: removing properties or adding required fields without defaults breaks consumers. For Avro: new fields need defaults, removed fields need defaults in the old schema.
- **Thread safety**: Even in a single-process Flask app, background tasks or test parallelism can cause race conditions. A simple `threading.Lock` around state mutations prevents this without the complexity of a full database.

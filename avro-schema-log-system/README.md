# avro-schema-log-system

A log processing system that serializes/deserializes log events using Apache Avro with support for backward, forward, and full schema compatibility across multiple schema versions.

---

## What This Project Does

This project explores Apache Avro as a schema-driven serialization format for structured log events, with a focus on schema evolution and compatibility guarantees.

**Core capabilities:**

- **Multi-version Avro schemas** -- Defines three versions of an Avro schema for structured log events. Version 1 covers basic fields (timestamp, level, message, source). Version 2 adds optional tracing fields (trace_id, span_id). Version 3 adds operational metadata (tags map, hostname). Each version only adds fields with defaults, guaranteeing full compatibility across all 9 version pairs.
- **Serialization and deserialization** -- Serializes log events into compact Avro binary format and deserializes them back into Python dictionaries, demonstrating the space efficiency of Avro compared to JSON.
- **Schema compatibility testing** -- Tests all three modes of schema compatibility:
  - **Backward compatibility** -- A new schema can read data written by an old schema.
  - **Forward compatibility** -- An old schema can read data written by a new schema.
  - **Full compatibility** -- Both directions work simultaneously.
- **Flask web dashboard and REST API** -- Provides a browser-accessible dashboard and programmatic API endpoints for interacting with schemas, testing compatibility, generating sample serialized data, and deserializing uploaded Avro binary payloads.
- **CLI for testing and benchmarks** -- Also runnable from the command line for unit/integration tests, compatibility matrix generation, and serialization/deserialization throughput benchmarks.

---

## Tech Stack

| Component         | Technology                      |
|-------------------|---------------------------------|
| Language          | Python 3.11+                    |
| Serialization     | Apache Avro (fastavro)          |
| Web framework     | Flask                           |
| Testing           | pytest, pytest-cov              |
| HTTP client       | requests (for integration tests)|
| CLI               | click                           |
| Containerization  | Docker, Docker Compose          |

---

## How to Run

### Docker (recommended)

Build and start all services:

```bash
docker compose up --build
```

The Flask dashboard will be available at `http://localhost:5050` (mapped from container port 5000).

Run tests inside Docker:

```bash
make test
```

Run benchmarks inside Docker:

```bash
docker compose run --rm tests python -m benchmarks.bench_throughput
```

Run the full verification suite:

```bash
bash verify.sh
```

Stop and clean up:

```bash
docker compose down -v
```

### Local Development

Create a virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Start the Flask development server:

```bash
export FLASK_APP=src.app
export FLASK_ENV=development
flask run --host=0.0.0.0 --port=5000
```

Run the test suite:

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Run benchmarks:

```bash
python -m benchmarks.bench_throughput
```

### CLI Usage

The CLI provides commands for common operations outside the web interface:

```bash
# List all registered schema versions
python -m src.cli schemas list

# Check compatibility between two versions
python -m src.cli compatibility check --writer v1 --reader v2

# Generate a full compatibility matrix
python -m src.cli compatibility matrix

# Run serialization/deserialization throughput benchmark
python -m src.cli benchmark --iterations 10000 --schema-version v2

# Serialize a sample log event to a file
python -m src.cli serialize --schema-version v1 --output sample.avro

# Deserialize an Avro file
python -m src.cli deserialize --schema-version v1 --input sample.avro
```

---

## API Endpoints

### `GET /api/schemas`

List all registered schema versions.

**Response:**

```json
{
  "status": "success",
  "data": {
    "schemas": [
      {"version": "v1", "name": "LogEvent", "fields": ["timestamp", "level", "message", "source"]},
      {"version": "v2", "name": "LogEvent", "fields": ["timestamp", "level", "message", "source", "trace_id", "span_id"]},
      {"version": "v3", "name": "LogEvent", "fields": ["timestamp", "level", "message", "source", "trace_id", "span_id", "tags", "hostname"]}
    ]
  }
}
```

### `GET /api/schemas/<version>`

Get the full Avro schema definition for a specific version.

**Parameters:**
- `version` (path) -- Schema version identifier (e.g., `v1`, `v2`, `v3`).

**Response:**

```json
{
  "version": "v1",
  "schema": {
    "type": "record",
    "name": "LogEvent",
    "fields": [...]
  }
}
```

### `POST /api/compatibility/check`

Check compatibility between two schema versions.

**Request body:**

```json
{
  "writer_schema": "v1",
  "reader_schema": "v2",
  "mode": "backward"
}
```

`mode` can be `backward`, `forward`, or `full`.

**Response:**

```json
{
  "compatible": true,
  "mode": "backward",
  "writer_schema": "v1",
  "reader_schema": "v2",
  "details": "Reader schema v2 can successfully read data written with schema v1."
}
```

### `POST /api/generate`

Generate a sample log event serialized with a chosen schema version.

**Request body:**

```json
{
  "schema_version": "v2",
  "count": 1
}
```

**Response:**

```json
{
  "schema_version": "v2",
  "count": 1,
  "events": [
    {
      "raw": {"timestamp": 1708617600000, "level": "INFO", "message": "User login", "source": "auth-service", "trace_id": "abc-123", "span_id": "def-456", "tags": {"env": "prod"}},
      "avro_binary_base64": "T2JqAQI...",
      "size_bytes": 142
    }
  ]
}
```

### `POST /api/deserialize`

Deserialize uploaded Avro binary data with a chosen schema version.

**Request body (multipart/form-data):**
- `file` -- The Avro binary file to deserialize.
- `schema_version` -- The schema version to use for reading.

**Response:**

```json
{
  "schema_version": "v2",
  "records": [
    {"timestamp": 1708617600000, "level": "INFO", "message": "User login", "source": "auth-service", "trace_id": "abc-123", "span_id": "def-456", "tags": {"env": "prod"}}
  ],
  "record_count": 1
}
```

---

## Project Structure

```
avro-schema-log-system/
├── README.md
├── requirements.txt
├── .gitignore
├── Dockerfile
├── Dockerfile.test
├── docker-compose.yml
├── Makefile
├── verify.sh
├── health_check.sh
├── load_test.sh
├── schemas/
│   ├── log_event_v1.avsc
│   ├── log_event_v2.avsc
│   └── log_event_v3.avsc
├── src/
│   ├── __init__.py
│   ├── schema_registry.py
│   ├── serializer.py
│   ├── deserializer.py
│   ├── compatibility.py
│   ├── log_event.py
│   ├── app.py
│   ├── cli.py
│   └── validators/
│       ├── __init__.py
│       └── schema_validator.py
├── templates/
│   └── dashboard.html
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_serializer.py
│   ├── test_deserializer.py
│   ├── test_compatibility.py
│   ├── test_schema_validator.py
│   └── test_api.py
└── benchmarks/
    ├── __init__.py
    └── bench_throughput.py
```

| Directory / File         | Purpose                                                        |
|--------------------------|----------------------------------------------------------------|
| `schemas/`               | Avro schema definitions (`.avsc` files) for each version       |
| `src/schema_registry.py` | Loads and manages schema versions from disk                    |
| `src/serializer.py`      | Encodes Python dicts into Avro binary using fastavro           |
| `src/deserializer.py`    | Decodes Avro binary back into Python dicts                     |
| `src/compatibility.py`   | Tests backward, forward, and full compatibility between schemas|
| `src/log_event.py`       | LogEvent dataclass with version-aware serialization            |
| `src/app.py`             | Flask application with REST API and web dashboard              |
| `src/cli.py`             | Click-based CLI for schemas, compatibility, and benchmarks     |
| `src/validators/`        | Schema validation and cross-version compatibility checker      |
| `templates/`             | HTML dashboard template                                        |
| `tests/`                 | pytest test suite (42 tests, 96% coverage)                     |
| `benchmarks/`            | Throughput benchmarks for serialization and deserialization     |
| `verify.sh`              | Comprehensive 8-check verification script                      |
| `health_check.sh`        | Quick health check for running containers                      |
| `load_test.sh`           | Sequential load test (10 requests, verify <100ms)              |

---

## What I Learned

- **Schema evolution requires discipline**: Every new field must have a default value to maintain backward and forward compatibility. Renaming or removing fields breaks compatibility — additive-only changes are the safe path.
- **fastavro vs apache-avro**: fastavro is significantly faster (~5x) and has a cleaner API. `parse_schema()` mutates in place, so deep-copying the raw schema before parsing is essential when you need both raw and parsed copies.
- **Schemaless vs container format**: `schemaless_writer/reader` is ideal for per-record serialization (API responses, network protocols) where the schema is known out-of-band. Container format (`writer/reader`) embeds the schema and supports streaming multiple records — better for file storage.
- **Compatibility testing by trial**: Rather than implementing complex schema comparison logic, the practical approach is to serialize sample data with the writer schema and attempt deserialization with the reader schema. If it works, they're compatible.
- **9/9 compatibility matrix**: When every version only adds fields with defaults, all writer/reader pairs work. This means any consumer can read any producer's data regardless of version skew — a powerful property for distributed systems.

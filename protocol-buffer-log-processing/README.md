# Protocol Buffer Log Processing

A log processing pipeline that generates, validates, serializes, and benchmarks log entries using Protocol Buffers vs JSON, then produces a detailed performance comparison report. Includes high-load concurrent simulation, schema evolution demos, and a production-optimized Docker image.

## Tech Stack

- **Language**: Python 3.12
- **Serialization**: Protocol Buffers (`protobuf 5.29.3`), JSON (stdlib)
- **Concurrency**: `concurrent.futures.ThreadPoolExecutor`
- **Testing**: pytest
- **Containerization**: Docker (multi-stage builds), Docker Compose (profiles)

## Architecture

The pipeline runs five stages in sequence:

1. **Generate** -- Create a batch of realistic log entries with random services, levels, messages, and metadata.
2. **Validate** -- Check every entry against the proto schema constraints (required fields, enum ranges, timestamp bounds, metadata limits).
3. **Serialize** -- Encode the batch in both JSON and Protobuf binary formats and write them to disk.
4. **Benchmark** -- Time serialization and deserialization for both formats across many iterations, collecting mean, stddev, min, and max.
5. **Report** -- Print a formatted comparison report covering speed, size, real-world projections, and high-scale impact.

## Project Structure

```
protocol-buffer-log-processing/
├── main.py                  # CLI entry point — orchestrates the pipeline
├── compile_proto.sh         # Compiles .proto files to Python
├── verify.sh                # Comprehensive verification script
├── Dockerfile               # Development image
├── Dockerfile.test          # Test image
├── Dockerfile.production    # Multi-stage production image (no compiler toolchain)
├── docker-compose.yml       # Basic app, test, and verify services
├── docker-compose.complete.yml  # Full compose with all profiles
├── Makefile                 # Build / run / test / clean / profile targets
├── requirements.txt         # Python dependencies
├── proto/
│   ├── log_entry.proto      # LogEntry and LogBatch schema (v1)
│   └── log_entry_v2.proto   # LogEntryV2 schema (v2 — schema evolution)
├── src/
│   ├── __init__.py
│   ├── config.py            # Dataclass config with env var overrides
│   ├── log_generator.py     # Random log entry generator
│   ├── validator.py         # Schema-aware validation
│   ├── serializer.py        # JSON + Protobuf encode / decode
│   ├── benchmark.py         # Timed benchmark harness
│   ├── report.py            # Formatted report builder
│   ├── high_load.py         # Concurrent multi-service simulation
│   ├── schema_evolution.py  # Forward/backward compatibility demo
│   └── generated/           # Auto-generated protobuf Python code
│       ├── __init__.py
│       ├── log_entry_pb2.py
│       └── log_entry_v2_pb2.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_log_generator.py
│   ├── test_validator.py
│   ├── test_serializer.py
│   └── test_benchmark.py
└── logs/                    # Generated at runtime
    ├── json/batch.json
    └── protobuf/batch.pb
```

## How to Run

### Quick Start

```bash
make build        # Build the Docker image
make run          # Run the full pipeline (generate, validate, serialize, benchmark, report)
```

### Run Tests

```bash
make test         # Build test image and run pytest inside Docker
```

### Full Verification

```bash
docker compose run --rm app bash verify.sh
```

This runs seven automated checks: project structure, Python environment, proto compilation, unit tests, main pipeline execution, log file output, and protobuf-vs-JSON size comparison.

### Available Make Targets

| Target             | Description                                                  |
|--------------------|--------------------------------------------------------------|
| `build`            | Build the Docker image                                       |
| `run`              | Run the pipeline inside Docker                               |
| `test`             | Build test image and run unit tests                          |
| `clean`            | Remove containers, images, volumes, and logs                 |
| `build-prod`       | Build the multi-stage production image                       |
| `run-prod`         | Run the pipeline using the production image                  |
| `compare-images`   | Show size comparison between dev and production images       |
| `run-perf`         | Run pipeline with higher benchmark iterations (500) and 5k logs |
| `run-datagen`      | Run pipeline with 10k logs for data generation               |
| `run-high-load`    | Run concurrent multi-service high-load simulation            |
| `run-verify`       | Run comprehensive verification via compose profiles          |
| `run-all-profiles` | Run tests, verification, and normal pipeline sequentially    |

## Docker Compose Profiles

The project provides two compose files:

- **`docker-compose.yml`** -- Basic compose with app, test, verify, and production services.
- **`docker-compose.complete.yml`** -- Extended compose with all profiles for different run modes.

### Available Profiles

| Profile      | Service          | Description                                      |
|--------------|------------------|--------------------------------------------------|
| *(default)*  | `app`            | Normal pipeline run (1k logs, 100 iterations)    |
| `test`       | `tests`          | Run pytest unit test suite                       |
| `perf`       | `perf`           | Benchmark with 500 iterations and 5k logs        |
| `datagen`    | `datagen`        | Generate 10k logs for data analysis              |
| `high-load`  | `high-load`      | Concurrent multi-service simulation              |
| `verify`     | `verify`         | Run the comprehensive verification script        |
| `production` | `app-production` | Run using the lean multi-stage production image  |

### Usage Examples

```bash
# Run the default pipeline
docker compose -f docker-compose.complete.yml run --rm app python main.py

# Run tests
docker compose -f docker-compose.complete.yml --profile test run --rm tests

# Run performance benchmark
docker compose -f docker-compose.complete.yml --profile perf run --rm perf

# Run high-load simulation
docker compose -f docker-compose.complete.yml --profile high-load run --rm high-load

# Run comprehensive verification
docker compose -f docker-compose.complete.yml --profile verify run --rm verify
```

## High-Load Simulation

The `--high-load` mode simulates a realistic concurrent multi-service logging environment. It spawns one thread per service, each generating logs at a configurable rate for a configurable duration.

### How It Works

- Uses `ThreadPoolExecutor` with one worker per service.
- Each worker generates log entries, serializes them with both JSON and Protobuf, and tracks throughput.
- Results include per-service throughput, aggregate size/speed ratios, and daily cost projections.

### Configuration

| Environment Variable  | Default | Description                         |
|-----------------------|---------|-------------------------------------|
| `HIGH_LOAD`           | `false` | Enable high-load mode               |
| `HIGH_SCALE_RATE`     | `1000`  | Target logs per second per service   |
| `HIGH_LOAD_DURATION`  | `30`    | Simulation duration in seconds       |
| `NUM_SERVICES`        | `5`     | Number of concurrent services        |
| `DAILY_LOG_VOLUME`    | `10000000` | Projected daily logs for cost estimates |

### Running

```bash
# Via Make target (uses compose profile with sensible defaults)
make run-high-load

# Via CLI flag
docker compose run --rm app python main.py --high-load

# Via environment variable
HIGH_LOAD=true docker compose run --rm app python main.py
```

## Schema Evolution

The `--schema-evolution` flag runs a demonstration of Protocol Buffer forward and backward compatibility.

### What It Demonstrates

1. **Forward Compatibility** -- v1 serialized data is read by v2 code. New fields get their default values (empty string, 0, 0.0).
2. **Backward Compatibility** -- v2 serialized data is read by v1 code. Unknown fields are preserved in the wire format.
3. **Round-trip Preservation** -- v2 data passes through a v1 intermediary and back to v2 without losing the extra fields.

### How It Works

Two proto schemas are maintained:
- `proto/log_entry.proto` (v1) -- The original LogEntry schema.
- `proto/log_entry_v2.proto` (v2) -- Extended with `trace_id`, `response_code`, and `duration_ms` fields.

The demo serializes messages with one schema version and deserializes with the other, verifying that data integrity is maintained in both directions.

### Running

```bash
docker compose run --rm app python main.py --schema-evolution
```

## Production Image

The project includes a multi-stage production Dockerfile (`Dockerfile.production`) that produces a lean runtime image without the protobuf compiler toolchain.

### Build Stages

1. **Builder stage** -- Installs `protobuf-compiler`, compiles `.proto` files, installs Python dependencies with `--prefix=/install`.
2. **Runtime stage** -- Copies only the compiled proto output and installed Python packages. Runs as a non-root `appuser`. Includes a health check.

### Size Comparison

```bash
make compare-images    # Shows dev vs production image sizes
```

The production image is typically 40-50% smaller than the development image because it excludes the protobuf compiler, apt caches, and build tools.

### Running

```bash
make build-prod       # Build the production image
make run-prod         # Run the pipeline with the production image
```

## Sample Output

Below is an abbreviated example of the benchmark report produced by the pipeline:

```
========================================================================
  PROTOCOL BUFFERS LOG PROCESSING PIPELINE
========================================================================

[1/5] Generating 1,000 log entries ...
       Generated 1,000 entries.

[2/5] Validating all entries ...
       All 1,000 entries valid.

[3/5] Serializing to JSON and Protobuf ...
       JSON file  : logs/json/batch.json   (266.18 KB)
       Proto file : logs/protobuf/batch.pb (150.44 KB)
       Size ratio : 1.77x (JSON / Protobuf)

[4/5] Running benchmarks (100 iterations) ...
       Benchmarks complete.

[5/5] Generating report ...

========================================================================
  PROTOCOL BUFFERS vs JSON  --  BENCHMARK REPORT
========================================================================

+----------------------------------------------------------------------+
| SECTION 1: SPEED COMPARISON                                          |
+----------------------------------------------------------------------+

  Operation                    Mean (ms)     StdDev    Min (ms)    Max (ms)
  ------------------------------------------------------------------
  JSON serialize                  1.8432     0.2510      1.5100      3.1200
  JSON deserialize                2.1055     0.3120      1.7800      3.5400
  Protobuf serialize              4.2310     0.4800      3.6200      6.0100
  Protobuf deserialize            1.0240     0.1200      0.8900      1.5500

+----------------------------------------------------------------------+
| SECTION 2: SIZE COMPARISON                                           |
+----------------------------------------------------------------------+

  JSON size     :    266.18 KB  (272,568 bytes)
  Protobuf size :    150.44 KB  (154,050 bytes)
  Size ratio    : 1.77x smaller with Protobuf
  Savings       : 43.5%

========================================================================
  SUMMARY: Protobuf is 1.2x faster and 1.8x smaller than JSON
========================================================================

Pipeline finished successfully.
```

## Key Learnings

- **Protobuf produces smaller serialized output** -- approximately 40-50% storage savings compared to JSON for structured log data, which compounds significantly at scale.
- **Pure Python protobuf is slower for serialization** but the size advantage matters more for storage and network transfer in real-world pipelines.
- **Proto schemas provide type safety and compatibility** -- forward and backward compatibility come built-in with proto3, making schema evolution straightforward.
- **Docker Compose profiles** provide a clean way to organize different run modes (test, perf, verify, high-load) without cluttering the default service set.
- **Multi-stage Docker builds** cut production image size by excluding build-time dependencies like the protobuf compiler.
- **Concurrent simulation reveals real-world bottlenecks** -- the high-load mode shows how serialization format choice impacts throughput and storage under multi-service load.
- **Docker-based testing ensures reproducible builds** -- proto compilation, dependency installation, and test execution all happen inside containers, eliminating host environment differences.

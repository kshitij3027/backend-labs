# Protocol Buffer Log Processing

A log processing pipeline that generates, validates, serializes, and benchmarks log entries using Protocol Buffers vs JSON, then produces a detailed performance comparison report.

## Tech Stack

- **Language**: Python 3.12
- **Serialization**: Protocol Buffers (`protobuf 5.29.3`), JSON (stdlib)
- **Testing**: pytest
- **Containerization**: Docker, Docker Compose

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
├── Dockerfile               # Production image
├── Dockerfile.test          # Test image
├── docker-compose.yml       # App, test, and verify services
├── Makefile                 # Build / run / test / clean targets
├── requirements.txt         # Python dependencies
├── proto/
│   └── log_entry.proto      # LogEntry and LogBatch schema
├── src/
│   ├── __init__.py
│   ├── config.py            # Dataclass config with env var overrides
│   ├── log_generator.py     # Random log entry generator
│   ├── validator.py         # Schema-aware validation
│   ├── serializer.py        # JSON + Protobuf encode / decode
│   ├── benchmark.py         # Timed benchmark harness
│   ├── report.py            # Formatted report builder
│   └── generated/           # Auto-generated protobuf Python code
│       ├── __init__.py
│       └── log_entry_pb2.py
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

| Target  | Description                                    |
|---------|------------------------------------------------|
| `build` | Build the Docker image                         |
| `run`   | Run the pipeline inside Docker                 |
| `test`  | Build test image and run unit tests            |
| `clean` | Remove containers, images, volumes, and logs   |

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
- **Docker-based testing ensures reproducible builds** -- proto compilation, dependency installation, and test execution all happen inside containers, eliminating host environment differences.

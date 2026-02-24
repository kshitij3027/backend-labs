# Universal Log Translator

A plugin-based CLI tool that accepts raw log data in multiple formats (JSON, plain text/syslog, Protobuf, Avro), auto-detects the format, and converts it into a single standardized log entry. New formats can be added by writing a single class -- no changes to existing code required.

## Tech Stack

- **Language**: Python 3.12
- **CLI**: Click
- **Serialization**: protobuf, fastavro, grpcio-tools (for proto compilation)
- **Testing**: pytest, pytest-cov
- **Containers**: Docker, Docker Compose

## Architecture

The core pipeline is:

```
raw bytes --> FormatDetector --> BaseHandler.parse() --> LogEntry
```

**Plugin registration via `__init_subclass__`**: Every handler subclasses `BaseHandler` with a `format_name` keyword argument. The base class auto-registers it in a class-level registry -- no decorators, no entry points, no config files. Adding a new format means adding a new class. Zero core files need to change.

**Detection order** (most unambiguous first, most permissive last):

1. **Avro** -- magic bytes `Obj\x01` (4-byte header, trivially detectable)
2. **JSON** -- starts with `{` or `[` after stripping whitespace
3. **Text/Syslog** -- UTF-8 decodable with syslog priority/timestamp patterns
4. **Protobuf** -- varint heuristic (best-effort last resort)

**Performance tracking**: EWMA (exponentially weighted moving average) with alpha=0.1 tracks per-handler parse/detect timing. Every 100 calls the detection order is reordered so the fastest, highest-success-rate handlers are probed first.

## Supported Formats

### JSON
Flexible key mapping for common field names:
- Timestamp: `timestamp`, `ts`, `time`, `@timestamp`, `datetime`
- Level: `level`, `severity`, `log_level`, `loglevel`
- Message: `message`, `msg`, `log`, `text`, `body`

### Text / Syslog
Parses (in order of specificity):
- RFC 5424 syslog (`<priority>version timestamp hostname app-name ...`)
- RFC 3164 syslog (`<priority>Mon DD HH:MM:SS hostname ...`)
- Generic timestamped lines (`2024-01-15T10:30:00 INFO message`)
- Plain text fallback (message only, level=UNKNOWN)

### Protobuf
- Custom `log_entry.proto` schema in `proto/`
- Compiled at build time using `grpcio-tools` (no system `protoc` needed)
- Detection uses varint byte heuristic (placed last in detection order)

### Avro
- Apache Avro OCF (Object Container File) format
- Schema defined in `schemas/log_entry.avsc`
- Detected by 4-byte magic header `Obj\x01`

## How to Run

All commands use Docker. The Makefile wraps `docker compose` for convenience.

```bash
# Build all images
make build

# Run tests (136 tests, ~84% coverage)
make test

# E2E verification (all formats, detect, stdin, text output)
make verify

# Benchmark (10k mixed logs, ~107k logs/sec)
make benchmark

# Clean up containers and images
make clean
```

## CLI Usage

```bash
# Translate a log file (auto-detect format)
docker compose run --rm app translate sample_logs/sample.json

# Explicit format (skip auto-detection)
docker compose run --rm app translate --format json sample_logs/sample.json

# Text output instead of JSON
docker compose run --rm app translate --output text sample_logs/sample.txt

# Detect format only (no parsing)
docker compose run --rm app detect sample_logs/sample.avro

# Read from stdin
cat sample_logs/sample.json | docker compose run --rm -T app translate -

# Benchmark with adaptive reordering
docker compose run --rm app benchmark --count 10000 --adaptive
```

## Adding a New Format

The plugin extension point requires only one new class:

```python
class XmlHandler(BaseHandler, format_name="xml"):
    def can_handle(self, raw_data: bytes) -> bool:
        return raw_data.lstrip().startswith(b"<")

    def parse(self, raw_data: bytes) -> LogEntry:
        # your parsing logic here
        ...
```

No other files need to change. The `__init_subclass__` mechanism registers the handler automatically. The `FormatDetector` will pick it up on the next run.

## Project Structure

```
universal-log-translator/
├── Dockerfile              # Production image
├── Dockerfile.test         # Test/verify/benchmark image
├── docker-compose.yml      # Services: app, tests, verify, benchmark
├── Makefile                # build, test, verify, benchmark, clean
├── requirements.txt        # Python dependencies
├── compile_proto.sh        # Proto compilation script
├── verify.sh               # E2E verification script
├── proto/
│   └── log_entry.proto     # Protobuf schema
├── schemas/
│   └── log_entry.avsc      # Avro schema
├── sample_logs/
│   ├── sample.json         # Sample JSON logs
│   └── sample.txt          # Sample text/syslog logs
├── scripts/
│   └── generate_samples.py # Sample data generator
├── src/
│   ├── __init__.py
│   ├── __main__.py         # Entry point
│   ├── cli.py              # Click CLI (translate, detect, benchmark)
│   ├── models.py           # LogEntry, LogLevel, UnsupportedFormatError
│   ├── base_handler.py     # BaseHandler ABC with auto-registration
│   ├── detector.py         # FormatDetector (ordered probe)
│   ├── normalizer.py       # LogNormalizer (detect + parse)
│   ├── performance.py      # EWMA tracker, adaptive normalizer
│   └── handlers/
│       ├── __init__.py     # Imports all handlers to trigger registration
│       ├── json_handler.py
│       ├── text_handler.py
│       ├── protobuf_handler.py
│       └── avro_handler.py
└── tests/
    ├── conftest.py         # Shared fixtures
    ├── test_models.py
    ├── test_json_handler.py
    ├── test_text_handler.py
    ├── test_protobuf_handler.py
    ├── test_avro_handler.py
    ├── test_detection.py
    ├── test_normalizer.py
    ├── test_performance.py
    └── test_cli.py
```

## Performance

From the benchmark (10,000 mixed-format logs, Docker):

- **Throughput**: ~107,000 logs/sec
- **Success rate**: 100% across all formats
- **Adaptive reordering**: optimizes detection order after warmup based on EWMA scores

## What I Learned

- **`__init_subclass__` for plugin registration** is a clean alternative to decorators or entry_points. The handler just declares its format name in the class definition and gets registered automatically.
- **Detection order matters**: put the most unambiguous checks first (magic bytes for Avro), most permissive last (varint heuristic for Protobuf). Getting this wrong causes false positives.
- **EWMA provides smooth averages without storing history**: with alpha=0.1, it effectively weights roughly the last 10 observations. Good enough for adaptive reordering without memory overhead.
- **`grpcio-tools` for proto compilation** avoids requiring a system `protoc` install. The Python package includes its own compiler, so `python -m grpc_tools.protoc` works everywhere.
- **Avro OCF's 4-byte magic header** (`Obj\x01`) makes it trivially detectable -- the most reliable format check in the system.
- **Syslog RFC 5424 vs RFC 3164**: the version number field in 5424 (always `1`) makes it unambiguous to distinguish from 3164. Without it, the two formats look similar.
- **Protobuf detection is the hardest**: there is no magic header or self-describing framing. The varint heuristic (checking if leading bytes look like valid varint-encoded field tags) is best-effort and must be the last resort in detection order.

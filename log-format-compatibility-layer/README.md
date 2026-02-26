# Log Format Compatibility Layer

A system that auto-detects, parses, and translates logs from multiple formats (syslog RFC 3164/5424, journald, JSON) into a unified schema. Includes a CLI tool, streaming pipeline, and Flask web UI.

## Tech Stack

- **Language**: Python 3.12
- **CLI**: Click 8.1
- **Web Framework**: Flask 3.1
- **Parsing**: Pure regex (no external syslog libraries)
- **Testing**: pytest 8.3 + pytest-cov
- **Containerization**: Docker + Docker Compose

## Architecture

```
                         +------------------+
  Log File / Stream ---> | Detection Engine |
                         +--------+---------+
                                  |
                    +-------------+-------------+
                    |             |              |
              +-----v----+ +-----v-----+ +-----v------+
              |   JSON   | |  Syslog   | |  Journald  |
              |  Adapter | | RFC3164/  | |   Adapter  |
              |          | |   5424    | |            |
              +-----+----+ +-----+-----+ +-----+------+
                    |             |              |
                    +-------------+-------------+
                                  |
                         +--------v---------+
                         |   ParsedLog      |
                         | (Unified Schema) |
                         +--------+---------+
                                  |
                    +-------------+-------------+
                    |             |              |
              +-----v----+ +-----v-----+ +-----v------+
              |   JSON   | | Structured| |   Plain    |
              | Formatter| | Formatter | | Formatter  |
              +----------+ +-----------+ +------------+
```

### Detection Order (cheapest first)
1. **JSON** -- `{` prefix check + `json.loads()` --> confidence 0.95
2. **Syslog RFC 5424** -- `<PRI>VERSION` pattern --> confidence 0.95
3. **Syslog RFC 3164** -- `<PRI>Month` pattern --> confidence 0.90
4. **Journald** -- Weighted heuristics (no `<PRI>` prefix) --> confidence varies

### Key Design Decisions
- Pure regex parsing for speed and permissiveness with malformed logs
- Weighted confidence scoring for format detection
- Generator pipeline for memory-efficient streaming of large files
- Adapter pattern for extensible format support

## How to Run

### Prerequisites
- Docker and Docker Compose

### Quick Start
```bash
# Run all tests
make test

# Process a log file
docker compose run --rm app process logs/samples/mixed_sample.txt --format json

# Start the web UI
make run
# Then open http://localhost:8080

# Run verification suite
make verify

# Run full E2E tests
make e2e
```

### CLI Commands

```bash
# Process logs (translate to unified format)
python -m src process INPUT_FILE [--output-dir DIR] [--format json|structured|plain]

# Detect formats without parsing
python -m src detect INPUT_FILE

# Show and parse sample data
python -m src sample [--type syslog|journald|json|mixed]

# Start web server
python -m src serve [--host HOST] [--port PORT] [--debug]
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Web UI |
| GET | `/health` | Health check |
| POST | `/api/upload` | Process uploaded logs (multipart file or text) |
| GET | `/api/sample?type=mixed` | Get sample data |
| GET | `/api/config` | Current configuration |

## Supported Formats

| Format | Detection | Confidence | Example |
|--------|-----------|------------|---------|
| JSON | `{` prefix + valid parse | 0.95 | `{"level": "ERROR", "message": "timeout"}` |
| Syslog RFC 5424 | `<PRI>VERSION` | 0.95 | `<165>1 2003-10-11T22:14:15.003Z host app ...` |
| Syslog RFC 3164 | `<PRI>Month Day` | 0.90 | `<34>Oct 11 22:14:15 mymachine su: ...` |
| Journald | Heuristics (no PRI) | 0.5-1.0 | `Feb 14 06:36:01 myhost systemd[1]: ...` |

## What I Learned

- **Syslog priority decomposition**: `facility = priority >> 3`, `severity = priority & 0x07`
- **Journald vs Syslog detection**: The key differentiator is the `<PRI>` prefix -- syslog always has it, journald never does
- **Adapter pattern with confidence scoring**: Each adapter reports how confident it is, and the registry picks the best match
- **Generator pipelines in Python**: Using `yield` throughout enables processing arbitrarily large files without loading them into memory
- **Chunked file reading**: 8KB chunks with line boundary tracking for streaming reads

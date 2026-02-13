# Log Processing Pipeline

An end-to-end pipeline that generates, collects, parses, stores, and queries log data using five integrated Docker containers communicating via shared volumes.

## Architecture

```
Generator ──(appends)──> /logs/app.log
Collector ──(polls, reads from offset)──> /data/collected/batch_*.log
Parser    ──(polls, regex parses)──────> /data/parsed/parsed_*.json
Storage   ──(polls, indexes, rotates)──> /data/storage/{active,archive,index}/
Query     ──(reads storage)────────────> stdout
```

Each component is a polling loop with graceful shutdown (SIGINT/SIGTERM). Data flows through Docker named volumes with read-only mounts where possible.

## Tech Stack

- **Language:** Python 3.12
- **Runtime:** Docker / Docker Compose
- **Config:** YAML (central `config.yml`)
- **Storage format:** NDJSON (one JSON object per line)
- **Indexing:** Manifest-based (level + date indexes)
- **Testing:** unittest (runs inside Docker)

## Components

| Component | Description |
|-----------|-------------|
| **Generator** | Produces Apache Combined Log Format lines at a configurable rate |
| **Collector** | Polls source log file, tracks byte offset, writes atomic batch files |
| **Parser** | Reads batch files, applies Apache regex, writes structured JSON arrays |
| **Storage** | Ingests parsed JSON into NDJSON, maintains level/date indexes, handles rotation |
| **Query** | CLI tool for pattern search and index-based lookup with text/JSON output |

## How to Run

### Docker Compose (recommended)

```bash
# Build all images
make build

# Start the pipeline (generator, collector, parser, storage)
make run

# Wait ~15 seconds for data to flow through, then query
make query ARGS="--pattern GET --lines 10"
make query ARGS="--index-type level --index-value ERROR --lines 5"
make query ARGS="--pattern 404 --output json --lines 3"

# View live logs from all services
make logs

# Stop the pipeline
make stop

# Stop and remove all volumes
make clean
```

### Local (no Docker)

```bash
make local-setup
make local-run     # starts all components as subprocesses
make local-stop    # sends SIGTERM to all
```

## Configuration

All settings live in `config.yml`. Each component reads its own section:

```yaml
generator:
  log_file: /logs/app.log
  rate: 10              # lines per second
  format: apache        # apache | nginx | syslog | json | multi

collector:
  poll_interval: 2      # seconds
  batch_size: 100       # max lines per batch file

parser:
  poll_interval: 2

storage:
  rotation_size_mb: 5   # rotate when active file exceeds this
  rotation_hours: 24    # rotate after this many hours

query:
  storage_dir: /data/storage
```

## Query CLI Usage

```
python -m query.main --pattern <regex> [--output text|json] [--lines N]
python -m query.main --index-type <level|date> --index-value <value> [--lines N]
```

## Storage Structure

```
/data/storage/
  active/store_current.ndjson          # currently written NDJSON file
  archive/store_YYYYMMDDTHHMMSS.ndjson # rotated files
  index/
    level/{INFO,WARNING,ERROR}/manifest.json
    date/{YYYY-MM-DD}/manifest.json
```

Each manifest maps data files to line numbers for fast indexed lookups.

## Testing

```bash
docker build -t pipeline-tests -f Dockerfile.test .
docker run --rm pipeline-tests
```

Runs 29 tests: unit tests for each component + end-to-end integration test.

## What I Learned

- **Polling-based pipeline design** — simpler than event-driven, predictable resource usage
- **Atomic file writes** — `tmp` + `os.replace()` prevents partial reads across components
- **Byte offset tracking** — efficient incremental reads with truncation/inode detection
- **NDJSON + manifest indexing** — line-number-based indexing enables fast lookups without a database
- **Docker volume chaining** — named volumes with `:ro` mounts create clean data boundaries
- **Graceful shutdown** — SIGINT/SIGTERM handlers ensure clean state persistence

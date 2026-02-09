# Log Capture Service

A real-time log file watcher that monitors log files using `watchdog`, parses text and JSON formats, applies configurable regex filtering and tagging, buffers entries, and writes structured JSON output in batches. Designed to pair with the [configurable-log-generator](../configurable-log-generator/) via Docker Compose with a shared volume.

## Tech Stack

- **Python 3.12** (Alpine-based Docker image)
- **watchdog** — filesystem event monitoring
- **PyYAML** — filter/tag rule configuration
- **Docker / Docker Compose** — orchestration with the log generator

## Architecture

```
Log Generator  ──writes──>  /app/logs/app.log  (shared volume)
                                    │
                         watchdog on_modified()
                                    │
                            LogHarvester
                        (reads from offset, tracks position)
                                    │
                          thread-safe Queue
                                    │
                            BatchWriter
                  (parse → filter → tag → buffer → flush)
                                    │
                   /app/collected_logs/collected_*.json
```

## How to Run

```bash
# From this directory
docker compose up --build

# Watch collector output
docker compose logs -f log-collector

# Inspect collected files on host
ls -la collected_logs/
cat collected_logs/collected_*.json | python -m json.tool | head -50
```

## Configuration

Configuration is split across three layers:

| Source | What | Examples |
|---|---|---|
| **CLI args** | Runtime paths | `--log-files /app/logs/app.log`, `--output-dir collected_logs/`, `--config config.yml` |
| **Env vars** | Tuning knobs | `BATCH_SIZE=50`, `FLUSH_INTERVAL=5.0`, `REGISTRY_FILE=collected_logs/.registry.json` |
| **YAML file** | Structured rules | Filter patterns (include/exclude), tag rules |

### Filter/Tag Rules (`config.example.yml`)

```yaml
filters:
  - pattern: "Health check"
    action: exclude
  - pattern: "DEBUG"
    action: exclude

tags:
  - name: "critical"
    pattern: "circuit.breaker|out.of.memory|connection.failed"
    field: message
  - name: "payment"
    pattern: "payment|purchase|order"
    field: message
  - name: "slow"
    pattern: "\\b[5-9]\\d{2,}ms\\b"
    field: raw
```

- **Exclude rules**: if any pattern matches the raw line, the entry is dropped.
- **Include rules**: if any exist, at least one must match for the entry to pass.
- **Tag rules**: each entry always gets `severity:<LEVEL>`; additional tags are applied when regex matches the specified field (case-insensitive).

## Output Format

Each batch file is a valid JSON array:

```json
[
  {
    "timestamp": "2025-05-14 10:23:45",
    "level": "INFO",
    "id": "abc-1234",
    "service": "user-service",
    "user_id": "user-67890",
    "request_id": "req-xyz789",
    "duration_ms": 142,
    "message": "User login successful",
    "source_file": "/app/logs/app.log",
    "tags": ["severity:INFO", "payment"],
    "captured_at": "2025-05-14T10:23:46.123456",
    "raw": "2025-05-14 10:23:45 | INFO    | abc-1234 | ..."
  }
]
```

## Project Structure

```
log-capture-service/
├── src/
│   ├── __init__.py        # empty
│   ├── main.py            # entry point: CLI, wiring, signal handling
│   ├── config.py          # Config dataclass, env/YAML/CLI loading
│   ├── models.py          # LogEntry dataclass
│   ├── parsers.py         # text + JSON parsers with auto-detect
│   ├── filters.py         # EntryProcessor: regex filter + tag
│   ├── harvester.py       # LogHarvester: watchdog handler, offset tracking
│   ├── buffer.py          # BatchWriter: queue consumer, batch flush
│   └── registry.py        # OffsetRegistry: persist file positions
├── config.example.yml     # example filter/tag rules
├── .env.example           # env var defaults
├── requirements.txt       # watchdog, pyyaml
├── Dockerfile
├── docker-compose.yml     # generator + collector with shared volume
└── README.md
```

## What I Learned

- **watchdog** delivers filesystem events but can fire duplicates — reading from a tracked offset makes duplicate events harmless (second read finds 0 new bytes).
- **Partial line buffering** is essential when a producer writes lines faster than the consumer reads — without it, you get truncated entries mid-line.
- **Offset registry with atomic writes** (`os.replace`) prevents corruption if the service restarts mid-save.
- **File rotation detection** via inode comparison lets the collector handle log rotation without missing entries or re-reading old data.
- **Thread-safe Queue** cleanly decouples the watchdog event loop (producer) from the batch writer (consumer) without shared mutable state.
- **Three-layer config** (CLI for paths, env vars for tuning, YAML for rules) keeps each concern in its natural home.

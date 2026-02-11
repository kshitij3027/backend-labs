# Log Storage Service

A file-based log storage engine that writes logs to flat files and automatically rotates, compresses, and purges them based on configurable size, time, count, and age policies. Includes a CLI log inspector for querying stored logs.

## Tech Stack

- Language: Python 3.12 (stdlib only, no third-party dependencies)
- Containerization: Docker Compose
- Compression: gzip (stdlib `gzip` + `shutil.copyfileobj`)

## How to Run

### Demo (generates logs with rotation visible in real time)

```bash
docker compose up --build
```

Watch stderr for rotation, compression, and purge events. Log files appear in `./logs/`.

### Run Tests

```bash
docker compose --profile test run --build --rm tests
```

### Inspect Logs (while service is running)

```bash
# List all log files with sizes
docker exec log_storage python log_inspector.py --list

# Read the active log file
docker exec log_storage python log_inspector.py --read application.log

# Search across all files (including compressed)
docker exec log_storage python log_inspector.py --search "ERROR"
```

### Stop

```bash
docker compose down
```

## Configuration

All settings are controlled via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `LOG_DIR` | `./logs` | Directory to store log files |
| `LOG_FILENAME` | `application.log` | Active log file name |
| `MAX_FILE_SIZE_MB` | `10` | Max file size before rotation (MB) |
| `MAX_FILE_SIZE_BYTES` | — | Max file size (bytes, takes precedence over MB) |
| `ROTATION_INTERVAL_SECONDS` | `3600` | Max time before rotation |
| `MAX_FILE_COUNT` | `10` | Max rotated files to keep |
| `MAX_AGE_DAYS` | `7` | Max age of rotated files |
| `COMPRESSION_ENABLED` | `true` | Gzip-compress rotated files |

The demo docker-compose uses small values (`MAX_FILE_SIZE_BYTES=1024`, `ROTATION_INTERVAL_SECONDS=30`, `MAX_FILE_COUNT=5`) so rotation is visible within seconds.

## Architecture

```
main.py              Entry point — generates demo logs, orchestrates rotation/compression/purge
log_inspector.py     CLI wrapper — argparse over src/inspector.py

src/
  config.py          Frozen dataclass loaded from env vars
  writer.py          Append-only writer with rename-and-create rotation
  rotator.py         Gzip compression + age/count retention enforcement
  inspector.py       List, read (with transparent gzip), and search logic
```

### Rotation Strategy: Rename-and-Create

1. Close the active file handle
2. Rename `application.log` to `application.log.YYYYMMDD_HHMMSS_ffffff`
3. Open a fresh `application.log`

Triggers: file size exceeds threshold **or** elapsed time since last rotation exceeds interval.

### Retention Enforcement

Runs after each rotation:
1. **Age-based purge** — delete rotated files older than `MAX_AGE_DAYS`
2. **Count-based purge** — if survivors exceed `MAX_FILE_COUNT`, delete oldest first

### Log Entry Format

```
2025-01-15 12:00:00.123 [INFO] [auth-api] [a1b2c3d4] Request processed successfully
```

`YYYY-MM-DD HH:MM:SS.mmm [LEVEL] [service] [req-id] message`

## What I Learned

- **Rename-and-create vs copy-and-truncate**: Rename-and-create is simpler and avoids the window where the file is being copied. The active log file is unavailable only for the instant between `os.rename()` and `open()`, which is effectively atomic from the writer's perspective.
- **Microsecond precision in filenames**: At high rotation rates (small file sizes), second-precision timestamps can collide. Using `%Y%m%d_%H%M%S_%f` (microseconds) avoids this.
- **Injectable clock for testing**: Passing `time_func` to `LogWriter` and `enforce_retention` makes time-based logic fully deterministic in tests — no `time.sleep()` or fragile timing needed.
- **Streaming gzip compression**: `shutil.copyfileobj()` with `gzip.open()` handles arbitrarily large files without loading them into memory.
- **Thread safety with a simple lock**: A single `threading.Lock()` around write + rotation is sufficient since all file operations are sequential within the lock. No need for more complex concurrency primitives.
- **Age-before-count retention**: Running age-based purge first, then count-based on survivors, ensures that old files are always removed regardless of count limits, and count limits only apply to still-valid files.

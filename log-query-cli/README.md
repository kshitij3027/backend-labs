# log-query-cli

A command-line tool that parses, filters, and searches through application log files. Built around a generator pipeline for memory-efficient processing of large log files.

## Tech Stack

- **Language**: Python 3.12 (stdlib only, zero dependencies)
- **Containerization**: Docker, Docker Compose
- **Testing**: unittest (87 tests — unit + integration)

## Architecture

```
files on disk → reader (gen) → parser (gen) → filters (gen) → islice (gen) → formatter/stats → stdout
```

Every stage is a generator. No intermediate lists are created, making it memory-efficient for large files. Only `--stats` mode consumes the full stream (required for aggregation).

## How to Run

### With Docker (recommended)

```bash
# Build and run against a log file
docker compose run --rm log-query /logs/sample.log

# Filter by level
docker compose run --rm log-query /logs/sample.log --level ERROR

# Search by keyword (case-insensitive)
docker compose run --rm log-query /logs/sample.log --search "database"

# Filter by date
docker compose run --rm log-query /logs/sample.log --date 2025-05-15

# Filter by time range (inclusive, supports cross-midnight)
docker compose run --rm log-query /logs/sample.log --time-range "14:25-14:31"

# Combine filters
docker compose run --rm log-query /logs/sample.log --level ERROR --search "database"

# Limit output
docker compose run --rm log-query /logs/sample.log --lines 5

# JSON output (NDJSON, compatible with jq)
docker compose run --rm log-query /logs/sample.log --output json

# Colorized output
docker compose run --rm log-query /logs/sample.log --color

# Statistics mode
docker compose run --rm log-query /logs/sample.log --stats
docker compose run --rm log-query /logs/sample.log --stats --output json

# Tail mode (follow new lines like tail -f)
docker compose run --rm log-query /logs/sample.log --tail
```

### Without Docker

```bash
python main.py logs/sample.log
python main.py logs/sample.log --level ERROR --search "timeout" --lines 10
```

### Run Tests

```bash
# In Docker
docker compose --profile test run --rm tests

# Locally
python -m unittest discover tests -v
```

## CLI Reference

```
usage: log-query [-h] [--level LEVEL] [--search SEARCH] [--date DATE]
                 [--time-range TIME_RANGE] [--lines LINES]
                 [--output {text,json}] [--color] [--stats] [--tail]
                 files [files ...]

positional arguments:
  files                 Log file path(s) or glob pattern(s)

options:
  --level LEVEL         Filter by log level (ERROR, WARN, INFO, DEBUG)
  --search SEARCH       Filter by keyword in message (case-insensitive)
  --date DATE           Filter by date (YYYY-MM-DD)
  --time-range RANGE    Filter by time range (HH:MM-HH:MM, inclusive)
  --lines LINES         Limit output to N entries
  --output {text,json}  Output format (default: text)
  --color               Colorize output by log level (ANSI)
  --stats               Show statistics instead of log entries
  --tail                Follow a log file for new entries (like tail -f)
```

## Expected Log Format

```
[YYYY-MM-DD HH:MM:SS] [LEVEL] message text here
```

Example:
```
[2025-05-15 14:28:30] [ERROR] Database connection timeout after 30s
```

## Project Structure

```
log-query-cli/
├── main.py              # Entry point: argparse + pipeline orchestration
├── src/
│   ├── parser.py        # LogEntry frozen dataclass + compiled regex
│   ├── reader.py        # Generator-based file reading, glob, tail
│   ├── filters.py       # Filter predicates (level, search, date, time-range)
│   ├── formatter.py     # Output: text, JSON (NDJSON), colorized (ANSI)
│   └── stats.py         # Statistics: level counts, entries/hour, errors
├── tests/
│   ├── test_parser.py
│   ├── test_reader.py
│   ├── test_filters.py
│   ├── test_formatter.py
│   ├── test_stats.py
│   └── test_integration.py   # E2E via subprocess
├── logs/
│   └── sample.log       # 20 hand-crafted entries for testing
├── Dockerfile           # ENTRYPOINT ["python", "main.py"]
├── Dockerfile.test      # Runs unittest discover
└── docker-compose.yml   # Service + test profile
```

## What I Learned

- **Generator pipelines** chain naturally in Python — each stage yields items lazily, keeping memory constant regardless of file size.
- **Frozen dataclasses** provide immutability guarantees that prevent accidental mutation of log entries as they flow through the pipeline.
- **Compiled regex** with `re.compile()` avoids recompiling the pattern on every line, meaningful for large files.
- **`build_filter_chain`** composes multiple predicates into a single callable using closures, keeping the pipeline code clean and the filter logic extensible.
- **Cross-midnight time ranges** require special handling: if start > end, the entry matches when time >= start OR time <= end.
- **NDJSON** (one JSON object per line) is friendlier than JSON arrays for streaming and piping to `jq`.
- **`BrokenPipeError`** handling is essential when piping CLI output to tools like `head` — without it, Python prints a noisy traceback.

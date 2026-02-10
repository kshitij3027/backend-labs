# Log Parsing Service

A Python service that transforms raw, unstructured log lines (Apache, Nginx, JSON, Syslog) into consistent, structured JSON data and collects parsing statistics. Two modes: a one-shot demo script and a long-lived file-watching service.

## Tech Stack

- **Language:** Python 3.12
- **Libraries:** watchdog (file system monitoring)
- **Container:** Docker + Docker Compose

## Supported Log Formats

| Format | Detection | Example |
|--------|-----------|---------|
| Apache Combined | Regex: `host ident user [time] "request" status size` | `192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326` |
| Nginx Combined | Regex: same as Apache + `"referer" "user_agent"` | `93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /page HTTP/1.1" 200 512 "-" "Mozilla/5.0"` |
| JSON | Starts with `{`, parsed via `json.loads` | `{"timestamp":"...","level":"INFO","message":"Started","service":"auth"}` |
| Syslog (RFC 3164) | Starts with `<priority>`, regex parsed | `<13>Jan  5 14:30:01 myhost sshd[12345]: Accepted publickey for user` |

Auto-detection order: JSON -> Syslog -> Nginx -> Apache -> Unknown.

## How to Run

### Demo Mode (one-shot)

```bash
docker build -t log-parser .
docker run --rm log-parser python demo.py
```

Parse a specific file:

```bash
docker run --rm log-parser python demo.py --file samples/mixed.log
```

### Service Mode (file watcher)

```bash
docker compose up --build
```

The service watches `./logs/` for `.log` files. Drop or copy log files in:

```bash
cp samples/apache.log logs/
cp samples/mixed.log logs/
```

Check output in `./parsed_logs/`:
- `parsed_<filename>.json` — structured entries per file
- `parsing_stats.json` — aggregate statistics

Stop the service:

```bash
docker compose down
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LOG_INPUT_DIR` | `./logs` | Directory to watch for `.log` files |
| `LOG_OUTPUT_DIR` | `./parsed_logs` | Directory for parsed JSON output and stats |

## Output Schema

All formats are normalized to a single schema. Fields not present in a format are omitted from JSON output.

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 normalized timestamp |
| `source_format` | string | `"apache"`, `"nginx"`, `"json"`, `"syslog"`, or `"unknown"` |
| `raw` | string | Original log line |
| `parsed` | boolean | Whether parsing succeeded |
| `remote_host` | string | Client IP (Apache/Nginx) |
| `method` | string | HTTP method (Apache/Nginx) |
| `path` | string | Request path (Apache/Nginx) |
| `protocol` | string | HTTP protocol version (Apache/Nginx) |
| `status_code` | integer | HTTP status code (Apache/Nginx) |
| `body_bytes` | integer | Response size in bytes (Apache/Nginx) |
| `referer` | string | Referer header (Nginx) |
| `user_agent` | string | User-Agent header (Nginx) |
| `level` | string | Log level (JSON/Syslog) |
| `message` | string | Log message (JSON/Syslog) |
| `service` | string | Service name (JSON) |
| `extras` | object | Extra JSON keys beyond known fields (JSON) |
| `hostname` | string | Source hostname (Syslog) |
| `priority` | integer | Syslog priority value |
| `facility` | integer | Syslog facility (priority // 8) |
| `severity` | integer | Syslog severity (priority % 8) |
| `tag` | string | Application/process name (Syslog) |
| `pid` | integer | Process ID (Syslog) |

## Project Structure

```
log-parsing-service/
├── src/
│   ├── __init__.py       # Package marker
│   ├── config.py         # Config dataclass, env var loading
│   ├── models.py         # ParsedLogEntry normalized schema
│   ├── parsers.py        # Regex parsers + auto-detect
│   ├── stats.py          # Per-file stats tracking + persistence
│   ├── watcher.py        # Watchdog file handler + processing
│   └── service.py        # Service entry point + signal handling
├── demo.py               # One-shot demo script
├── samples/              # Sample log files for testing
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md
```

## What I Learned

- **Regex design for log parsing:** Apache and Nginx log formats are surprisingly similar — Nginx extends Apache's format with referer and user-agent fields. Ordering the regex attempts from most specific to least specific prevents false matches.
- **Auto-detection strategy:** A simple heuristic (first character check for JSON/Syslog, then try-and-fall-through for Nginx/Apache) works well and avoids the need for user-specified format configuration.
- **Atomic file writes:** Using `tempfile.mkstemp()` + `os.replace()` ensures output files are never in a partially-written state, even if the process crashes mid-write.
- **Watchdog debouncing:** File system events fire multiple times for a single write operation. A 0.5-second debounce window per file prevents redundant re-processing.
- **Full re-read vs offset tracking:** For this use case ("parse ALL lines"), re-reading the entire file on each change is simpler than tracking offsets. Per-file stats replacement prevents double-counting.
- **Syslog RFC 3164 quirks:** Syslog timestamps lack a year field, so the parser prepends the current year. Priority encodes both facility and severity via `priority = facility * 8 + severity`.

# TCP Log Collection Server

A multi-threaded TCP server in Python that receives JSON log messages from remote clients over TCP sockets. The server filters by log level, persists to disk, rate-limits per client IP, and sends acknowledgments.

## Tech Stack

- **Language:** Python 3.12 (stdlib only — no external runtime dependencies)
- **Testing:** pytest, pytest-cov
- **Containerization:** Docker, Docker Compose

## Architecture

```
Client  ──TCP──►  Server (accept loop)
                      │
                      ├─► Thread per client
                      │       │
                      │       ├─ Parse NDJSON line
                      │       ├─ Rate limit check (per IP)
                      │       ├─ Log level filter
                      │       ├─ Persist to file (thread-safe)
                      │       └─ Send JSON ack
                      │
                      └─► Graceful shutdown (SIGINT/SIGTERM)
```

## Protocol

Newline-delimited JSON (NDJSON) over TCP:

```
Client sends:  {"level": "ERROR", "message": "disk full"}\n
Server replies: {"status": "ok", "message": "received"}\n
```

Response types:
| Status | Message | Meaning |
|--------|---------|---------|
| `ok` | `received` | Message accepted and persisted |
| `ok` | `filtered` | Message below minimum log level |
| `error` | `invalid JSON` | Could not parse input |
| `error` | `missing required fields: level, message` | Incomplete payload |
| `error` | `expected JSON object` | Input is not a JSON object |
| `error` | `rate limit exceeded` | Too many requests from this IP |

## Configuration

| Parameter | Default | Env Var |
|---|---|---|
| Host | `0.0.0.0` | `SERVER_HOST` |
| Port | `9000` | `SERVER_PORT` |
| Buffer Size | `4096` | `BUFFER_SIZE` |
| Min Log Level | `INFO` | `MIN_LOG_LEVEL` |
| Log Persistence | `true` | `ENABLE_LOG_PERSISTENCE` |
| Log Directory | `./logs` | `LOG_DIR` |
| Log Filename | `server.log` | `LOG_FILENAME` |
| Rate Limit | `true` | `RATE_LIMIT_ENABLED` |
| Max Requests | `100` | `RATE_LIMIT_MAX_REQUESTS` |
| Window (seconds) | `60` | `RATE_LIMIT_WINDOW_SECONDS` |

## How to Run

All commands use Docker — no local Python environment required.

```bash
# Build images
make build

# Run the server (background)
make run

# Send test messages with the built-in client
make client

# View server logs
make logs

# Stop everything
make stop

# Quick smoke test with netcat
echo '{"level":"ERROR","message":"disk full"}' | nc localhost 9000
```

## How to Test

```bash
# Run all unit + integration tests in Docker
make test
```

This builds a test image with pytest and runs 73 tests covering:
- Config loading and env var overrides
- Log level filtering (parametrized matrix)
- Thread-safe file persistence (including 5-thread concurrency test)
- Fixed-window rate limiter with injectable clock
- Full-stack integration over real TCP sockets

## Project Structure

```
tcp-log-collection-server/
├── main.py              # Entry point
├── test_client.py       # Standalone smoke-test client
├── Dockerfile           # Server image
├── Dockerfile.test      # Test runner image
├── docker-compose.yml   # Server + test + client services
├── Makefile             # build/run/stop/test shortcuts
├── requirements.txt     # pytest, pytest-cov
├── .env.example         # Documented env var defaults
├── src/
│   ├── config.py        # Frozen dataclass + env var loading
│   ├── filter.py        # Log level comparison
│   ├── persistence.py   # Thread-safe file writer
│   ├── rate_limiter.py  # Per-IP fixed-window rate limiter
│   ├── handler.py       # Per-client connection handler
│   └── server.py        # TCP accept loop + threading
└── tests/
    ├── test_config.py
    ├── test_filter.py
    ├── test_persistence.py
    ├── test_rate_limiter.py
    └── test_integration.py
```

## What I Learned

- **TCP socket programming**: accept loops, `recv` buffering, NDJSON framing with `\n` delimiters, `settimeout` for cooperative shutdown
- **Thread-per-client model**: daemon threads for client handlers, `threading.Event` for shutdown signaling, `threading.Lock` for shared file/dict access
- **Rate limiting patterns**: fixed-window counters, per-IP bucket tracking, injectable `time_func` for deterministic tests
- **Testing network code**: `port=0` for OS-assigned ports (no conflicts), connection retry helpers for startup races, real TCP sockets in integration tests
- **Graceful shutdown**: signal handlers (SIGINT/SIGTERM) setting a shared event, 1-second socket timeouts enabling periodic shutdown checks

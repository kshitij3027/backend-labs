# UDP Log Shipping System

A UDP-based log forwarder with a client that ships JSON log messages and a server that receives, buffers, and persists them to disk.

---

## Architecture

```
                          UDP (fire-and-forget)
 +-----------+       --------------------------->       +------------+
 |           |                                          |            |
 |  Client   |       <---------------------------      |   Server   |
 |           |         ACK (ERROR logs only)            |            |
 +-----------+                                          +-----+------+
                                                              |
                                                         +----v-----+
                                                         |  Buffer  |
                                                         | (count / |
                                                         | timeout) |
                                                         +----+-----+
                                                              |
                                                         +----v-----+
                                                         |   Disk   |
                                                         | (JSONL)  |
                                                         +----------+

                                                         +----------+
                                                         | Dashboard|
                                                         | (Flask)  |
                                                         | :8080    |
                                                         +----------+
```

- **Client** sends structured JSON log entries over UDP to the server.
- **Server** listens on a UDP socket, parses each datagram, and routes it through the buffer.
- **Buffer** accumulates entries in memory and flushes to disk when either the count threshold or the timeout interval is reached.
- **ERROR logs** bypass the buffer entirely: they are written to disk immediately, stored in the in-memory error tracker, and acknowledged back to the client.
- **Dashboard** runs in a daemon thread alongside the server, serving a Flask web UI with live metrics, level distribution, and recent errors.

---

## Features

- **UDP transport** -- connectionless, fire-and-forget delivery using `SOCK_DGRAM`.
- **Buffered disk writes** -- entries accumulate in memory and flush on count threshold or timeout, reducing disk I/O.
- **Priority handling** -- ERROR-level logs bypass the buffer and are written to disk immediately.
- **ACK system for ERROR logs** -- the server sends a UDP acknowledgment back to the client for every ERROR log received, providing selective reliability.
- **Flask monitoring dashboard** -- auto-refreshing web UI (every 2 seconds) showing total received, logs/sec, uptime, level distribution bar chart, and recent errors table.
- **Multi-threaded load testing** -- configurable worker count, target rate, and total log volume for stress testing.
- **Configurable socket buffers** -- `SO_RCVBUF` and `SO_SNDBUF` tunable via environment variables to absorb traffic bursts.
- **Size-based log rotation** -- when the log file exceeds a configurable size limit, it is rotated with up to 10 archived copies.
- **Structured JSON log format** -- every entry includes timestamp, sequence number, app name, level, message, and hostname.
- **In-memory error tracker** -- ring buffer that retains the most recent N error entries for dashboard display.

---

## Tech Stack

| Component       | Technology                                 |
|-----------------|--------------------------------------------|
| Language        | Python 3.12                                |
| Networking      | `socket` module (UDP / `SOCK_DGRAM`)       |
| Web framework   | Flask 3.x                                  |
| Concurrency     | `threading` (daemon threads, locks, events)|
| Testing         | pytest, pytest-cov                         |
| Containers      | Docker + Docker Compose                    |
| Base image      | `python:3.12-alpine`                       |

---

## Configuration

All settings are loaded from environment variables with sensible defaults. Copy `.env.example` to `.env` to customize.

| Variable            | Description                                            | Default     |
|---------------------|--------------------------------------------------------|-------------|
| `SERVER_HOST`       | Address the server binds to                            | `0.0.0.0`  |
| `SERVER_PORT`       | UDP port the server listens on                         | `5514`      |
| `BUFFER_SIZE`       | Max bytes to read per `recvfrom` call                  | `65536`     |
| `LOG_DIR`           | Directory where log files are written                  | `./logs`    |
| `LOG_FILENAME`      | Name of the log file                                   | `server.log`|
| `FLUSH_COUNT`       | Number of buffered entries before a flush              | `100`       |
| `FLUSH_TIMEOUT_SEC` | Seconds before the buffer flushes regardless of count  | `5`         |
| `MAX_ERRORS`        | Max recent errors held in the in-memory ring buffer    | `100`       |
| `RCVBUF_SIZE`       | `SO_RCVBUF` socket receive buffer size (bytes)         | `4194304` (4 MB) |
| `SNDBUF_SIZE`       | `SO_SNDBUF` socket send buffer size (bytes)            | `1048576` (1 MB) |
| `MAX_LOG_SIZE_MB`   | Max log file size in MB before rotation                | `100`       |
| `DASHBOARD_PORT`    | Port for the Flask monitoring dashboard                | `8080`      |

---

## How to Run

### Prerequisites

- Docker and Docker Compose installed.

### Make Commands

```bash
# Build all Docker images
make build

# Start the UDP server (detached)
make run

# Send 20 sample logs from the client
make client

# Run the unit test suite inside Docker
make test

# Run the multi-threaded load test (10,000 logs, 4 workers, 1000 logs/sec target)
make loadtest

# Tail server logs
make logs

# Stop all containers
make stop

# Stop containers, remove images, and delete log files
make clean
```

### Manual Docker Compose

```bash
# Start the server
docker compose up -d udp-server

# Run the client
docker compose --profile client run --build --rm udp-client

# Run tests
docker compose --profile test run --build --rm tests

# Run load test
docker compose --profile loadtest run --build --rm loadtest
```

### Dashboard

Once the server is running, open `http://localhost:8080` in a browser. The dashboard auto-refreshes every 2 seconds and shows:

- Total logs received
- Throughput (logs/sec)
- Server uptime
- Level distribution (horizontal bar chart)
- Recent ERROR entries (table with timestamp, level, sequence, message)

### API Endpoints

| Endpoint   | Method | Description                                       |
|------------|--------|---------------------------------------------------|
| `/`        | GET    | HTML dashboard page                               |
| `/stats`   | GET    | JSON metrics (total, rate, distribution, errors)   |
| `/health`  | GET    | Health check (`{"status": "ok"}`)                  |

---

## Project Structure

```
udp-log-shipping-system/
├── main.py                  # Server entry point (signal handling, dashboard thread)
├── client.py                # Client CLI entry point (argparse, sample log generation)
├── loadtest.py              # Multi-threaded load test script
├── src/
│   ├── __init__.py
│   ├── server.py            # UDPLogServer — receive loop, dispatch, ACK sending
│   ├── client.py            # UDPLogClient — send, ACK listener thread
│   ├── buffer.py            # BufferedWriter — count/timeout flush, log rotation
│   ├── config.py            # Config dataclass, env var loading
│   ├── formatter.py         # Structured JSON log entry builder
│   ├── metrics.py           # Thread-safe counters (total, per-level, rate)
│   ├── error_tracker.py     # In-memory ring buffer for recent ERROR entries
│   └── dashboard.py         # Flask app factory and runner
├── templates/
│   └── dashboard.html       # Dashboard UI (vanilla JS, auto-refresh)
├── tests/
│   ├── __init__.py
│   ├── test_server.py       # Server receive loop and ACK tests
│   ├── test_buffer.py       # Buffered writer and log rotation tests
│   ├── test_client.py       # Client send and ACK listener tests
│   ├── test_formatter.py    # Log entry format validation
│   ├── test_metrics.py      # Metrics counter tests
│   ├── test_error_tracker.py# Error tracker ring buffer tests
│   └── test_dashboard.py    # Flask endpoint tests
├── Dockerfile               # Server image (python:3.12-alpine)
├── Dockerfile.client        # Client / loadtest image
├── Dockerfile.test          # Test runner image
├── docker-compose.yml       # Service definitions (server, client, tests, loadtest)
├── Makefile                 # Shortcut commands (build, run, test, etc.)
├── requirements.txt         # Python dependencies (pytest, pytest-cov, flask)
├── .env.example             # Environment variable template
└── .gitignore
```

---

## What I Learned

### UDP Semantics

UDP is connectionless and unreliable by design. There is no handshake, no retransmission, and no delivery guarantee. A `sendto` call succeeds as long as the datagram fits the OS send buffer -- the sender has no idea whether the receiver got it. This "fire-and-forget" model makes UDP ideal for high-volume telemetry where occasional loss is acceptable but throughput matters.

### Buffered I/O Patterns

Writing every incoming log entry to disk individually would destroy throughput under load. The buffered writer accumulates entries in memory and flushes them in batches when either of two conditions is met: the count threshold (e.g., 100 entries) or the timeout interval (e.g., 5 seconds). This dual-trigger approach balances latency against efficiency -- bursty traffic flushes on count, quiet periods flush on timeout so data is never stuck in memory indefinitely.

### Socket Tuning (SO_RCVBUF)

Under burst traffic, the kernel's UDP receive buffer is the first line of defense against packet loss. If the application cannot drain the socket fast enough, the kernel silently drops datagrams. Setting `SO_RCVBUF` to a larger value (e.g., 4 MB) gives the server more headroom to absorb spikes. The actual value granted by the OS may differ from the requested value, so logging the effective `SO_RCVBUF` after `setsockopt` is important for verification.

### Threading Model

The server uses a single main thread for the receive loop and daemon threads for the flush timer and the Flask dashboard. Daemon threads are terminated automatically when the main thread exits, simplifying shutdown. Cooperative shutdown is coordinated through `threading.Event` -- the signal handler sets the event, the receive loop checks it on every timeout, and the flush timer checks it between sleep intervals.

### ACK-on-Error Pattern

Full acknowledgment for every log would negate the performance benefits of UDP. Instead, the server only sends ACKs for ERROR-level logs, providing selective reliability for the messages that matter most. The client runs a background ACK listener thread to collect these confirmations without blocking the send path.

### Flask in a Daemon Thread

Running the Flask dashboard inside a daemon thread alongside the UDP server avoids the complexity of a separate process or container. The dashboard shares the same `Metrics` and `ErrorTracker` instances as the server (thread-safe via locks), providing real-time visibility with zero serialization overhead. Setting `use_reloader=False` is necessary to prevent Flask from spawning a child process that conflicts with the threading model.

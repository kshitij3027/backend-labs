# Batch Log Shipper

A UDP-based batch log shipper system: a client collects log messages into configurable batches before transmitting them to a remote server, optimizing network usage through batching, compression, and automatic splitting of oversized payloads.

---

## Tech Stack

| Component       | Technology                                 |
|-----------------|--------------------------------------------|
| Language        | Python 3.12 (stdlib only -- no third-party dependencies at runtime) |
| Networking      | `socket` module (UDP / `SOCK_DGRAM`)       |
| Testing         | pytest, pytest-cov                         |
| Containers      | Docker + Docker Compose                    |
| Base image      | `python:3.12-alpine`                       |

---

## Architecture

```
                         UDP (batched datagrams)
 +-------------+       --------------------------->       +--------------+
 |             |                                          |              |
 |  Client App |                                          | UDPLogServer |
 |             |                                          |              |
 +------+------+                                          +------+-------+
        |                                                        |
   +----v------+                                          +------v--------+
   | BatchBuffer|                                          | deserialize   |
   | (size /    |                                          | (auto-detect  |
   |  timer)    |                                          |  compression) |
   +----+------+                                          +------+--------+
        |                                                        |
   +----v------+                                          +------v--------+
   | Serializer |                                          | log entries   |
   | (JSON +    |                                          |               |
   |  zlib)     |                                          +---------------+
   +----+------+
        |
   +----v------+
   | Splitter   |
   | (binary    |
   |  split)    |
   +----+------+
        |
   +----v------+
   | UDPSender  |
   | (retry +   |
   |  backoff)  |
   +------------+
```

Two long-lived processes:

- **UDP Log Server** (`main.py`) -- binds a UDP socket, receives compressed/uncompressed batches, deserializes, and logs each entry.
- **Batch Log Client** (`client.py`) -- collects logs into a buffer, flushes on size threshold or time interval, compresses with zlib, splits oversized payloads, and ships via UDP with retry + exponential backoff.

---

## Features

- **Configurable batch size and flush interval** -- size-based + time-based flushing ensures batches ship promptly regardless of traffic volume.
- **zlib compression with magic header detection** -- the receiver auto-detects compressed vs. raw payloads, making the protocol backward-compatible.
- **Automatic batch splitting** -- payloads exceeding the UDP datagram limit (65,507 bytes) are recursively binary-split into chunks that fit.
- **Retry with exponential backoff and jitter** -- send failures are retried with capped exponential backoff and randomized jitter to prevent thundering herd.
- **Thread-safe metrics collection** -- counters, averages, and interpolated p50/p95 percentiles for batch size and send time, plus flush trigger ratio.
- **Dynamic reconfiguration** -- batch size and flush interval can be changed at runtime without restarting the client.
- **Graceful shutdown** -- SIGINT/SIGTERM handlers coordinate a clean shutdown, draining the buffer and flushing remaining entries before exit.

---

## Configuration

All settings are loaded from environment variables with sensible defaults. CLI flags override environment variables.

| Variable         | Default     | Description                           |
|------------------|-------------|---------------------------------------|
| `SERVER_HOST`    | `0.0.0.0`  | Server bind address                   |
| `SERVER_PORT`    | `9999`      | Server UDP port                       |
| `BUFFER_SIZE`    | `65535`     | Server socket receive buffer (bytes)  |
| `TARGET_HOST`    | `localhost` | Client target server hostname         |
| `TARGET_PORT`    | `9999`      | Client target server port             |
| `BATCH_SIZE`     | `10`        | Entries per batch before flush        |
| `FLUSH_INTERVAL` | `5.0`       | Seconds before timer-based flush      |
| `COMPRESS`       | `true`      | Enable zlib compression               |
| `MAX_RETRIES`    | `3`         | Max send retry attempts               |
| `LOGS_PER_SECOND`| `5`         | Sample log generation rate            |
| `RUN_TIME`       | `30`        | Client run duration in seconds        |

### CLI Flags

```
--batch-size        Entries per batch (overrides BATCH_SIZE)
--batch-interval    Flush interval in seconds (overrides FLUSH_INTERVAL)
--target-host       Server hostname (overrides TARGET_HOST)
--target-port       Server port (overrides TARGET_PORT)
--logs-per-second   Sample log generation rate (overrides LOGS_PER_SECOND)
--run-time          Client run duration in seconds (overrides RUN_TIME)
--no-compress       Disable zlib compression
```

---

## How to Run

### Prerequisites

- Docker and Docker Compose installed.

### Make Commands

```bash
# Build all images
make build

# Start the server (detached)
make run

# Run the client (sends sample logs for 30s)
make client

# Run with custom settings
BATCH_SIZE=20 make client
docker compose run --rm log-client --batch-size 5 --batch-interval 2.0

# View server logs
make logs

# Run all tests (59 tests)
make test

# Stop everything
make stop

# Clean up (remove containers, images, volumes)
make clean
```

### Manual Docker Compose

```bash
# Start the server
docker compose up -d log-server

# Run the client
docker compose --profile client run --rm log-client

# Run tests
docker compose --profile test run --rm tests
```

---

## Project Structure

```
batch-log-shipper/
├── main.py                    # Server entry point (signal handling, config loading)
├── client.py                  # Client entry point (signal handling, sample log generation)
├── src/
│   ├── __init__.py
│   ├── config.py              # Frozen dataclasses loaded from env vars + CLI args
│   ├── models.py              # LogEntry dataclass and factory functions
│   ├── batch_buffer.py        # Thread-safe buffer with size/timer flush triggers
│   ├── batch_client.py        # High-level client orchestrator (buffer + splitter + sender + metrics)
│   ├── serializer.py          # JSON serialization with zlib compression and magic header
│   ├── splitter.py            # Recursive binary-split for oversized UDP payloads
│   ├── sender.py              # UDP sender with retry and exponential backoff
│   ├── metrics.py             # Thread-safe counters, averages, and percentile calculations
│   └── server.py              # UDP receive loop with auto-detect deserialization
├── tests/
│   ├── __init__.py
│   ├── test_config.py         # Config loading from env vars and CLI args
│   ├── test_models.py         # LogEntry creation and serialization
│   ├── test_batch_buffer.py   # Size/timer flush, dynamic reconfig, shutdown drain
│   ├── test_batch_client.py   # End-to-end client flush, timer, shutdown, metrics
│   ├── test_serializer.py     # Round-trip, magic header, compression detection
│   ├── test_splitter.py       # Binary split, chunk size, entry preservation
│   ├── test_sender.py         # UDP send, backoff calculation, jitter range
│   ├── test_server.py         # Receive loop, compressed/uncompressed, invalid data
│   ├── test_metrics.py        # Counters, percentiles, thread safety, uptime
│   └── test_integration.py    # Full pipeline E2E (50 logs, timer flush, shutdown, compression)
├── Dockerfile                 # Server image (python:3.12-alpine)
├── Dockerfile.client          # Client image
├── Dockerfile.test            # Test runner image
├── docker-compose.yml         # Service definitions (server, client, tests)
├── Makefile                   # Shortcut commands (build, run, test, client, etc.)
├── requirements.txt           # Python dependencies (pytest, pytest-cov)
├── .env.example               # Environment variable template
└── .gitignore
```

---

## What I Learned

### Callback-Based Buffer Design

Batching network I/O reduces per-message overhead significantly. The key insight is the callback pattern that decouples the buffer from the network layer -- `BatchBuffer` knows nothing about UDP or serialization; it simply calls `on_flush` when a batch is ready. This makes the buffer independently testable and reusable.

### Lock Discipline

Calling the flush callback OUTSIDE the lock is critical. Holding a lock during network I/O would serialize all producers behind potentially slow sends. The pattern is: acquire the lock, copy the buffer and clear it, release the lock, then invoke the callback. This keeps the critical section as small as possible.

### Compression with Auto-Detection

zlib compression with a 3-byte magic header prefix (`\xcb\xf2` + flags byte) lets the receiver auto-detect the format. If the magic bytes are present, decompress; otherwise, treat as raw JSON. This makes the protocol backward-compatible and allows the client to toggle compression without coordinating with the server.

### Binary-Search Splitting

Recursive binary-split of oversized batches is simple and bounded at O(log n) recursion depth. Serialize the full batch, check the size, and if it exceeds the UDP limit (65,507 bytes), split the entry list in half and recurse on each half. The base case handles a single entry that is itself too large.

### Thread-Safe Metrics with Percentiles

Interpolated percentile calculations (p50/p95) over a list of send times give meaningful production-style observability. The `MetricsCollector` accumulates raw values behind a lock and computes percentiles on demand in the `snapshot()` method, avoiding the complexity of streaming quantile algorithms for this scale.

### Exponential Backoff with Jitter

Capped exponential backoff (0.1s base, 2x growth, 2.0s cap) with a random jitter factor (0.8x--1.2x) prevents the thundering herd problem when multiple clients retry simultaneously. The jitter spreads retry attempts across time, reducing the chance of correlated retransmissions.

### Ephemeral Ports in Tests

Using `port=0` when binding test UDP sockets lets the OS assign an ephemeral port, eliminating flaky tests from port conflicts. The actual port is retrieved from `getsockname()` after binding and passed to the client under test.

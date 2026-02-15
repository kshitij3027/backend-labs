# Log Shipping Client

A TCP log shipping client that reads log lines from a local file and forwards them as NDJSON over TCP to a centralized log server. Supports batch and continuous (tailing) modes, gzip compression, batched sends, automatic reconnection with exponential backoff, health monitoring, and metrics reporting.

## Tech Stack

- **Language:** Python 3.12 (stdlib only — no external dependencies at runtime)
- **Testing:** pytest, pytest-cov
- **Infrastructure:** Docker, Docker Compose

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Log Shipping Client                │
│                                                     │
│   ┌──────────┐    ┌───────────┐    ┌────────────┐  │
│   │  File     │───>│ Formatter │───>│ TCP Client │──┼──> TCP Server
│   │  Reader   │    │ (NDJSON)  │    │ (sendall)  │  │     (ack)
│   └──────────┘    └───────────┘    └────────────┘  │
│                                                     │
│   Resilient mode adds:                              │
│   ┌──────────┐    ┌───────┐    ┌────────────────┐  │
│   │ Producer │───>│ Queue │───>│   Consumer     │  │
│   │ (reader) │    │(50000)│    │ (batch+retry)  │  │
│   └──────────┘    └───────┘    └────────────────┘  │
│                                                     │
│   ┌───────────────┐  ┌─────────────────┐           │
│   │ Health Monitor│  │ Metrics Reporter│           │
│   │ (TCP probe)   │  │ (periodic log)  │           │
│   └───────────────┘  └─────────────────┘           │
└─────────────────────────────────────────────────────┘
```

## Protocol

### Message Format (NDJSON)

Client sends newline-delimited JSON:
```json
{"level": "INFO", "message": "Application started successfully", "timestamp": "2024-01-15 08:23:45"}
```

Server responds with an ack per line:
```json
{"status": "ok", "message": "received"}
```

### Compression Framing

When compression is enabled, messages use length-prefixed gzip framing:
```
[4-byte BE uint32 compressed_length][gzip compressed NDJSON data]
```

The server auto-detects compressed vs plain payloads by checking for gzip magic bytes (`0x1f 0x8b`) at offset 4.

## Configuration

| Env Var | CLI Flag | Default | Description |
|---------|----------|---------|-------------|
| `LOG_FILE` | `--log-file` | `/var/log/app.log` | Path to log file to ship |
| `SERVER_HOST` | `--server-host` | `localhost` | Target server hostname |
| `SERVER_PORT` | `--server-port` | `9000` | Target server port |
| `SHIPPING_MODE` | `--mode` | `batch` | `batch` or `continuous` |
| `COMPRESS` | `--compress` | `false` | Enable gzip compression |
| `BATCH_SIZE` | `--batch-size` | `1` | Lines per network send |
| `RESILIENT` | `--resilient` | `false` | Use buffered producer-consumer |
| `BUFFER_SIZE` | `--buffer-size` | `50000` | Queue capacity (resilient mode) |
| `METRICS_INTERVAL` | `--metrics-interval` | `0` | Metrics report interval in seconds (0 = off) |
| `POLL_INTERVAL` | `--poll-interval` | `0.5` | File polling interval in seconds |

Precedence: CLI args > env vars > defaults.

## How to Run

### Prerequisites

- Docker and Docker Compose

### Commands

```bash
make build    # Build all Docker images
make run      # Start the log server (background)
make batch    # Ship sample_logs.txt in batch mode
make client   # Start continuous-mode client (tails logs/app.log)
make test     # Run all tests in Docker (pytest -v --cov=src)
make logs     # Follow server logs
make stop     # Stop all services
make clean    # Remove containers, images, and log files
```

### Examples

**Batch mode with compression:**
```bash
make run
docker compose --profile batch run --rm -e COMPRESS=true log-client-batch
```

**Resilient mode with batching and metrics:**
```bash
make run
docker compose --profile batch run --rm \
  -e RESILIENT=true \
  -e BATCH_SIZE=5 \
  -e METRICS_INTERVAL=10 \
  log-client-batch
```

**Continuous mode (tail a file):**
```bash
make run
make client
# In another terminal:
echo "2024-01-15 12:00:00 INFO Test message" >> logs/app.log
```

## How to Test

```bash
make test
```

Runs `pytest -v --cov=src` inside a Docker container. Tests cover:
- Config loading (defaults, env vars, CLI precedence)
- Log line parsing and NDJSON formatting
- File reading (batch) and tailing (continuous, rotation, truncation)
- TCP client (connect, send/recv, backoff, shutdown)
- Server (valid/invalid messages, multi-message, compression)
- Basic shipper (batch, continuous, connection failure)
- Resilient shipper (delivery, reconnect, buffer-full, shutdown)
- Compression (roundtrip, framing, auto-detection)
- Batch sending (batch_size=5 with 10 lines, 7 lines, regression)
- Health monitor (healthy, unhealthy, transition, timeout)
- Metrics (counters, latency, snapshot-and-reset, reporter lifecycle)

## Project Structure

```
log-shipping-client/
├── main.py                   # Client CLI entry point
├── server_main.py            # Server entry point
├── sample_logs.txt           # Sample log file for testing
├── Dockerfile                # Client image
├── Dockerfile.server         # Server image
├── Dockerfile.test           # Test runner image
├── docker-compose.yml        # Client + server + test services
├── Makefile                  # Build/run/test shortcuts
├── requirements.txt          # Test dependencies (pytest, pytest-cov)
├── .env.example              # Environment variable reference
├── src/
│   ├── config.py             # Frozen dataclass + env/CLI loading
│   ├── formatter.py          # Log line parsing + NDJSON serialization
│   ├── file_reader.py        # Batch reader + continuous file tailer
│   ├── tcp_client.py         # Socket wrapper with reconnect/backoff
│   ├── server.py             # Simple TCP log server (testing counterpart)
│   ├── shipper.py            # Basic single-threaded shipper
│   ├── resilient_shipper.py  # Buffered producer-consumer with retry
│   ├── compressor.py         # Gzip + length-prefix framing
│   ├── health.py             # TCP connect-probe health monitor
│   └── metrics.py            # Thread-safe counters + periodic reporter
└── tests/
    ├── test_config.py
    ├── test_formatter.py
    ├── test_file_reader.py
    ├── test_tcp_client.py
    ├── test_server.py
    ├── test_shipper.py
    ├── test_resilient_shipper.py
    ├── test_compressor.py
    ├── test_health.py
    └── test_metrics.py
```

## What I Learned

- **TCP stream framing:** TCP is a byte stream, not a message protocol. NDJSON (newline-delimited) works for plain text, but binary/compressed data needs explicit length-prefix framing to know where messages end.
- **Producer-consumer decoupling:** Using `queue.Queue` between file reading and network sending lets each side work at its own pace. Backpressure is built-in via `maxsize`.
- **Exponential backoff with jitter:** Base delay doubles each attempt, capped at a max. Adding random jitter prevents thundering herd when many clients reconnect simultaneously.
- **Thread-safe shutdown:** `threading.Event` lets you interrupt `sleep()`-like waits cleanly with `event.wait(timeout)` instead of blocking `time.sleep()`.
- **Gzip auto-detection:** Gzip data always starts with magic bytes `0x1f 0x8b`. This lets the server auto-detect whether a payload is compressed without out-of-band signaling.
- **Batch amortization:** Grouping multiple log lines into a single `sendall()` reduces syscall overhead and amortizes compression cost across the batch.

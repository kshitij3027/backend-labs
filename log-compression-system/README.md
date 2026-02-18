# Log Compression System

Client-server log shipping system with batched compression over TCP.

## Tech Stack

- **Language:** Python 3.12
- **Compression:** gzip / zlib (standard library)
- **Networking:** TCP sockets (standard library)
- **Monitoring:** psutil (CPU-based adaptive compression)
- **Containers:** Docker, Docker Compose
- **Testing:** pytest

## Architecture

Two long-lived processes communicate over a persistent TCP connection. The client generates synthetic log entries, batches them by count or time window, compresses each batch, and sends it over TCP using a length-prefixed wire protocol. The server accepts connections, reads framed messages, decompresses payloads, and outputs the log entries.

```
Generator -> BatchBuffer -> CompressionHandler -> TCPClient -> [TCP] -> TCPLogReceiver -> Decompress -> Log entries
```

**Log Shipper (Client):**
- `LogGenerator` produces synthetic log entries at a configurable rate
- `BatchBuffer` accumulates entries until a count threshold or time interval is reached
- `CompressionHandler` compresses the JSON-serialized batch using gzip or zlib
- `TCPClient` wraps the compressed payload in a 5-byte framed header and sends it over TCP
- `AdaptiveCompression` (optional) monitors CPU usage and adjusts compression level dynamically

**Log Receiver (Server):**
- `TCPLogReceiver` listens for TCP connections and spawns a thread per client
- Each thread reads framed messages, decompresses based on the flags byte, and logs entries
- `ReceiverMetrics` tracks batches received, bytes compressed/decompressed, and throughput
- `DashboardServer` (optional) serves a live HTML stats page on port 8080

## Wire Protocol

Every TCP message uses a **5-byte header** followed by the payload:

```
Offset  Field            Type              Description
------  ---------------  ----------------  ---------------------------
0-3     payload_length   uint32 big-endian Length of payload in bytes
4       flags            uint8             Bit flags (see below)
5..N    payload          bytes             Compressed or raw JSON array
```

**Flags byte layout:**

```
Bit:  7  6  5  4  3  2  1  0
      [reserved         ][algo][C]

C (bit 0):      1 = compressed, 0 = raw
algo (bit 1-2): 00 = none, 01 = gzip, 10 = zlib
```

Examples: uncompressed = `0x00`, gzip = `0x03`, zlib = `0x05`.

The payload is a UTF-8 JSON array of log entry objects. The self-describing protocol means the server automatically handles multiple compression algorithms from different clients without any server-side configuration.

## Configuration

All settings are controlled via environment variables. See `.env.example` for defaults.

| Variable | Default | Description |
|---|---|---|
| `SERVER_HOST` | `0.0.0.0` | Address the server binds to |
| `SERVER_PORT` | `5000` | TCP port for the server |
| `BATCH_SIZE` | `50` | Number of log entries per batch |
| `BATCH_INTERVAL` | `5.0` | Max seconds before flushing a partial batch |
| `COMPRESSION_ALGORITHM` | `gzip` | Compression algorithm: `gzip` or `zlib` |
| `COMPRESSION_LEVEL` | `6` | Compression level (1-9, higher = better ratio) |
| `COMPRESSION_ENABLED` | `true` | Enable/disable compression |
| `LOG_RATE` | `100` | Log entries generated per second |
| `RUN_TIME` | `30` | Client run duration in seconds |
| `BYPASS_THRESHOLD` | `256` | Skip compression for batches smaller than this (bytes) |
| `ADAPTIVE_ENABLED` | `false` | Enable CPU-based adaptive compression |
| `ADAPTIVE_MIN_LEVEL` | `1` | Minimum compression level (high CPU) |
| `ADAPTIVE_MAX_LEVEL` | `9` | Maximum compression level (low CPU) |
| `ADAPTIVE_CHECK_INTERVAL` | `5.0` | Seconds between CPU checks |

## How to Run

```bash
# Build all Docker images
make build

# Start the server (runs in background)
make run

# Run the client (sends logs for 30 seconds by default)
make client

# View server logs
make logs

# Stop the server
make stop

# Use zlib instead of gzip
COMPRESSION_ALGORITHM=zlib make client

# Enable adaptive compression
ADAPTIVE_ENABLED=true make client

# Clean up images and volumes
make clean
```

## How to Test

```bash
# Run all unit + integration tests in Docker
make test

# Run compression benchmark (compares algorithms and levels)
make benchmark
```

## Project Structure

```
log-compression-system/
├── src/
│   ├── __init__.py            # Package init
│   ├── config.py              # Frozen dataclasses + env loading
│   ├── models.py              # LogEntry dataclass + factory
│   ├── compression.py         # CompressionHandler: gzip/zlib, stats, bypass
│   ├── protocol.py            # Wire protocol: frame encode/decode, Algorithm enum
│   ├── batch_buffer.py        # Count-or-time batching with callback
│   ├── tcp_client.py          # TCP client with framed send + reconnect
│   ├── tcp_server.py          # TCP receiver: accept loop, thread-per-client
│   ├── metrics.py             # Thread-safe ShipperMetrics + ReceiverMetrics
│   ├── log_shipper.py         # Client orchestrator: buffer -> compress -> send
│   ├── log_generator.py       # Synthetic log generator at configurable rate
│   ├── adaptive.py            # CPU-based adaptive compression level (psutil)
│   └── dashboard.py           # Simple HTTP stats dashboard
├── tests/
│   ├── __init__.py
│   ├── test_config.py         # Config loading and defaults
│   ├── test_models.py         # LogEntry creation and serialization
│   ├── test_compression.py    # Compress/decompress, bypass, algorithms
│   ├── test_protocol.py       # Frame encoding/decoding, edge cases
│   ├── test_batch_buffer.py   # Batch count/time triggers, flush
│   ├── test_tcp_client.py     # TCP client send and reconnect
│   ├── test_tcp_server.py     # TCP server accept and receive
│   ├── test_metrics.py        # Metrics recording and snapshot
│   ├── test_adaptive.py       # Adaptive level calculation
│   └── test_integration.py    # End-to-end client-server data flow
├── server_main.py             # Server entry point
├── client_main.py             # Client entry point
├── benchmark.py               # Algorithm/level comparison script
├── Dockerfile                 # Server image
├── Dockerfile.client          # Client image
├── Dockerfile.test            # Test runner image
├── docker-compose.yml         # Service orchestration
├── Makefile                   # Build/run/test shortcuts
├── .env.example               # Environment variable defaults
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## What I Learned

- **Compression trade-offs matter**: Higher compression levels yield better ratios but consume significantly more CPU. Level 6 (default) is a good balance for log data, which is highly compressible due to repeated patterns.
- **Wire protocols need self-describing headers**: Embedding compression flags in the frame header lets the server handle multiple algorithms from different clients without coordination, making the protocol extensible.
- **TCP framing is essential**: Raw TCP is a byte stream with no message boundaries. Length-prefixed framing (4-byte uint32 + payload) is the simplest reliable approach to reconstruct complete messages on the receiving end.
- **Adaptive algorithms add complexity but real value**: Dynamically adjusting compression level based on CPU usage prevents the system from becoming CPU-bound under load while still achieving good compression when resources are available.
- **Thread safety requires careful lock discipline**: Metrics and batch buffers accessed from multiple threads need locking, but keeping I/O outside the lock (batch callback pattern) prevents holding locks during slow operations like network sends.

# TLS-Encrypted Log Transmission

A Python stdlib-only client-server system that sends gzip-compressed, JSON-encoded log entries over TLS 1.3 using self-signed certificates.

## Tech Stack

- **Language**: Python 3.12 (stdlib only — no third-party dependencies)
- **Transport**: TLS 1.3 over TCP (port 8443)
- **Compression**: gzip
- **Serialization**: JSON
- **Containers**: Docker + Docker Compose
- **Certificates**: OpenSSL (self-signed CA + server cert with SANs)

## Architecture

```
┌─────────────────────┐         TLS 1.3          ┌─────────────────────────────┐
│     TLS Client      │ ──────────────────────▶   │        TLS Server           │
│                     │   gzip + len-prefix       │                             │
│  Log Entry (dict)   │   [4B header][payload]    │  Decompress → Parse → Print │
│  → JSON encode      │                           │  → Rotate to .jsonl files   │
│  → gzip compress    │   ◀──────────────────     │  → Track metrics            │
│  → length-prefix    │   JSON ack (uncompressed) │  → Serve dashboard :8080    │
└─────────────────────┘                           └─────────────────────────────┘
```

### Wire Protocol

```
[4-byte BE uint32: payload_length][payload_length bytes: data]
```

- **Client → Server**: payload = `gzip.compress(json.dumps(log_entry).encode())`
- **Server → Client**: payload = `json.dumps({"status":"ok"}).encode()` (acks are uncompressed)

### Certificate Chain

```
generate_certs.sh → CA key/cert → signs → Server cert (with SANs)
                                           DNS:localhost
                                           DNS:tls-server
                                           IP:127.0.0.1
```

## How to Run

### Prerequisites

- Docker and Docker Compose

### Quick Start

```bash
# Start the TLS server (generates certs at build time)
make run

# Send 5 sample log entries
make client

# Send 50 anonymized healthcare entries
make healthcare

# Send logs with proper CA certificate validation
make verified

# View server logs
make logs

# Check dashboard metrics
curl http://localhost:8081/api/stats

# Run all tests (unit + integration)
make test

# Stop everything
make stop

# Full cleanup (images + volumes)
make clean
```

### Available Make Targets

| Target | Description |
|--------|-------------|
| `make build` | Build all Docker images |
| `make run` | Start the TLS server (port 8443 + dashboard on 8081) |
| `make stop` | Stop all services |
| `make test` | Run 45 unit + integration tests in Docker |
| `make client` | Send 5 sample logs over TLS |
| `make healthcare` | Send 50 anonymized healthcare logs |
| `make verified` | Send logs with CA certificate verification |
| `make logs` | Tail server logs |
| `make clean` | Remove containers, images, and volumes |

## Features

### Core
- **TLS 1.3 encryption** with self-signed certificates (CA + server cert with SANs)
- **Gzip compression** with length-prefixed wire framing
- **Thread-per-client** server handling concurrent connections
- **Exponential backoff** retry on client connection failures
- **Docker healthcheck** via TLS handshake probe

### Healthcare Simulation
- **HIPAA-compliant logging** with SHA-256 patient ID anonymization
- **50-entry simulation** across 5 departments and event types
- **Rotating log files** (10 entries per file, JSONL format)

### Observability
- **Web dashboard** on port 8080 with auto-refresh HTML
- **JSON API** at `/api/stats` with real-time metrics
- **Compression statistics** printed on client side

### Security
- **CA certificate validation** mode with shared volume cert distribution
- **TLS 1.2+ minimum** version enforcement
- **Server-side cert wrapping** (individual connections, not listener socket)

## Project Structure

```
tls-encrypted-log-transmission/
├── server_main.py              # Server entry point
├── client_main.py              # Client entry point
├── healthcheck.py              # TLS health check for Docker
├── generate_certs.sh           # OpenSSL cert generation
├── Dockerfile                  # Server image
├── Dockerfile.client           # Client image
├── Dockerfile.test             # Test runner image
├── docker-compose.yml          # Service orchestration
├── Makefile                    # Build/run/test targets
├── src/
│   ├── config.py               # Frozen dataclasses + env vars
│   ├── protocol.py             # 4-byte length-prefixed framing
│   ├── tls_context.py          # SSLContext factories
│   ├── server.py               # TLS accept loop
│   ├── handler.py              # Per-client decompression + ack
│   ├── client.py               # TLS client with retry
│   ├── models.py               # Log entry helpers
│   ├── anonymizer.py           # SHA-256 patient ID hashing
│   ├── log_rotation.py         # Rotating file writer
│   ├── metrics.py              # Thread-safe counters
│   ├── dashboard.py            # HTTP stats dashboard
│   └── simulation.py           # Standard + healthcare sims
└── tests/
    ├── test_config.py
    ├── test_protocol.py
    ├── test_models.py
    ├── test_tls_context.py
    ├── test_anonymizer.py
    ├── test_log_rotation.py
    ├── test_metrics.py
    └── test_integration.py     # Real TLS sockets in-process
```

## What I Learned

- **SSL context wrapping**: Python's `ssl` module requires wrapping individual accepted connections (`wrap_socket(conn, server_side=True)`), not the listener socket itself
- **Certificate SANs**: Modern TLS clients require Subject Alternative Names — just a CN isn't enough for hostname verification
- **Alpine shell compatibility**: Alpine uses `ash`/`sh` which lacks bash features like process substitution (`<(...)`) — use temp files instead
- **Gzip overhead on small payloads**: Individual log entries (~100 bytes) compress poorly due to gzip header overhead; batching would improve ratios
- **TLS 1.3 negotiation**: Python's ssl module auto-negotiates TLS 1.3 when both sides support it, even with `minimum_version=TLSv1_2`
- **Docker healthchecks**: Self-signed TLS services need custom health check scripts that skip cert verification
- **Thread safety for shared state**: Module-level mutable state (log writer, metrics) accessed from handler threads needs explicit locking

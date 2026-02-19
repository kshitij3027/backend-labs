# Backend Labs

A collection of independent mini-projects built during a backend engineering learning journey.

## Projects

| Project | Description | Tech |
|---|---|---|
| [configurable-log-generator](./configurable-log-generator/) | Continuously generates realistic, timestamped application logs at a configurable rate with burst mode and state machine patterns | Python 3.12, Docker |
| [log-capture-service](./log-capture-service/) | Real-time log file watcher that parses text/JSON logs, applies regex filtering and tagging, and writes structured JSON batches | Python 3.12, watchdog, Docker |
| [log-parsing-service](./log-parsing-service/) | Transforms raw log lines (Apache, Nginx, JSON, Syslog) into normalized structured JSON with auto-format detection and aggregate statistics | Python 3.12, watchdog, Docker |
| [log-storage-service](./log-storage-service/) | File-based log storage engine with automatic rotation, compression, and purging based on configurable policies, plus a CLI log inspector | Python 3.12, Docker Compose |
| [log-query-cli](./log-query-cli/) | CLI tool that parses, filters, and searches log files using a memory-efficient generator pipeline with text/JSON/color output and statistics | Python 3.12, Docker |
| [log-processing-pipeline](./log-processing-pipeline/) | End-to-end pipeline that generates, collects, parses, stores, and queries log data using five Docker containers with shared volumes | Python 3.12, Docker Compose |
| [tcp-log-collection-server](./tcp-log-collection-server/) | Multi-threaded TCP server that receives NDJSON log messages, filters by level, persists to disk, and rate-limits per client IP | Python 3.12, Docker Compose |
| [log-shipping-client](./log-shipping-client/) | TCP log shipping client that reads log files, formats as NDJSON, and ships over TCP with compression, batching, reconnect, health monitoring, and metrics | Python 3.12, Docker Compose |
| [udp-log-shipping-system](./udp-log-shipping-system/) | UDP-based log forwarder — client ships JSON logs over UDP, server buffers and persists to disk with priority handling, ACKs, Flask dashboard, and load testing | Python 3.12, UDP sockets, Flask, Docker |
| [batch-log-shipper](./batch-log-shipper/) | UDP-based batch log shipper — client collects logs into configurable batches with compression, splitting, and retry before shipping to a UDP server | Python 3.12, Docker Compose |
| [log-compression-system](./log-compression-system/) | Client-server log shipping system with batched gzip/zlib compression over TCP, adaptive compression levels, wire protocol framing, and stats dashboard | Python 3.12, TCP sockets, gzip/zlib, psutil, Docker Compose |
| [tls-encrypted-log-transmission](./tls-encrypted-log-transmission/) | TLS 1.3 encrypted log transmission with gzip compression, self-signed certs, healthcare simulation with HIPAA anonymization, log rotation, and metrics dashboard | Python 3.12, TLS/SSL, Docker Compose |

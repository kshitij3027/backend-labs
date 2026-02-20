# Load Generator Testing

A load generator that stress-tests a TCP-based log collection server by simulating high-volume log traffic and producing performance benchmark reports.

## Tech Stack

- **Language:** Python 3.12+
- **Networking:** TCP sockets (stdlib)
- **Concurrency:** threading / asyncio
- **Output:** JSON benchmark reports

## Architecture

The system has three components:

| Component | Type | Description |
|-----------|------|-------------|
| **TCP Log Server** | Long-lived process | Accepts TCP connections and ingests log messages |
| **Load Generator** | CLI tool | Sends configurable volumes of log traffic to the server |
| **Benchmark Analyzer** | One-shot CLI | Parses results and outputs a JSON performance report |

### Load Generator Parameters

- `--total-logs` — number of log messages to send
- `--duration` — time-bounded test duration (seconds)
- `--concurrency` — number of simultaneous client connections

## How to Run

<!-- Fill in as development progresses -->

## What I Learned

<!-- Fill in as the project evolves -->

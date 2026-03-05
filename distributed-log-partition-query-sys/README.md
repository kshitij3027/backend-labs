# Distributed Log Partition Query System

A query coordination system that intelligently searches log data distributed across multiple partitions using the scatter-gather pattern, merging results into coherent, globally-ordered responses.

## Tech Stack

- **Language**: Python 3.12
- **Framework**: FastAPI + Uvicorn
- **HTTP Client**: httpx (async scatter-gather)
- **Validation**: Pydantic
- **Logging**: structlog
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker + Docker Compose

## Architecture

```
                          +-------------------+
                          |   REST Client /   |
                          |    Web UI         |
                          +--------+----------+
                                   |
                                   v
                          +-------------------+
                          | Query Coordinator |
                          |   (port 8080)     |
                          +--------+----------+
                                   |
                    +--------------+--------------+
                    |              |              |
                    v              v              v
            +------+------+ +-----+-------+ +----+--------+
            | Partition 0 | | Partition 1 | | Partition N |
            | (port 8081) | | (port 8082) | | (port 808N) |
            +-------------+ +-------------+ +-------------+
```

**Query Coordinator** (port 8080): Receives client queries, fans them out (scatter) to all partition servers in parallel, collects partial results, merges and globally sorts them (gather), and returns a unified response.

**Partition Servers** (ports 8081, 8082, ...): Each holds a subset of log data. Responds to query requests by searching its local partition and returning matching log entries.

**Scatter-Gather Pattern**: The coordinator sends the same query to all partitions concurrently, waits for responses (with configurable timeouts), and merges results using timestamp-based global ordering.

## How to Run

<!-- Fill in as development progresses -->

## API Endpoints

<!-- Fill in as development progresses -->

## What I Learned

<!-- Fill in as the project evolves -->

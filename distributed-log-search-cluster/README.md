# Distributed Log Search Cluster

A multi-node distributed search system that partitions an inverted index across nodes using consistent hashing and coordinates scatter-gather queries to return unified search results.

## Overview

Multi-process server system:
- **Coordinator service** (HTTP API) — accepts document ingestion and search queries; routes documents to the right node via consistent hashing; fans out searches to all nodes and merges results.
- **N index node services** (HTTP APIs) — each owns a partition of the inverted index and responds to local index/search calls.

All services are long-lived processes orchestrated together. Users interact only with the coordinator's REST API.

## Tech Stack
- Language: Python
- Framework: TBD (FastAPI / Flask — to be finalized)
- Storage: In-memory inverted index per node (persistence TBD)
- Partitioning: Consistent hashing
- Transport: HTTP (JSON)

## Architecture

```
        ┌────────────────┐
        │     Client     │
        └───────┬────────┘
                │ REST
        ┌───────▼────────┐
        │  Coordinator   │  consistent-hash ring
        └──┬──┬──┬──┬────┘
           │  │  │  │    scatter-gather
      ┌────▼┐ ┌▼─┐ ┌▼─┐ ┌▼──┐
      │Node0│ │N1│ │N2│ │N3 │   each holds a partition of the index
      └─────┘ └──┘ └──┘ └───┘
```

### Coordinator responsibilities
- `POST /documents` — hash doc ID, forward to owning node.
- `POST /search` — fan out to all nodes, merge + rank results, return unified response.
- Maintain the consistent-hash ring / node membership.

### Index node responsibilities
- `POST /index` — add document to local inverted index.
- `POST /search` — run query against local partition, return scored hits.
- `GET /health` — liveness.

## How to Run
_To be filled in once implementation starts._

## API (planned)

### Ingest
```
POST /documents
{ "id": "doc-1", "text": "..." }
```

### Search
```
POST /search
{ "query": "error timeout", "limit": 10 }
→ { "hits": [ { "id": "...", "score": 0.82, "snippet": "..." }, ... ] }
```

## What I Learned
_To be filled in as the project evolves._

## Status
Scaffold only — README, requirements, and .gitignore. No implementation yet.

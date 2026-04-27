# log-search-api

A production-grade RESTful API that exposes log search capabilities programmatically, with authentication, rate limiting, caching, and pagination on top of an underlying search index.

## Overview

The service is a long-lived HTTP server that fronts a search index with a hardened, programmable API. Clients authenticate, send search queries against indexed log data, and get back paginated, ranked results. A Redis-backed cache short-circuits repeat queries; a per-key rate limiter protects the index from abusive callers; pagination keeps response sizes bounded for large result sets.

The deployment unit is a Docker Compose stack: the FastAPI/Uvicorn API process, the search index, and the cache run as separate containers wired together on a private network. An optional frontend dev server can be brought up alongside for an interactive UI, but the API is the contract — anything the UI does, an external client can do too.

## Tech Stack

- Python 3.12 (slim base image)
- FastAPI + Uvicorn (HTTP layer)
- Pydantic / pydantic-settings (request/response models + config)
- JWT-based authentication (python-jose) with API key fallback
- SlowAPI (per-key / per-IP rate limiting)
- Redis (response cache + rate-limiter token bucket store)
- Search index (Elasticsearch / OpenSearch — finalized during build)
- Docker + docker compose (single source of truth for build/run/test)
- pytest + pytest-asyncio + httpx (unit + integration tests)

## Architecture

```
                  +---------------------------------------------------------+
                  |                  FastAPI app (uvicorn)                  |
                  |                                                         |
   POST /token    |  +-----------+   +-----------+   +----------------+     |
  -------------> |  | Auth      |-->| RateLimit |-->| Cache lookup   |     |
   GET  /search  |  | (JWT/API  |   | (SlowAPI  |   | (Redis, keyed  |     |
   GET  /logs/.. |  |  key)     |   |  + Redis) |   |  by query+page)|     |
                  |  +-----------+   +-----------+   +-------+--------+     |
                  |                                          |              |
                  |                                  miss    v   hit        |
                  |                            +-------------+----------+   |
                  |                            | Search client          |   |
                  |                            | (paginated query +     |   |
                  |                            |  filter pushdown)      |   |
                  |                            +-----------+------------+   |
                  |                                        |                |
                  |                              +---------v-----------+    |
                  |                              | Response shaping    |    |
                  |                              | (pagination cursor, |    |
                  |                              |  rate-limit hdrs,   |    |
                  |                              |  cache write-back)  |    |
                  |                              +---------------------+    |
                  +---------------------------------------------------------+
                                        |               |
                                        v               v
                                +---------------+  +-----------+
                                | Search index  |  |  Redis    |
                                | (ES/OpenSrch) |  |  cache +  |
                                +---------------+  |  limiter  |
                                                   +-----------+
```

## Planned API Surface

| Method | Path | Purpose |
|--------|------|---------|
| POST   | `/auth/token`            | Exchange credentials for a JWT |
| GET    | `/health`                | Liveness probe (unauthenticated) |
| GET    | `/api/v1/search`         | Paginated log search with filters |
| GET    | `/api/v1/logs/{id}`      | Fetch a single log document |
| POST   | `/api/v1/logs`           | Index a single log document |
| POST   | `/api/v1/logs/bulk`      | Index a batch of log documents |
| GET    | `/api/v1/stats`          | Index size, cache hit rate, rate-limit metrics |

Pagination uses opaque cursors (`?cursor=...&limit=...`) so deep pagination stays cheap on the underlying index. Rate limits are surfaced via standard `X-RateLimit-*` response headers.

## How to Run

The full stack lives behind Docker Compose:

```
docker compose up -d        # API + search index + cache
docker compose logs -f api  # tail API logs
docker compose down         # stop everything
```

Filled in once the implementation lands — Make targets, demo script, load test gate, and Chrome UI smoke will follow the conventions used elsewhere in this monorepo.

## Configuration

All tunables live on a `Settings` model loaded from environment / `.env` via pydantic-settings. The full table will be filled in alongside `.env.example` during implementation. Expected groups:

- HTTP: bind host/port, log level
- Auth: JWT secret, token TTL, API key list
- Rate limit: per-key rps, burst, window
- Cache: Redis URL, TTL, max key size
- Search: index host, index name, default page size, max page size

## What I Learned

To be filled in as the project evolves.

## License

Personal learning project.

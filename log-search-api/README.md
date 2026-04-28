# log-search-api

A production-style log search API: JWT-authenticated, rate-limited, Redis-cached
full-text search over an Elasticsearch index, with field weighting, fuzzy
matching, time/level/service filters, sort, offset/limit pagination, and
aggregations. Ships with a Jinja2 dashboard at the app root, OpenAPI/Swagger
docs at `/api/docs`, sample-data + load-test scripts, and an end-to-end demo
walkthrough.

The deployment unit is a Docker Compose stack: FastAPI + Uvicorn for the API,
Elasticsearch 8.15 single-node for the index, Redis 7 for the cache and
rate-limit store, and a `tester` container for `pytest`.

---

## Quick start

```bash
make build                                    # build the api image
cp .env.example .env                          # create a local env file

# Generate a real bcrypt hash for the seeded user (default: demo / demo).
# Don't forget to escape every `$` as `$$` when you paste the hash into .env.
docker compose run --rm api python scripts/seed_password.py demo

make up                                       # start api + elasticsearch + redis
make seed                                     # ingest ~5,000 synthetic log entries
make demo                                     # run ten example searches + /stats
```

When the stack is up:

- Dashboard: http://localhost:8000/
- Swagger UI: http://localhost:8000/api/docs
- ReDoc: http://localhost:8000/api/redoc
- OpenAPI JSON: http://localhost:8000/openapi.json

---

## Setting the seeded password

The single seeded user is configured by two env vars:

```
SEED_USERNAME=demo
SEED_PASSWORD_HASH=$$2b$$12$$...
```

`SEED_PASSWORD_HASH` is a **bcrypt** hash, not a plaintext password. Generate
one for an arbitrary plaintext via:

```bash
docker compose run --rm api python scripts/seed_password.py demo
# prints: $2b$12$...
```

When you paste the hash into `.env`, **escape every literal `$` as `$$`**.
docker compose interpolates `$VAR` and `${VAR}` in env files; `$$` is the
escape that becomes a single `$` at runtime. So a hash that starts with
`$2b$12$` should appear in `.env` as `$$2b$$12$$`.

---

## API endpoints

All API endpoints are mounted under `/api/v1`. The dashboard lives at `/`.

| Method | Path                          | Auth         | Description |
| ------ | ----------------------------- | ------------ | ----------- |
| GET    | `/`                           | none         | Jinja2 dashboard (single-page client) |
| GET    | `/api/docs`                   | none         | Swagger UI |
| GET    | `/api/redoc`                  | none         | ReDoc UI |
| GET    | `/openapi.json`               | none         | OpenAPI 3 spec |
| GET    | `/api/v1/health`              | none         | Liveness probe |
| GET    | `/api/v1/health/detailed`     | none         | ES + Redis dependency probes |
| POST   | `/api/v1/auth/token`          | none (form)  | Exchange username + password for JWT |
| GET    | `/api/v1/auth/me`             | bearer       | Echo the current authenticated subject |
| POST   | `/api/v1/logs`                | bearer       | Ingest a single log entry |
| POST   | `/api/v1/logs/bulk`           | bearer       | Bulk-ingest up to 1000 entries |
| GET    | `/api/v1/logs/{id}`           | bearer       | Fetch a log entry by id |
| GET    | `/api/v1/logs/search`         | bearer       | Search via query-string parameters |
| POST   | `/api/v1/logs/search`         | bearer       | Search via JSON body |
| GET    | `/api/v1/stats`               | bearer       | Cache + index counters |

Rate limit defaults to 100 req/min per authenticated user (or per IP if
unauthenticated). Health endpoints are exempt.

---

## Auth

Get a token (form-encoded body, OAuth2 password flow):

```bash
curl -sS -X POST http://localhost:8000/api/v1/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'username=demo' \
  --data-urlencode 'password=demo'
# {"access_token":"eyJ...","token_type":"bearer","expires_at":"..."}
```

Use the token as a bearer header on every other call:

```bash
TOKEN="$(curl -sS -X POST http://localhost:8000/api/v1/auth/token \
  --data-urlencode 'username=demo' --data-urlencode 'password=demo' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"

curl -sS -H "Authorization: Bearer ${TOKEN}" http://localhost:8000/api/v1/auth/me
# {"username":"demo"}
```

Tokens are HS256 signed with `SECRET_KEY` and live for `ACCESS_TOKEN_TTL_MINUTES`
(default 15). There are no refresh tokens — re-issue from `/auth/token`.

---

## Search examples

### GET (query-string)

```bash
curl -sS -H "Authorization: Bearer ${TOKEN}" \
  'http://localhost:8000/api/v1/logs/search?q=payment%20error&levels=ERROR&limit=10&offset=0&sort_by=timestamp&sort_order=desc'
```

### POST (JSON body)

```bash
curl -sS -X POST http://localhost:8000/api/v1/logs/search \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "payment error",
    "levels": ["ERROR", "CRITICAL"],
    "services": ["payment-service"],
    "start_time": "2025-01-01T00:00:00Z",
    "end_time":   "2026-12-31T23:59:59Z",
    "limit": 25,
    "offset": 0,
    "sort_by": "relevance",
    "sort_order": "desc",
    "include_content": true
  }'
```

### Search parameters

| Parameter         | Type            | Default      | Description |
| ----------------- | --------------- | ------------ | ----------- |
| `q`               | string ≤512     | (empty)      | Full-text query (multi_match best_fields, fuzziness=AUTO) |
| `levels`          | array<string>   | (any)        | One or more of DEBUG/INFO/WARN/ERROR/CRITICAL |
| `services`        | array<string>   | (any)        | Service-name filter (exact match) |
| `start_time`      | ISO 8601        | (none)       | Inclusive lower bound on `timestamp` |
| `end_time`        | ISO 8601        | (none)       | Inclusive upper bound on `timestamp` |
| `limit`           | int 1..1000     | 100          | Page size |
| `offset`          | int ≥0          | 0            | Pagination offset |
| `sort_by`         | enum            | relevance    | `relevance` or `timestamp` |
| `sort_order`      | enum            | desc         | `asc` or `desc` |
| `include_content` | bool            | true         | Include the dynamic `content` object in results |

### Sample response

```json
{
  "query": "payment error",
  "total_hits": 42,
  "execution_time_ms": 18.4,
  "cache_hit": false,
  "results": [
    {
      "id": "seed-3a8f...",
      "timestamp": "2026-04-25T10:14:22.000+00:00",
      "level": "ERROR",
      "service_name": "payment-service",
      "message": "Payment failed for order ord-12345",
      "content": {"amount": 199.99, "currency": "USD", "latency_ms": 842},
      "score": 8.213
    }
  ],
  "pagination": {"offset": 0, "limit": 25, "has_more": true},
  "aggregations": {
    "levels":   [{"key": "ERROR", "doc_count": 27}, {"key": "WARN", "doc_count": 15}],
    "services": [{"key": "payment-service", "doc_count": 42}],
    "timeline": [{"key_as_string": "2026-04-25T10:00:00.000Z", "doc_count": 18}]
  }
}
```

---

## Dashboard

Visit http://localhost:8000/ after `make up`. Features:

- Login form (issues a JWT against `/api/v1/auth/token`, stores it in
  `localStorage`).
- Search form: query text, multi-select levels, dynamic multi-select services
  (populated from the most recent search's aggregations), date-time range,
  sort by/order, limit + offset.
- Results table with timestamp / level / service / message / score plus
  pagination prev/next.
- Live badges for `total_hits`, `execution_time_ms`, and `cache_hit`.
- Aggregations panel: small stacked bars for level counts, service counts,
  and the timeline.
- Toasts surface API errors (`error.message` + `request_id` from the envelope)
  and rate-limit responses.

---

## OpenAPI docs

- Swagger UI: http://localhost:8000/api/docs
- ReDoc: http://localhost:8000/api/redoc
- Raw JSON spec: http://localhost:8000/openapi.json

---

## Stats endpoint

```bash
curl -sS -H "Authorization: Bearer ${TOKEN}" http://localhost:8000/api/v1/stats
```

```json
{
  "cache": {"hits": 18, "misses": 32, "errors": 0, "hit_rate": 0.36},
  "index": {"index": "logs", "doc_count": 5000, "size_in_bytes": 1234567},
  "timestamp": "2026-04-26T15:00:00+00:00"
}
```

---

## Configuration

All settings load from environment / `.env` via `pydantic-settings`. See
`.env.example` for the canonical form.

| Variable                     | Default                                            | Description |
| ---------------------------- | -------------------------------------------------- | ----------- |
| `API_HOST`                   | `0.0.0.0`                                          | Uvicorn bind host |
| `API_PORT`                   | `8000`                                             | Uvicorn bind port |
| `PROJECT_NAME`               | `Log Search API`                                   | OpenAPI / dashboard title |
| `API_V1_PREFIX`              | `/api/v1`                                          | Prefix for all v1 endpoints |
| `SECRET_KEY`                 | (required)                                         | JWT HMAC secret (≥ 8 chars; ≥ 32 in prod) |
| `JWT_ALGORITHM`              | `HS256`                                            | JWT signing algorithm |
| `ACCESS_TOKEN_TTL_MINUTES`   | `15`                                               | Access token lifetime |
| `RATE_LIMIT_REQUESTS`        | `100`                                              | Requests per window |
| `RATE_LIMIT_WINDOW_SECONDS`  | `60`                                               | Window length |
| `REDIS_URL`                  | `redis://redis:6379`                               | Base Redis URL |
| `CACHE_REDIS_DB`             | `0`                                                | Redis DB for the search cache |
| `RATE_LIMIT_REDIS_DB`        | `1`                                                | Redis DB for slowapi |
| `ELASTICSEARCH_URL`          | `http://elasticsearch:9200`                        | ES base URL |
| `ELASTICSEARCH_INDEX`        | `logs`                                             | Target index name |
| `SEARCH_CACHE_TTL_SECONDS`   | `300`                                              | Cache TTL per search request |
| `DEFAULT_SEARCH_LIMIT`       | `100`                                              | Default page size |
| `MAX_SEARCH_LIMIT`           | `1000`                                             | Max page size enforced by validation |
| `DEFAULT_SEARCH_OFFSET`      | `0`                                                | Default pagination offset |
| `DEFAULT_SORT_BY`            | `relevance`                                        | Default sort key |
| `DEFAULT_SORT_ORDER`         | `desc`                                             | Default sort order |
| `DEFAULT_INCLUDE_CONTENT`    | `true`                                             | Include dynamic content by default |
| `CORS_ALLOWED_ORIGINS`       | `http://localhost:3000,http://localhost:8000`      | Comma-separated origin list |
| `SEED_USERNAME`              | `demo`                                             | Seeded user's username |
| `SEED_PASSWORD_HASH`         | (required)                                         | bcrypt hash; escape `$` as `$$` in `.env` |
| `LOG_LEVEL`                  | `INFO`                                             | Python logging level |
| `DEBUG_ENDPOINTS_ENABLED`    | `false`                                            | Mount the `/_debug/*` test routes |
| `LOAD_TEST_ENABLED`          | (unset)                                            | Set to non-empty to enable the load smoke test |

---

## Operations

### Make targets

| Target              | What it does |
| ------------------- | ------------ |
| `make build`        | Build the api image |
| `make build-test`   | Build the tester image |
| `make up`           | `docker compose up -d --wait` (waits on healthchecks) |
| `make down`         | Stop the stack |
| `make down-v`       | Stop the stack and drop named volumes (wipes ES data) |
| `make logs`         | Tail api logs |
| `make test`         | Run the full pytest suite inside the tester container |
| `make test-unit`    | Run only `tests/unit/` |
| `make test-integration` | Run only `tests/integration/` |
| `make lint`         | Byte-compile the codebase as a smoke check |
| `make seed`         | Ingest ~5,000 synthetic log entries |
| `make demo`         | End-to-end demo: stack + seed + 10 searches + /stats |
| `make load-test`    | Run the full 500-request load test (asserts p95 SLOs) |

### docker compose

```bash
docker compose up -d --wait
docker compose ps
docker compose logs -f api
docker compose run --rm api python scripts/seed_data.py
docker compose down
docker compose down -v   # also drops the ES volume
```

---

## Performance SLOs

The load-test gate (`make load-test`, `scripts/load_test.py`) enforces:

- **p95 uncached < 500 ms** (500 distinct queries, semaphore 50)
- **p95 cached  < 100 ms** (500 identical queries against a primed cache)
- 50+ concurrent connections sustained without errors
- Non-zero exit on SLO breach or any failed request

A lighter integration smoke (`tests/integration/test_load.py`, opt-in via
`LOAD_TEST_ENABLED=1`) runs 50 reqs at concurrency 10 against the same SLOs
so the regular test suite stays fast.

---

## Architecture

```
                 +--------------------------------------------------+
   browser  -->  |   FastAPI app (uvicorn, ORJSONResponse)          |
   curl     -->  |                                                  |
                 |  +----------+   +-----------+   +-------------+  |
                 |  | request- |-->| CORS +    |-->| slowapi     |  |
                 |  | id       |   | error     |   | per-user/ip |  |
                 |  | header   |   | envelope  |   | 100/min     |  |
                 |  +----------+   +-----------+   +------+------+  |
                 |                                        |         |
                 |             +---------+   +------------v------+  |
                 |  POST /token| auth    |   | bearer dependency |  |
                 |  ---------->| (PyJWT  |   | (RequireUser)     |  |
                 |             | HS256)  |   +-----+-------------+  |
                 |             +---------+         |                |
                 |                                 v                |
                 |  +-------------+   +------------+----------+     |
                 |  | search/api  |-->| SearchService          |     |
                 |  | (GET/POST)  |   | + Redis cache (5m TTL) |     |
                 |  +-------------+   +-----------+------------+     |
                 |                                |                  |
                 |                                v                  |
                 |                       +--------+---------+        |
                 |                       | ES query builder |        |
                 |                       | multi_match +    |        |
                 |                       | filters + aggs   |        |
                 |                       +--------+---------+        |
                 +----------------------------------|----------------+
                                                    |
                            +-----------------------+----+
                            |                            |
                       +----v----+                  +----v----+
                       | Redis 7 |                  | ES 8.15 |
                       | DB0=cache|                 | single   |
                       | DB1=rate |                 | node     |
                       +---------+                  +---------+
```

Key building blocks:

- **FastAPI** + Uvicorn, async-only, `ORJSONResponse` as the default response
  class for fast cache-hit serialisation.
- **Elasticsearch 8.15** with `multi_match best_fields`, fields
  `message^3, content.*^1, service_name^2`, `tie_breaker=0.3`, `fuzziness=AUTO`,
  plus terms aggs (`level`, `service_name`) and a 1-hour `date_histogram`.
- **Redis** (one container, two logical DBs): cache=DB0, rate-limit=DB1, with
  persistence disabled (`--save "" --appendonly no`).
- **JWT auth** via PyJWT (HS256), single seeded user from env, 15-minute access
  TTL, no refresh tokens.
- **slowapi** rate limiter keyed on JWT subject (or remote IP if unauth);
  storage is the Redis URL string.

---

## Testing

```bash
# Unit tests (no live services required)
make test-unit

# Integration tests against the live compose stack
make test-integration

# Full suite
make test
```

Integration tests skip if `API_URL` is not set; the tester container always
sets it via the `.env` file (it inherits `API_URL=http://api:8000` from the
compose service definition).

The load smoke (`tests/integration/test_load.py`) is opt-in:

```bash
docker compose run --rm -e LOAD_TEST_ENABLED=1 tester pytest tests/integration/test_load.py -v
```

The full load test (`make load-test`) runs `scripts/load_test.py` directly in
the api container (500 reqs, semaphore 50, asserts p95 SLOs).

---

## Known limitations

- **Single seeded user** baked into env. No user CRUD; rotate the password by
  re-running `scripts/seed_password.py` and bouncing the api container.
- **HS256 secret** lives in `SECRET_KEY` and is rotated manually; old tokens
  remain valid until they expire (no token revocation list).
- **Single-node Elasticsearch** with `xpack.security.enabled=false` — fine for
  local dev and learning, **not** a production-ready cluster.
- **No refresh tokens** for v1; the access token is the only credential the
  API issues.
- **Cache invalidation is TTL-based** (`SEARCH_CACHE_TTL_SECONDS=300`). Newly
  ingested logs become searchable within 5s (ES `refresh_interval`) but cached
  responses persist until TTL.
- **Bind-mount perf on macOS**: `./src` is bind-mounted into the api container
  for hot reload; ES + Redis use named volumes (bind-mounted ES is 5-10× slower
  on virtiofs).

---

## License

Personal learning project.

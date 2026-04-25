# log-fulltext-search-rerank

A natural-language log search service that ranks results with TF-IDF plus a multi-factor reranker (recency, severity, service authority, context mode).

## Overview

The service ingests log entries over HTTP, indexes them in a hand-rolled in-process inverted index, and answers natural-language queries with a top-K list of relevance-ranked entries. Each result carries an explanation breaking the score down across its weighted factors, so the ranking is auditable end-to-end.

The pipeline is parser -> retriever -> reranker -> cache. A regex-and-NLTK tokenizer preserves compound patterns (IPs, UUIDs, dotted identifiers). A small intent detector and synonym expander widen the recall without leaking unrelated tokens. The retriever unions posting lists, caps the candidate set at 200, and hands the cohort to a multi-factor reranker that mixes TF-IDF with temporal decay, severity boosts, service authority, and a context-mode override (e.g. `incident` mode shortens the half-life and amplifies severity).

The HTTP surface is FastAPI. A Jinja2 + vanilla-JS dashboard ships in the same Python process - no Node toolchain. Everything (build, unit tests, E2E, load tests) runs inside Docker.

## Tech Stack

- Python 3.12 (slim base image)
- FastAPI + Uvicorn (HTTP layer)
- Pydantic / pydantic-settings (models + config)
- NLTK 3.9.1 (tokenization, stopwords, WordNet lemmatization) - corpora baked into the image
- Hand-rolled TF-IDF, prefix trie, LRU query cache (no external search engine)
- Jinja2 + vanilla JS (dashboard)
- Docker + docker compose (single source of truth for build/run/test)
- pytest + pytest-asyncio + httpx (185 unit tests)

## Architecture

```
                 +------------------------------------------------------+
                 |            FastAPI app (uvicorn, 1 worker)           |
                 |                                                      |
  POST /api/logs |  +----------+     +----------+     +--------------+  |
 --------------> |  |Ingest    |---->|Tokenizer |---->|InvertedIndex |  |
  POST /bulk     |  |Router    |     |(NLTK +   |     | postings:    |  |
                 |  +----------+     | regex)   |     | {tok:{id:tf}}|  |
                 |                   +----------+     | +PrefixTrie  |  |
                 |                                    +------+-------+  |
  POST /search   |  +----------+     +----------+            |          |
 --------------> |  |Query     |---->|Retriever |------------+          |
                 |  |Parser    |     |top-K     |   +--------------+    |
                 |  |intent +  |     |cand.     |-->|Reranker      |    |
                 |  |synonyms  |     |(heapq)   |   |tfidf+temporal|    |
                 |  +----------+     +----------+   |+severity+svc |    |
                 |                                  |+context mode |    |
  GET /suggest   |  +----------+                    +------+-------+    |
 --------------> |  |Trie      |                           |            |
                 |  |prefix    |                           v            |
                 |  +----------+                  +--------------+      |
                 |                                |Explain+format|      |
  GET /stats     |                                |SearchResponse|      |
 --------------> |  IndexStats/Cache stats ------>+--------------+      |
                 |                                                      |
  GET /          |  Jinja2 dashboard + static/ (app.js, app.css)        |
 --------------> |                                                      |
                 |  LRU QueryCache(norm_query, mode, index_version)     |
                 +------------------------------------------------------+
                                       |
                                       v
                    docker compose: app (port 8000) + test profile
```

## API Surface

| Method | Path | Purpose |
|--------|------|---------|
| GET    | `/health`                          | Liveness probe |
| POST   | `/api/logs`                        | Ingest a single `LogEntry` |
| POST   | `/api/logs/bulk`                   | Ingest a batch (max 10000) |
| POST   | `/api/search`                      | Natural-language ranked search |
| GET    | `/api/search/suggestions?q=&limit=`| Prefix autocomplete |
| GET    | `/api/search/stats`                | Index + cache + latency stats |
| POST   | `/api/sample/seed?count=`          | Seed synthetic log entries (dashboard helper) |
| GET    | `/`                                | Jinja2 dashboard |
| GET    | `/static/...`                      | Dashboard CSS/JS |

### Sample request/response — `POST /api/search`

Request:

```json
{
  "query": "authentication error",
  "limit": 5,
  "context": {"mode": "incident"}
}
```

Response (truncated):

```json
{
  "query": "authentication error",
  "intent": "troubleshooting",
  "expanded_terms": ["authentication", "auth", "error", "failure"],
  "results": [
    {
      "log_entry": "auth: token validation failed for user 42",
      "timestamp": 1714060800.123,
      "service": "auth",
      "level": "ERROR",
      "score": 0.873,
      "ranking_explanation": {
        "tfidf": 0.62,
        "temporal": 0.95,
        "severity": 1.0,
        "service": 0.9,
        "context": 0.4,
        "reasons": ["incident_mode_boost", "severity:ERROR"]
      }
    }
  ],
  "total_hits": 18,
  "ranked_hits": 5,
  "execution_time_ms": 0.42
}
```

## How to Run

Every command runs inside Docker. The host machine never executes app or test code directly.

```
make build          # build app + test images
make up             # start the app (docker compose up -d)
make demo           # full scripted walkthrough of every endpoint
make load           # performance gate (asserts the SLOs below)
make test           # full unit test suite (in Docker)
make e2e            # demo as e2e (start, run, stop)
make logs           # tail app logs
make ui             # open dashboard in browser
make down           # stop everything
make clean          # nuke volumes and local images
```

### Try it (curl)

```bash
# Seed synthetic data
curl -fsS -XPOST 'http://localhost:8000/api/sample/seed?count=500'

# Natural-language search
curl -fsS -XPOST http://localhost:8000/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"authentication error","limit":5}'

# Incident-mode search (severity-weighted)
curl -fsS -XPOST http://localhost:8000/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"error","limit":10,"context":{"mode":"incident"}}'

# Autocomplete
curl -fsS 'http://localhost:8000/api/search/suggestions?q=auth&limit=5'

# Stats
curl -fsS http://localhost:8000/api/search/stats
```

## Configuration

All tunables live on `Settings` (`src/config.py`) and are loaded from environment / `.env` via pydantic-settings. Defaults match `project_requirements.md` so a clean environment yields the documented behaviour.

| Setting | Default | Purpose |
|---------|---------|---------|
| `HTTP_HOST`                       | `0.0.0.0` | Server bind address |
| `HTTP_PORT`                       | `8000`    | Server port |
| `LOG_LEVEL`                       | `INFO`    | Uvicorn / app log verbosity |
| `MIN_TOKEN_LENGTH`                | `3`       | Drop corpus tokens shorter than this (queries keep shorter tokens) |
| `STOP_WORDS_LANGUAGE`             | `english` | NLTK stopword list to strip |
| `TEMPORAL_HALF_LIFE_NORMAL_S`     | `21600`   | Recency half-life in seconds (6h) for the default mode |
| `TEMPORAL_HALF_LIFE_INCIDENT_S`   | `900`     | Recency half-life (15min) when `mode=incident` |
| `DEFAULT_LIMIT`                   | `10`      | Default `limit` if the request omits it |
| `QUERY_CACHE_SIZE`                | `1000`    | LRU capacity of the in-process query cache |
| `CANDIDATE_TOP_K`                 | `200`     | Cap on candidates handed to the reranker |
| `IDF_REBUILD_EVERY_N_DOCS`        | `500`     | IDF cache rebuilds after this many new docs |
| `IDF_REBUILD_EVERY_S`             | `2.0`     | IDF cache rebuilds at most this often (seconds) |
| `SYNONYMS_PATH`                   | unset     | Override path to synonyms JSON |
| `INTENT_PATTERNS_PATH`            | unset     | Override path to intent regex JSON |

Dict-valued settings (`severity_weights`, `ranking_weights`, `incident_ranking_weights`, `service_authority_weights`) live on `Settings` defaults and are not flat-env overrideable; edit `src/config.py` or pass a custom `Settings` to `build_app()` to tune them.

## Performance

`scripts/load_test.py` (run via `make load`) is a hard gate. Observed numbers on a recent run versus the SLO targets:

| Metric                  | SLO          | Observed |
|-------------------------|--------------|----------|
| Search p95 latency      | < 100 ms     | ~0.5 ms  |
| Search throughput       | > 50 QPS     | ~1700 QPS|
| Per-doc index update    | < 10 ms      | < 10 ms (cold ingest) |
| App RSS                 | < 200 MB     | well under 200 MB at 10k docs |

`make load` exits non-zero if any threshold regresses, so the gate is a real CI-style guard rather than a manual check.

## Testing

Tests are layered and all run inside Docker:

- **Unit (185 tests)** — `make test`. Tokenizer, index, trie, parser, intent, synonyms, ranking primitives, reranker, query cache, service facade, every API route. `pytest -v --tb=short` inside the `test` profile.
- **E2E** — `make e2e`. `scripts/demo.py` walks `/health` -> ingest -> search -> incident-mode search -> suggestions -> stats against a live container.
- **Performance** — `make load`. `scripts/load_test.py` runs a warm-up pass then 200 sampled queries and asserts the SLOs in the table above.
- **Chrome UI smoke** — main thread runs Chrome MCP against `http://localhost:8000/`, types a query, asserts a result card with a numeric score is rendered (per the chrome-ui-testing skill).

The host never installs requirements; `pytest` only runs through `docker compose --profile test`.

## Project Layout

```
log-fulltext-search-rerank/
|-- Dockerfile / Dockerfile.test
|-- docker-compose.yml / Makefile / start.sh / stop.sh
|-- requirements.txt / pytest.ini / .env.example
|-- src/
|   |-- main.py            # build_app, lifespan, module-level app
|   |-- config.py          # Settings (pydantic-settings)
|   |-- service.py         # SearchService facade
|   |-- models.py / sample_data.py / logging_setup.py
|   |-- api/               # routes_logs / routes_search / routes_health / routes_dashboard
|   |-- index/             # tokenizer, inverted_index, trie, stats
|   |-- query/             # parser, intent, synonyms (+ JSON defaults)
|   |-- ranking/           # tfidf, temporal, severity, service_authority, context, reranker, explain
|   `-- cache/             # query_cache (LRU)
|-- templates/dashboard.html
|-- static/app.css, app.js
|-- scripts/demo.py, load_test.py
`-- tests/                 # 185 tests, mirrors src/ layout
```

## What I Learned

- A hand-rolled TF-IDF (`log((N+1)/(df+1)) + 1` with a lazy-rebuilt `idf_cache`) beats `TfidfVectorizer` for streaming corpora — sklearn's API is batch-only, so streaming ingest plus on-demand search needs an incremental scorer.
- Append-only postings plus a single writer-side `asyncio.Lock` gives effectively lock-free reads. Old posting dicts are never mutated, so concurrent searches just hold a stale-but-consistent view.
- Trie autocomplete is cheap if you lazy-rebuild on `index.version` changes instead of mutating per ingest. Steady-state suggest calls then never touch the index at all.
- The query cache key has to include `index_version` (and the mode + limit). Without the version, stale entries leak across ingests; with it, "stale" entries are simply never hit.
- Running the rescoring loop inside `asyncio.to_thread` keeps the event loop responsive when the reranker fans out across 200 candidates per query.
- Bake NLTK corpora into the runtime image at build time. Calling `nltk.download` at request time is the canonical foot-gun (offline builds break, p95 spikes on first hit).
- Mode-driven ranking weights and per-mode half-life turn one ranker into a small family — `incident` mode raises severity from 0.15 to 0.25 and shrinks the half-life from 6h to 15min, which dramatically reorders the same candidate set.
- Jinja2 + vanilla JS is enough for an interactive dashboard. The stack stays pure-Python, the build is one Docker image, and the UX still feels SPA-ish because all the dynamic bits hit JSON endpoints.

## License

Personal learning project.

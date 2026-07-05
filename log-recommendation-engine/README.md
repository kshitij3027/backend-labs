# Log Recommendation Engine

A recommendation system that matches a **new incident** against a **historical incident database** using **semantic + contextual similarity**, then surfaces a **ranked list of solution suggestions**. Every suggestion carries a similarity score and the resolution that fixed the matched historical incident. The engine **improves over time via feedback** — engineers mark which suggestions were helpful, and those signals re-rank future results. Exposed through a **FastAPI REST API** and a **separate interactive web dashboard**, running as two long-lived processes.

---

## What It Does

The engine turns an incoming incident description into ranked, actionable fixes:

1. **Ingest history** — resolved incidents (`title`, `description`, `service`, `severity`, `tags`, `resolution`) are stored as the historical knowledge base.
2. **Embed** — each incident's text is encoded into a dense semantic vector with a sentence-transformer model, capturing meaning rather than exact keyword overlap.
3. **Match** — a new incident is embedded and compared against the history:
   - **Semantic similarity** — cosine similarity between embeddings finds incidents that *mean* the same thing even when worded differently.
   - **Contextual similarity** — structured signals (matching `service`, `severity`, overlapping `tags`, recency) boost or dampen candidates so the ranking respects operational context, not just prose.
4. **Rank** — semantic and contextual scores are blended into a single relevance score; the top-`k` historical incidents and their resolutions are returned as **ranked solution suggestions**.
5. **Feedback loop** — engineers submit feedback (`helpful` / `not helpful`) on each suggestion. Feedback is aggregated per `(query pattern → suggestion)` pair and folded back into the ranking so proven fixes rise and unhelpful matches sink over time.
6. **Serve** — the REST API answers recommendation queries, accepts feedback, and manages the incident corpus; the dashboard lets a human paste an incident, view ranked suggestions with scores, and vote on them.

---

## Architecture

Two long-lived processes, kept separate so the UI and the API scale and deploy independently:

| Process | Role |
|---|---|
| **Recommendation API** | FastAPI/Uvicorn REST service. Embeds incidents, computes semantic + contextual similarity, serves ranked suggestions, ingests historical incidents, and records feedback. |
| **Web Dashboard** | A separate interactive web UI process. Lets an engineer submit a new incident, browse ranked suggestions and their scores, and mark suggestions helpful/unhelpful — feeding the improvement loop. |

```
                                   feedback (helpful / not helpful)
                                   ┌───────────────────────────────┐
                                   ▼                               │
new incident ─► embed ─► semantic similarity ─┐                    │
 (title, desc,          (cosine over          ├─► blended rank ─► ranked suggestions ─► dashboard
  service, tags)         embeddings)          │   (top-k)          (score + resolution)
                                              │
        historical corpus ─► contextual signals┘
        (resolved incidents)  (service, severity,
                               tag overlap, recency)
```

---

## Tech Stack

- **Language:** Python 3.12
- **API:** FastAPI + Uvicorn, Pydantic / pydantic-settings
- **Semantic similarity:** sentence-transformers (`all-MiniLM-L6-v2`, 384-dim, L2-normalized → cosine), NumPy / scikit-learn
- **Vector store & retrieval:** **PostgreSQL 16 + pgvector** (`vector(384)` column, HNSW `vector_cosine_ops` K-NN with metadata pre-filtering)
- **Contextual similarity:** structured feature scoring over service / severity / tags (Jaccard) / recency (half-life decay)
- **Persistence:** PostgreSQL (incident corpus, embeddings, served recommendations, feedback) via SQLAlchemy + Alembic migrations
- **Cache / shared state:** Redis (embedding cache, recommendation cache, feedback + config epoch counters)
- **Dashboard:** separate React + Vite + Recharts SPA, served by nginx (reverse-proxies `/api` → API)
- **Observability:** structlog structured logs, Prometheus metrics

> Exact dependency versions are pinned in [`requirements.txt`](requirements.txt).

---

## How to Run

Everything runs in Docker via the `Makefile` — no local Python needed. The only prerequisite is **Docker** (with Compose v2). The MiniLM model is baked into the image, so there are no network calls at runtime.

```bash
# (optional) start from the committed env template — sane defaults work out of the box
cp .env.example .env

# 1) bring up the API + Postgres(pgvector) + Redis (detached)
make up            # API at http://localhost:8000  (GET /health)

# 2) ingest a synthetic historical-incident corpus (each row is embedded on ingest)
make seed          # or: make seed ARGS="--count 200"
make backfill      # embed any rows left NULL-embedded (no-op on a fresh seed)

# 3) bring up the interactive dashboard too (adds the React SPA behind nginx)
make ui            # Dashboard at http://localhost:8080 , API still at :8000
```

### Testing & verification

```bash
make test          # full pytest suite in Docker (unit + integration; rebuilds first)
make e2e           # black-box loop verifier: seed → recommend → feedback → recommend → assert the re-rank shift
make load          # perf gate (p95 recommend latency) + concurrent throughput/error-rate gate
make scale N=3     # run 3 API replicas load-balanced behind the dashboard's nginx
```

`make e2e` and `make load` are **hard-gated** — a blown p95, throughput, error-rate, or re-rank assertion fails the target (exit code propagates).

### Port conflicts

If host ports **8000 / 8080** are already taken, override them on any target:

```bash
API_PORT=8010 DASHBOARD_PORT=8081 make ui
# then the API is at :8010 and the dashboard at :8081
# for a scaled fleet: DASHBOARD_PORT=8081 make scale N=3  → hit :8081/api/health
```

### Quick try (curl)

Submit an unresolved incident and get ranked suggestions, each with the resolution that fixed the matched historical incident plus a per-signal score breakdown:

```bash
curl -s http://localhost:8000/recommend \
  -H 'Content-Type: application/json' \
  -d '{
        "title": "API latency spike after deploy",
        "description": "p99 latency jumped and the pods are getting OOM-killed under load",
        "service": "checkout-api",
        "severity": "high",
        "tags": ["latency", "oom", "kubernetes"]
      }'
```

Sample shape of the response (abridged):

```json
{
  "recommendation_id": 42,
  "count": 3,
  "cached": false,
  "suggestions": [
    {
      "incident_id": 17,
      "title": "Checkout pods OOM-killed under peak load",
      "resolution": "Raised the memory limit and added an HPA on memory; latency recovered.",
      "score": 0.83,
      "breakdown": { "semantic": 0.79, "contextual": 0.91, "feedback": 0.0 }
    }
  ]
}
```

The returned `recommendation_id` is what you send back to `POST /feedback` (`{"recommendation_id": 42, "incident_id": 17, "helpful": true}`) — proven fixes rise and unhelpful matches sink on the next query.

---

## API

Interactive docs are served at `http://localhost:8000/docs` (OpenAPI/Swagger).

| Method | Endpoint | Purpose |
|---|---|---|
| `POST`   | `/incidents` | Add one resolved incident to the corpus (embedded on ingest, immediately searchable) |
| `GET`    | `/incidents` | List / search the corpus — filters: `q` (title+description substring), `tags` (repeatable, overlap), `service`, `severity`, plus `limit` / `offset` |
| `GET`    | `/incidents/{id}` | Fetch one incident (`404` if absent) |
| `PUT`    | `/incidents/{id}` | Partial-update one incident (re-embeds when a text field changes) |
| `DELETE` | `/incidents/{id}` | Delete one incident and its dependent feedback rows (`204`) |
| `POST`   | `/recommend` | Submit a new incident, get the top-`k` ranked suggestions with resolutions + score breakdown |
| `POST`   | `/feedback` | Record a helpful / not-helpful vote on a suggestion from a prior recommendation |
| `GET`    | `/stats` | Corpus + feedback rollup (sizes, counts by service/severity, votes, busiest query patterns) |
| `GET`    | `/health` | Deep liveness + per-component readiness (database / redis / embedding model); always `200` while alive |
| `GET`    | `/config` | Current effective runtime ranking config + version |
| `PUT`    | `/config` | Update runtime-tunable ranking knobs — takes effect on the next request, no restart |
| `GET`    | `/metrics` | Prometheus text exposition |
| `GET`    | `/metrics/json` | Key counters/gauges as a JSON snapshot (for the dashboard) |

Every corpus mutation (`POST` / `PUT` / `DELETE /incidents`) and every `PUT /config` bumps a Redis **epoch** that is folded into the recommendation cache key, so cached results are invalidated and the next `/recommend` recomputes against the change.

---

## Configuration

All ranking behaviour is tunable without code changes. Defaults live in [`config/config.yaml`](config/config.yaml); any field can be overridden by an environment variable (the uppercased field name — e.g. `WEIGHT_SEMANTIC`, `TOP_K`), and a subset of ranking knobs can be retuned **live at runtime** via `PUT /config` (shared across every replica through Redis). See [`.env.example`](.env.example) for the common knobs. Highlights:

- **Blend weights** — `weight_semantic` (0.6), `weight_contextual` (0.4), `weight_feedback` (0.2).
- **Contextual signal weights** — `ctx_weight_service` / `ctx_weight_severity` / `ctx_weight_tags` / `ctx_weight_recency`, plus `recency_half_life_days` (30).
- **Retrieval** — `top_k` (5) returned, `candidate_k` (50) K-NN pool before re-ranking.
- **Feedback loop / exploration** — `feedback_smoothing` (Laplace, 2.0), `epsilon_explore` (0.1), `diversity_threshold` (0.9).
- **Confidence tiers** — `high_confidence_threshold` (0.75) / `medium_confidence_threshold` (0.5).

---

## What I Learned

- **Embedding-based retrieval** with `all-MiniLM-L6-v2`: encode each incident's canonical document text into a 384-dim dense vector and L2-normalize it, so a plain inner product *is* cosine similarity — incidents that *mean* the same thing match even when worded differently, unlike keyword search.
- **pgvector as the hybrid search engine**: store embeddings in a `vector(384)` column and do K-NN with an **HNSW index (`vector_cosine_ops`)**. The interesting part is layering signals correctly — metadata (`service` / `severity`) as *hard constraints* only when the caller opts in (`restrict_*`), while everything else stays a *soft preference* so the search never starves for candidates.
- **Blending semantic + contextual into one relevance score**: cosine similarity alone ignores operational context, so I fold in structured signals — exact service match, severity ordinal proximity, **tag Jaccard overlap**, and a **recency half-life decay** — each weighted, then combined with the semantic score into a single ranking. Ops context matters as much as prose.
- **Closing the feedback loop**: helpful/unhelpful votes are bucketed per `(query pattern → incident)` and turned into a **Laplace-smoothed net-helpfulness** boost (smoothing keeps one lucky upvote from dominating). The subtle bug was making votes actually *take effect* — solved with **epoch-based cache invalidation** so a vote bumps a Redis counter that invalidates cached recommendations.
- **Fighting the popularity feedback loop**: naively re-ranking by feedback makes popular fixes win forever and never surfaces newer/better ones. **ε-exploration** occasionally promotes a strong-but-unproven candidate, and a **diversity threshold** drops near-duplicate resolutions — so the system keeps learning instead of ossifying.
- **Zero-container load balancing**: horizontal API scaling (`make scale N=3`) needs no dedicated LB — the dashboard's **nginx re-resolves the `api` service name per request via Docker DNS** and round-robins across replicas. `GET /health` returns each replica's hostname, which makes the round-robin observable.
- **Graceful degradation**: a pgvector/DB outage surfaces as a clean `503` (not a raw 500), an *empty* corpus is a normal `200` with `count=0` (not an error), and Redis being down simply bypasses the cache. `/health` stays `200` while the process is alive and reports degradation in the body, so the container healthcheck tracks liveness while a dashboard still sees readiness detail.

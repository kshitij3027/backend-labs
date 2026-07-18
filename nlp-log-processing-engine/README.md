# NLP Log Processing Engine

A semantic **log-understanding** service that turns each free-text log line into structured signal. One `POST /api/analyze` call runs four NLP capabilities over a message — **named-entity recognition** (services, hosts, IPs, user ids, error codes, paths, URLs, ports), **intent classification** (what the line is *about*), **sentiment / severity** (how alarming it is), and **keyword extraction** (the salient phrases) — then folds the result into rolling dashboard aggregates and pushes it live to connected clients over WebSocket. It runs as two long-lived Docker services (a FastAPI backend, an nginx-served React SPA), keeps **all state in-memory** (no database, no Redis, no queue), loads every model **once at startup**, and is verified end-to-end by a black-box harness and a hard-gated load test.

---

## What It Does

Raw log lines are noisy, unstructured free text: `auth-svc rejected login for user 4821 from 10.0.0.1: invalid token` carries a service, a user id, an IP, an operational purpose, a severity, and a couple of searchable phrases — none of it machine-readable. This engine extracts all of that in one pass:

| Capability | Technique | Produces |
|---|---|---|
| **Entity recognition** | spaCy `en_core_web_sm` NER + an `EntityRuler` gazetteer + a custom regex pipeline component | `[{text, label, start, end}]` over `SERVICE · HOST · IP · USER_ID · ERROR_CODE · PATH · URL · PORT` (plus general spaCy entities) |
| **Intent classification** | TF-IDF (1,2-grams) → `LogisticRegression`, trained on a synthetic labeled corpus | one of 8 intents (or an `other` reject bucket) + a confidence |
| **Sentiment / severity** | VADER, lexicon-augmented for ops text, with a hard `critical` override | `positive · neutral · negative · critical` + the raw compound score |
| **Keyword extraction** | YAKE (unsupervised, single-document, offline) | up to 5 best-first keyphrases, also folded into a global *trending* counter |

Every analysis is served per-message over REST, aggregated into `/api/stats` (intent / sentiment / entity-type distributions, trending keywords, throughput), and broadcast to the dashboard so an operator watches entities, badges, and charts update live as lines stream in — without polling.

---

## Architecture

One FastAPI process runs the whole engine. A single `NLPEngine` owns exactly one instance of each of the four analyzers — and, crucially, **one spaCy model in the entire process** — all built once in `NLPEngine.load()` at startup (the FastAPI `lifespan`). After load, per-line `analyze()` is cheap and constructs nothing. All shared state (the loaded engine, the rolling `StatsAggregator`, the `/ws` `ConnectionManager`) lives on `app.state.runtime`; every handler reads it defensively via `getattr(...)` and degrades to a safe fallback rather than a 500.

```
POST /api/analyze            { "message": "<log line>" }
        │
        ▼
  run_in_threadpool(engine.analyze, message)     ← CPU-bound NLP off the event loop
        │
        ├─ EntityAnalyzer    spaCy ner + EntityRuler + regex component  → entities[]
        ├─ IntentAnalyzer    TF-IDF → LogisticRegression, reject < 0.45 → intent{label,confidence}
        ├─ SentimentAnalyzer VADER + ops lexicon + critical override    → sentiment{label,score}
        └─ KeywordAnalyzer   YAKE top-5                                  → keywords[]
        │
        ▼
   AnalysisResponse
        ├─► StatsAggregator.update(result)        rolling in-memory aggregates (feeds GET /api/stats)
        └─► ConnectionManager.broadcast(...)      best-effort push: one {type:"analysis"} frame
                                                  + one {type:"stats"} frame to every /ws client

React + Vite + Recharts dashboard (nginx :3000, /api + /ws reverse-proxied):
   AnalyzeBox · ResultCard · LiveFeed (WS) · KPI tiles · intent/sentiment/entity-type/trending charts
```

The analyze handlers are `async`, but the GIL-releasing spaCy/sklearn work is pushed to the threadpool via `run_in_threadpool` so the event loop stays free; the fast stats fold and the best-effort broadcast then run inline. Batch analyze amortizes the two per-call-expensive paths — entities through a single `nlp.pipe` pass, intents through one `predict_proba` over the batch. A broadcast failure can **never** turn a successful analysis into a failed HTTP response.

**Module layout** (`src/`; `models.py` is the single source of truth for the API vocabulary, `nlp/` is the analyzer subpackage with `NLPEngine` as its orchestrator):

```
src/
├── config.py             # pydantic-settings Settings + get_settings() (all env tunables)
├── models.py             # AnalyzeRequest/BatchAnalyzeRequest, AnalysisResponse, Entity, Intent/SentimentResult
├── api.py                # create_app(runtime) factory: REST routes + /ws WebSocket + CORS
├── main.py               # Runtime dataclass + lifespan (loads the engine once) + `app`
├── stats.py              # StatsAggregator — thread-safe rolling in-memory aggregates
├── ws.py                 # ConnectionManager (connect/disconnect/broadcast, dead-socket pruning)
├── generators.py         # deterministic seedable LABELED corpus (intent training + E2E ground truth)
└── nlp/
    ├── __init__.py       # NLPEngine — owns one of each analyzer, load once / analyze cheaply
    ├── entity.py         # EntityAnalyzer — spaCy NER + EntityRuler + regex log-entity component
    ├── intent.py         # IntentAnalyzer — TF-IDF + LogisticRegression, joblib-persisted
    ├── sentiment.py      # SentimentAnalyzer — ops-augmented VADER + critical override
    └── keyword.py        # KeywordAnalyzer (YAKE) + TrendingKeywords (rolling Counter)

scripts/
├── train_intent.py       # BUILD-TIME trainer — bakes intent.joblib into the image (accuracy-gated)
├── verify_e2e.py         # black-box E2E verifier (10 ordered HTTP/WS checks)
└── load_test.py          # concurrent perf/load harness (throughput, latency, memory gates)
```

**Services.**

| Service  | Port   | Role |
|----------|--------|------|
| backend  | `8000` | uvicorn / FastAPI — the NLP engine + REST API + `/ws` WebSocket (all state in-memory) |
| frontend | `3000` | nginx serving the React SPA, reverse-proxying `/api` → `backend:8000` and upgrading `/ws` |

---

## Tech Stack

- **Language / runtime:** Python 3.11
- **API:** FastAPI + `uvicorn[standard]`, with a real WebSocket (`/ws`) via `websockets`
- **Entities:** spaCy 3.7 `en_core_web_sm` (pinned as a wheel URL — no `spacy download` step), `EntityRuler` + a custom regex pipeline component
- **Intent:** scikit-learn (`TfidfVectorizer` + `LogisticRegression` `Pipeline`), persisted with `joblib`
- **Sentiment:** `vaderSentiment` (lexicon augmented for ops vocabulary)
- **Keywords:** `yake` (self-contained — ships its own stopword lists inside the wheel, reads **no** NLTK data)
- **Models / config:** pydantic v2 + pydantic-settings
- **Frontend:** React 18 + Vite 5 + Recharts, served by nginx
- **Infra:** Docker + Docker Compose. No database, no Redis, no message queue — state is in-memory, and the intent model is trained at image-build time and baked in.

> Guard: **torch never enters this project.** The `en_core_web_sm` pipeline is CPU-only; pulling torch would bloat the image ~1 GB for no benefit.

---

## NLP Components & Algorithms

Each capability lives in its own module, is independently unit-tested, and is fully deterministic (fixed model weights + rules, a seeded/baked intent pipeline, fixed lexicons, a fixed statistical keyword algorithm) — a given message always yields the same analysis, with no wall clock and no global RNG.

### Entity recognition (`EntityAnalyzer`)

A log line carries two kinds of entity: **general language** ones spaCy's statistical `ner` already handles (`PERSON`, `DATE`, `ORG`, `CARDINAL`, …), and **log-specific** ones it has never seen (it mislabels an IP as `CARDINAL`, a `user 4821` as a plain number). Rather than train a custom NER model, the analyzer keeps spaCy's `ner` and layers deterministic rules around it in one pipeline loaded **once**, with the tagger / parser / lemmatizer / attribute_ruler **disabled** (only `tok2vec` + `ner` are needed):

```
tok2vec → entity_ruler (before ner) → ner → log_entity_regex (last)
```

1. **`EntityRuler` (before `ner`)** — a **SERVICE gazetteer** (phrase patterns, robust to however spaCy tokenises `payments-api`) plus a `*-svc`/`*-api` shape, and `ERROR_CODE`/`USER_ID` token patterns. Running before `ner` means these win over the statistical model for their tokens.
2. **`log_entity_regex` component (last)** — the entities whose surface spaCy's tokenizer splits unpredictably (IP, URL, PATH, PORT, HOST, and the *contextual* USER_ID — bare digits after `user`/`for`/`by`) are matched with `re` against `doc.text` and projected back with `doc.char_span(..., alignment_mode="expand")`, so the span snaps to whatever token boundaries exist. Regex matches are resolved among themselves in a fixed priority order (URL ▶ PATH ▶ IP ▶ ERROR_CODE ▶ USER_ID ▶ PORT ▶ HOST) so a container wins over a fragment it encloses.
3. **Merge** — surviving regex spans are unioned with the ruler/`ner` entities that don't overlap them and passed through `spacy.util.filter_spans`, so a log-label span always beats a general span it overlaps (an IP is an IP, not a `CARDINAL`) while non-overlapping general entities are preserved.

Labels: `SERVICE · HOST · IP · USER_ID · ERROR_CODE · PATH · URL · PORT`, plus general spaCy entities (`PERSON`, `DATE`, `CARDINAL`, …) additively.

### Intent classification (`IntentAnalyzer`)

One sklearn `Pipeline`:

```
TfidfVectorizer(ngram_range=(1,2), sublinear_tf=True, min_df=2)
    → LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)
```

- `ngram_range=(1,2)` captures discriminative **phrases** ("health check passed", "high memory usage"), not just bag-of-words; `sublinear_tf` dampens repeats; `min_df=2` drops once-only surface noise (a specific IP/host/user id carries no intent).
- Trained on the **synthetic templated corpus** from `src/generators.py` — 8 intents: `authentication`, `deployment`, `error_report`, `health_check`, `resource_warning`, `network`, `database`, `config_change`.
- **Confidence** is the max `predict_proba` in `[0, 1]`. When it falls **below `0.45`** the line is too ambiguous / out-of-distribution to trust, so the label becomes the **`other`** reject bucket — but the reported confidence stays the real max probability (the `other` verdict is about the label, not the number).
- The whole pipeline is **trained at Docker build time** (`scripts/train_intent.py`, gated at ≥ 0.80 held-out accuracy) and baked in as a `joblib` artifact — persisted whole (vectorizer + model together) to avoid train/serve skew, gitignored, and reproduced deterministically on every build. Off-image (unit tests), it trains on the fly from the seeded corpus without writing the artifact, keeping tests hermetic.

### Sentiment / severity (`SentimentAnalyzer`)

Severity is a property of the *phrasing*, not the intent (an `authentication` line can be a cheerful "login succeeded" or a "brute-force attack detected"). VADER's lexicon is tuned for social media, so out-of-the-box it scores the overwhelming majority of ops log lines at compound `0.0` — flat neutral, useless as a signal. Two augmentations fix that:

- **Ops lexicon** — VADER's lexicon is merged (once, at construction) with ops/SRE vocabulary on its native −4…+4 valence scale (`failed`, `timeout`, `segfault`, `oom`, `healthy`, `recovered`, …), overriding any base entry so the scale is ops-consistent. The same VADER machinery (booster words, ALL-CAPS emphasis, negation) then lights up on real log phrasings.
- **Hard `critical` override** — a whole-word match on `fatal · panic · segfault · outage · oom · data loss · corrupt · critical` forces the `critical` label regardless of the compound score (a single "FATAL" is decisive even when surrounding positive words would dilute the sum).

Otherwise the compound score thresholds decide: `≤ −0.60 → critical`, `≤ −0.05 → negative`, `≥ +0.05 → positive`, else `neutral`. Returns `(label, compound)` with the raw score in `[-1, 1]`.

### Keyword extraction (`KeywordAnalyzer` + `TrendingKeywords`)

Extracted per line with **YAKE** — unsupervised, single-document, statistical (term casing, position, frequency, in-context relatedness, sentence dispersion): no corpus, no training, no persisted artifact. YAKE was chosen deliberately over `rake-nltk`, which needs `nltk.download(...)` corpora at runtime (the `punkt_tab` trap) — an extra image layer and an offline-build failure mode. YAKE ships its stopword lists **inside its own wheel**, so the image stays smaller and fully offline-capable. Returns up to 5 best-first (lower YAKE score = more relevant), de-duplicated case-insensitively. A global `TrendingKeywords` `Counter` folds every line's keywords in (case-normalised, ties broken alphabetically) to power the dashboard's trending panel.

### Serving

All four analyzers — and the single spaCy model — load **once** at startup in the FastAPI `lifespan`. Analyze handlers are `async` and offload the CPU work via `run_in_threadpool`; batch analyze uses `nlp.pipe` + a single vectorized `predict_proba`. Every analyze folds into the rolling stats and best-effort broadcasts an `analysis` frame + a `stats` frame over `/ws`.

---

## How to Run

Everything runs in Docker — no local Python or Node needed, only Docker with Compose v2.

```bash
# Full stack incl. the dashboard (backend + frontend), detached
make ui                 # Dashboard: http://localhost:3000 · API: http://localhost:8000

# backend only (no dashboard)
make up                 # API: http://localhost:8000  (GET /api/health)

# helper scripts (build, wait for /api/health, print the URL)
./start.sh
./stop.sh
```

**Overriding host ports.** Both host ports are compose-level and overridable — handy because sibling projects in this repo often hold `:8000` / `:3000`:

```bash
BACKEND_PORT=8010 FRONTEND_PORT=3001 make ui
# Dashboard: http://localhost:${FRONTEND_PORT:-3000} · API: http://localhost:${BACKEND_PORT:-8000}
```

Quick smoke test:

```bash
curl -s http://localhost:8000/api/health
# {"status":"healthy","analyzer_ready":true}

curl -s -X POST http://localhost:8000/api/analyze \
  -H 'Content-Type: application/json' \
  -d '{"message":"auth-svc rejected login for user 4821 from 10.0.0.1: invalid token"}'
```

### Make Targets

| Target       | What it does |
|--------------|--------------|
| `build`      | Build all images (backend + test) |
| `up`         | Run the backend detached, print the API URL |
| `down`       | Stop and remove the stack |
| `logs`       | Tail the backend logs |
| `ui`         | Run backend + React dashboard detached, print the URLs |
| `test`       | Full pytest suite in Docker (unit + integration; rebuilds the tester image first) |
| `test-unit`  | Unit tests only, in Docker |
| `test-int`   | Integration tests only, in Docker |
| `e2e`        | Black-box E2E verifier vs. the live backend — 10 ordered checks |
| `load`       | Perf/load gates vs. the live backend (throughput, p95 latency, memory) |
| `clean`      | `down` + remove volumes and orphans |

`make e2e` and `make load` are **hard-gated** — the first failed check / breached gate exits non-zero. Every gate is host-overridable, so e.g. `MIN_ACCURACY=0.99 make e2e` or `MIN_MSGS_PER_SEC=100000 make load` proves the gate bites.

---

## REST API

Every handler reads shared state off `app.state.runtime` and **degrades gracefully** when a piece is missing — reads fall back to empty, analyze falls back to `503` — so a missing runtime never becomes a `500`.

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/api/health`               | Liveness — dependency-free, always `200` while alive |
| `POST` | `/api/analyze`              | Analyze one log line → entities, intent, sentiment, keywords (+ stats fold + broadcast) |
| `POST` | `/api/analyze/batch`        | Analyze many lines in one request (order preserved) via `nlp.pipe` |
| `GET`  | `/api/stats`                | Rolling aggregate snapshot powering the dashboard |
| `GET`  | `/api/debug/memory`         | Backend RSS in MB (`{"memory_mb": …}`) — load-test probe |
| `GET`  | `/api/debug/ground-truth?n=`| Up to `n` labeled corpus samples for inspection (E2E aid) |
| `WS`   | `/ws`                       | Real-time live feed (see below) |

**`GET /api/health`** — the frozen contract (the two keys, nothing more; asserted verbatim by tests and the E2E verifier):

```json
{ "status": "healthy", "analyzer_ready": true }
```

**`POST /api/analyze`** — body `{ "message": "<log line>" }`. Requires a loaded engine (else `503 analyzer not ready`):

```json
{
  "message": "auth-svc rejected login for user 4821 from 10.0.0.1: invalid token",
  "entities": [
    { "text": "auth-svc", "label": "SERVICE", "start": 0, "end": 8 },
    { "text": "4821", "label": "USER_ID", "start": 32, "end": 36 },
    { "text": "10.0.0.1", "label": "IP", "start": 42, "end": 50 }
  ],
  "intent": { "label": "authentication", "confidence": 0.94 },
  "sentiment": { "label": "negative", "score": -0.6 },
  "keywords": ["invalid token", "rejected login", "auth-svc"]
}
```

**`POST /api/analyze/batch`** — body `{ "messages": [ … ] }` → `{ "results": [ …AnalysisResponse… ], "count": <int> }`. An empty list yields `{"results": [], "count": 0}`.

**`GET /api/stats`** — the rolling snapshot (every key always present):

```json
{
  "total_analyzed": 1284,
  "intent_distribution": { "authentication": 210, "error_report": 305, "...": 0 },
  "sentiment_distribution": { "negative": 540, "critical": 190, "neutral": 402, "positive": 152 },
  "entity_type_distribution": { "SERVICE": 900, "IP": 410, "USER_ID": 388, "...": 0 },
  "trending_keywords": [ ["invalid token", 74], ["connection refused", 51] ],
  "recent": [ { "message": "…", "intent": "error_report", "sentiment": "critical", "ts": 1752... } ],
  "throughput_per_sec": 42.7
}
```

### WebSocket `/ws`

- **Connect** to `ws://localhost:3000/ws` (through nginx) or `:8000/ws` (direct). The server accepts the handshake and registers the client.
- **Keepalive:** client sends the text frame `"ping"` → server replies `"pong"`. Any other inbound text is ignored (the client is a listener).
- **Push:** on every completed analyze, the server broadcasts one frame per result plus a trailing stats frame to all connected clients:

  ```json
  { "type": "analysis", "data": { /* a full AnalysisResponse */ } }
  { "type": "stats",    "data": { /* a full /api/stats snapshot */ } }
  ```

Broadcasting is best-effort and dead sockets are pruned, so one broken client never breaks the fan-out or the `POST` that triggered it.

---

## Dashboard

A React 18 + Vite SPA served by nginx, reverse-proxying `/api` (REST) and `/ws` (WebSocket) to the backend — the browser only ever talks to nginx on one origin, so no CORS and no hard-coded backend host. The layout:

- **AnalyzeBox + ResultCard** — type a log line, POST it, and see its highlighted entities, intent/sentiment badges, and keyword chips.
- **Live feed** — every analyzed line (including your own, which the backend echoes back over `/ws`) streams in newest-first over the WebSocket.
- **KPI tiles + charts row** (Recharts) — intent distribution, sentiment distribution, entity-type distribution, and trending keywords, all populated on load and updated live on each WS `stats` frame.

The charts hook uses the **WebSocket for live push** and a **REST poll for load-time bootstrap** (freshest snapshot wins via a monotonic `total_analyzed` merge, so neither a late poll nor an out-of-order frame moves counts backward). The ~5 s fallback poll intentionally **pauses while the tab is hidden** (`document.hidden`) and catches up on refocus — power-saving, so a backgrounded tab may show empty charts until you return to it. On a dropped feed the hook keeps the last-good data and flags it `stale` rather than blanking out.

---

## Configuration

Backend settings (`src/config.py`) are read from **field defaults → optional `.env` → environment variables**; each env var name is the **upper-cased field name** (e.g. `STATS_WINDOW` ← `stats_window`). See [`.env.example`](.env.example) for the full committed template. The two host ports are compose-level mappings, and the E2E / load gates are read by the verifier / load harness (not the backend).

| Setting | Default | Meaning |
|---------|---------|---------|
| `LOG_LEVEL` | `INFO` | Root log level (`DEBUG` \| `INFO` \| `WARNING` \| `ERROR`) |
| `BACKEND_PORT` | `8000` | API host port (compose maps → uvicorn `:8000`) |
| `FRONTEND_PORT` | `3000` | Dashboard host port (compose maps → nginx `:80`) |
| `STATS_WINDOW` | `500` | Rolling-window size for `recent` + the throughput window in `StatsAggregator` |
| `TRENDING_TOP_K` | `10` | Number of trending keywords `/api/stats` returns |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins, or `*` for any (credentials disabled with `*`) |
| `WS_ENABLED` | `true` | Operability switch for the `/ws` live feed |
| **E2E gates** (`make e2e`) | | *read by `scripts/verify_e2e.py`* |
| `MIN_ACCURACY` | `0.80` | Intent-accuracy floor over `ACCURACY_SAMPLES` ground-truth samples |
| `ACCURACY_SAMPLES` | `40` | Ground-truth samples for the accuracy / NER checks + latency loop |
| `MIN_NER_RECALL` | `0.80` | NER recall floor (fraction of ground-truth `(text, label)` entities returned) |
| `MIN_CRITICAL_RECALL` | `0.75` | Critical-severity recall floor (critical lines must read critical/negative) |
| `MIN_KEYWORDS` | `1` | Minimum keywords on the crafted analyze line |
| `MAX_P95_MS` | `500` | Analyze p95 latency ceiling (ms) — shared by e2e + load |
| `E2E_WS_TIMEOUT` | `10` | Seconds to wait for the WebSocket frames |
| **Load gates** (`make load`) | | *read by `scripts/load_test.py`* |
| `LOAD_MESSAGES` | `2000` | Total analyze POSTs fired |
| `LOAD_CONCURRENCY` | `20` | Max in-flight POSTs |
| `MIN_MSGS_PER_SEC` | `100` | Throughput floor (msgs/s) |
| `MAX_BACKEND_MEM_MB` | `500` | Backend RSS ceiling (MB) — shared by e2e + load |

---

## Testing & Measured Performance

Everything is verified **in Docker** — unit + integration tests, a black-box E2E verifier, and a load harness, all profile-gated compose services.

```bash
make test        # 129 unit + integration tests
make e2e         # 10-check black-box verifier vs. the live backend
make load        # hard-gated perf gates vs. the live backend
```

- **Unit tests** cover every module — entity, intent, sentiment, keyword, engine, stats, ws, health, generators.
- **Integration tests** exercise the analyze / batch / stats API and the WebSocket against an injected-runtime app.
- **E2E** (`scripts/verify_e2e.py`) walks 10 ordered black-box checks over HTTP/WS: `/api/health` verbatim → analyze exposes all four capabilities → batch preserves order + schema → **intent accuracy gate** → **NER recall gate** → **critical-severity recall gate** → `/api/stats` shape + counters advance → **WebSocket push** (analysis + stats frames) → **analyze p95 latency gate** → **backend memory ceiling**.
- **Load** (`scripts/load_test.py`) fires 2000 concurrent analyze POSTs and gates throughput, under-load p95 latency, zero errors, and server-reported memory.

### Measured Performance

From the final Docker verification run:

| Metric | Result | Gate |
|--------|--------|------|
| Unit + integration tests | **129 passing** | all green |
| E2E checks | **10 / 10 passed** | all pass |
| Intent accuracy | **0.975** (39/40) | ≥ 0.80 |
| NER recall | **1.000** (94/94 ground-truth entities) | ≥ 0.80 |
| Critical-severity recall | **0.926** (25/27) | ≥ 0.75 |
| Analyze latency (sequential) | **p50 3.1 ms · p95 5.6 ms** | p95 ≤ 500 ms |
| Backend RSS (E2E) | **249 MB** | ≤ 500 MB |
| WebSocket push | **verified** (analysis + stats frames) | frames arrive |
| Load throughput | **165 msgs/s** (2000 msgs @ concurrency 20 in ~12 s, **0 errors**) | ≥ 100 msgs/s |
| Load latency (under load) | **p50 112 ms · p95 185 ms · p99 331 ms** | p95 ≤ 500 ms |
| Backend RSS (load) | **263 MB** | ≤ 500 MB |

The **dashboard** was verified end-to-end in Chrome: the analyze flow renders highlighted entities + intent/sentiment badges + keyword chips; the live WebSocket feed streams each analyzed line; and the Recharts intent / sentiment / entity-type / trending panels populate on load and update live.

---

## What I Learned

- **Synthetic templated training is a double-edged sword.** Training the intent classifier on a balanced templated corpus yields ~1.0 held-out accuracy and a measured 0.975 on held-out draws — but a real log line phrased *outside* the templates abstains to `other`. That's the confidence threshold working exactly as intended (better an honest "not sure" than a confident wrong label), and it's precisely where a small amount of real labeled data would move the needle most.

- **VADER is useless on logs until you augment it.** Out-of-the-box VADER scores ops text at compound ~0.0 (flat neutral) because its lexicon is tuned for social media. Merging an ops/SRE lexicon on VADER's own valence scale — plus a hard `critical` override for unambiguous tokens like `fatal`/`oom`/`data loss` — was the difference between a dead signal and a faithful one. The override matters because a single "FATAL" should win even when surrounding positive words would otherwise dilute the compound.

- **YAKE over rake-nltk avoids the NLTK download trap.** `rake-nltk` needs `nltk.download(...)` corpora at runtime (the infamous `punkt_tab` failure), which means an extra image layer, a network fetch, and a new offline-build failure mode. YAKE ships its stopword lists inside its own wheel and reads no NLTK data — smaller image, fully offline, one less thing to break.

- **spaCy's tokenizer splits log tokens unpredictably, so rules must beat the model.** An IP or a path gets tokenised in ways the statistical `ner` mislabels (IP → `CARDINAL`). The reliable pattern was regex-on-`doc.text` → `doc.char_span` → `filter_spans`, with the log-label spans deliberately winning over overlapping general entities. It's robust — though general spaCy entities like `PERSON` still occasionally surface on short lines, which is fine as additive signal.

- **Load the model once, keep handlers thin.** Building all models once in the `lifespan` and offloading the CPU work to the threadpool keeps per-line latency at ~3–5 ms. (The well-known FastAPI + spaCy event-loop deadlock is transformer-pipeline-specific and N/A to `en_core_web_sm` — but `run_in_threadpool` is still the right call so a slow line never stalls the WebSocket broadcasts.)

- **Bake the models at build time for a self-contained image.** Pinning the spaCy model as a wheel URL (no `spacy download` step) and training + `joblib`-dumping the intent pipeline during `docker build` means the runtime image is fully offline and reproducible — the build even gates on held-out accuracy, so a broken model can never silently ship.

- **The dashboard needs two data paths, and the poll must pause.** WebSocket push gives instant updates, but a fresh page load has no live frame yet — so a REST poll bootstraps the charts, and a monotonic `total_analyzed` merge makes the two sources order-independent. Pausing that poll on `document.hidden` is deliberate power-saving: a backgrounded tab shows empty charts until refocus, which reads as a bug but isn't.

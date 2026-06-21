# ml-log-classifier

A streaming machine-learning pipeline that classifies raw log entries by **severity** and **category** in real time, using a **soft-voting ensemble** of text classifiers, served behind a FastAPI API with a live React + WebSocket dashboard.

---

## What It Does

- **Ingests raw, unstructured log text** over HTTP (single, batch, or streaming) and classifies each entry along two axes:
  - **Severity** — `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`.
  - **Category** — `SYSTEM`, `AUTH`, `NETWORK`, `DATABASE`, `PERFORMANCE`, `SECURITY`, `APPLICATION`.
- **Returns a confidence score** with every prediction (the max soft-voting probability per axis, plus an overall mean).
- **Streams predictions live** to a React dashboard over a WebSocket so you can watch classifications as logs flow in.
- **Trains on demand** — kick off a batch retrain from the API; the freshly trained ensemble is **hot-swapped** into the running service with zero downtime (the old model keeps serving until the new one is ready).
- **Classifies hierarchically per service** (web / database / cache): predict the service, apply that service's own severity model, and derive a cross-service **anomaly score** from ensemble voting (Feature Area A).
- **Adapts to drift** — ops submit ground-truth labels via `/feedback`; a drift monitor watches recent accuracy and **auto-retrains** when it slips below a threshold (Feature Area B).
- **Serves A/B model versions** with graceful fallback — traffic is split across a champion and challenger version, falling back to any healthy model on error, with per-version serving metrics and explicit promotion (Feature Area C).
- **Caches repeated log patterns** (LRU) to cut inference cost on hot traffic, and exposes cache effectiveness, feature importances, and live metrics for observability.

---

## Tech Stack

- **Language:** Python 3.11
- **Web framework:** FastAPI + Uvicorn (REST + async streaming + WebSocket)
- **ML:** scikit-learn — a **soft-voting `VotingClassifier`** ensemble of **MultinomialNB + RandomForest + GradientBoosting** (weights `[1, 2, 3]`), over **TF-IDF (1–2 grams) + temporal + metadata** features combined via a `ColumnTransformer` (`MinMaxScaler` on the dense columns); NumPy / SciPy; **NLTK** (`punkt` + `stopwords`, downloaded at image build time) for tokenization.
- **Model persistence:** joblib (versioned in-process registry).
- **Dashboard:** **React + Vite + Chart.js** (`react-chartjs-2`), built to static assets and served by **nginx**, which reverse-proxies `/api` and `/ws` to the backend; live updates over the FastAPI WebSocket.
- **Validation / config:** Pydantic v2 schemas + a dataclass-based `Settings` (precedence **defaults → YAML → env**).
- **Testing:** pytest, pytest-asyncio (223 tests), plus black-box perf / load / E2E scripts run in Docker.
- **Deployment:** Docker + Docker Compose.

---

## Architecture

### Backend (`src/`)

| Module | Responsibility |
|---|---|
| `config.py` | `Settings` dataclass + `get_config()` — precedence **defaults → `config/config.yaml` → env**. |
| `log_generator.py` | Template-based synthetic log generator (~1000 labeled logs across web / database / cache) with injected timestamp/IP/UUID noise; deterministic via `random_seed`. |
| `preprocess.py` | Strip timestamps / IPv4·IPv6 / UUIDs, lowercase, collapse whitespace, NLTK tokenize + stopword removal. |
| `features.py` | `FeaturePipeline` — `TfidfVectorizer(ngram_range=(1,2), max_features=5000)` on text + temporal (hour/day) + metadata (service / level indicator / request-id presence) dense columns, fused by a `ColumnTransformer` with `MinMaxScaler` so the combined matrix stays **non-negative** (required by MultinomialNB) and fixed-length. |
| `classifiers.py` | Factories for `MultinomialNB`, `RandomForestClassifier`, `GradientBoostingClassifier` + thin predict-with-confidence wrappers. |
| `ensemble.py` | `build_ensemble()` (soft-voting `VotingClassifier`, weights `[1,2,3]`) + `LogClassifier` — wires preprocess → features → **separate** severity & category ensembles, with LRU prediction caching and feature-importance introspection. |
| `trainer.py` | End-to-end training + `cross_val_score` (target > 85%) producing both ensembles and their metrics. |
| `model_store.py` | `ModelRegistry` — versioned joblib artifacts + `metadata.json` + a `threading.Lock`; `save_version` / `get_current` / `set_current` / `latest`. |
| `multiservice.py` | `MultiServiceClassifier` — hierarchical service → per-service severity model + global category, with a cross-service **anomaly score** from per-service severity-vote agreement (Feature Area A). |
| `adaptive.py` | `DriftMonitor` — rolling-window severity-correctness tracker that decides when recent accuracy has dropped below `accuracy_retrain_threshold` (Feature Area B). |
| `serving.py` | `ABRouter` — splits traffic across champion (A) / challenger (B) registry versions, **graceful fallback** to any healthy model, per-version serving metrics, and `promote` (Feature Area C). |
| `metrics.py` | `MetricsAggregator` (thread-safe totals, severity/category/service distributions, avg confidence, throughput, recent-predictions ring buffer) + `ConnectionManager` for WebSocket fan-out. |
| `cache.py` | LRU cache mapping a preprocessed log → its prediction (the bonus "cache repeated log patterns" optimization). |
| `schemas.py` | Pydantic v2 request/response models for every route. |
| `api.py` / `main.py` | FastAPI app factory (startup load-or-train via the lifespan, all routes, the background metrics broadcaster) + the uvicorn entrypoint. |

### Frontend (`frontend/`)

| File | Responsibility |
|---|---|
| `src/App.jsx` | Top-level layout; wires the WebSocket snapshot into every panel. |
| `src/hooks/useWebSocket.js` | Live `/ws/metrics` subscription with auto-reconnect. |
| `src/api.js` | Relative-URL REST client (`/api/...`) for classify / train / models / feature-importance. |
| `src/components/StatCards.jsx` | Live stat cards: total classified, throughput, avg confidence, model status. |
| `src/components/SeverityChart.jsx` / `CategoryChart.jsx` | Chart.js distributions of severity / category. |
| `src/components/FeatureImportance.jsx` | Top engineered features by RandomForest importance. |
| `src/components/PredictionsTable.jsx` | The most-recent predictions feed. |
| `src/components/ClassifyForm.jsx` | Submit a raw log and render its prediction. |
| `src/components/ModelPanel.jsx` | Model versions, A/B configuration, and adaptive/drift status. |
| `Dockerfile` / `nginx.conf` | Multi-stage `node:20-alpine` build → `nginx:alpine`, proxying `/api` + `/ws` to the `app` service. |

### Data flow

```
                              ┌──────────────────────────────────────────────┐
   raw logs (HTTP / WS) ─────▶│  FastAPI app (:8000)                          │
                              │   preprocess → features → ensembles           │
   React dashboard ◀──/ws──── │   severity + category + confidence            │
        (:8080, nginx)        │   ├─ MetricsAggregator (live snapshot)        │
                              │   ├─ MultiServiceClassifier (svc + anomaly)   │
   ops feedback ──/feedback──▶│   ├─ DriftMonitor → auto-retrain on drift     │
                              │   └─ ABRouter (champion/challenger + fallback)│
   POST /train ─────────────▶│   Trainer → ModelRegistry (versioned) ──hot-swap
                              └──────────────────────────────────────────────┘
```

On startup the app loads a persisted model from the registry, or (when `auto_train` is on) trains `v1` on the generated corpus — so a successful `GET /health` implies the model is **ready** before the first request is served.

---

## How to Run

Docker-first. From `ml-log-classifier/`:

```bash
make build      # build the app + test images
make up         # run the API live (detached) → http://localhost:8000
make ui         # run API + React dashboard (detached) → http://localhost:8080
make logs       # tail the app logs
make down       # stop and remove the stack
make clean      # down + remove volumes and orphans
```

- API (OpenAPI docs): http://localhost:8000/docs
- Dashboard: http://localhost:8080

> First boot trains a model during startup (~10–15 s) before the service reports healthy; the Docker healthcheck has a 60 s grace period to cover this.

Quick smoke test once `make up` is healthy:

```bash
curl -s localhost:8000/health
curl -s -X POST localhost:8000/classify \
  -H 'Content-Type: application/json' \
  -d '{"raw_log":"Database connection failed with timeout error"}'
```

---

## Run Tests

All tests run **inside Docker** (never on the host):

```bash
make test       # full pytest suite (223 tests) in Docker
make test-unit  # unit tests only
make test-int   # integration tests only
make e2e        # full black-box end-to-end verifier against the live app
make load       # latency + throughput probes against the live app
```

- `make e2e` brings up `app`, runs `scripts/verify_e2e.py` through the **entire flow** (health → classify → stream → WebSocket → train → models / feature-importance / adaptive / services / cache → stats), and fails the build on the first failed check.
- `make load` runs `scripts/perf_test.py` (gate: p95 `/classify` latency < 100 ms) and `scripts/load_test.py` (gate: > 50 req/s, plus a reported batch logs/s).

---

## API Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness probe (used by the Docker healthcheck). |
| `GET` | `/stats` | `total_classified` count + `model_status`. |
| `POST` | `/classify` | Classify a single log → severity + category + confidence. |
| `POST` | `/classify/batch` | Classify a list of logs in one vectorized pass. |
| `POST` | `/classify/stream` | Stream classification results as newline-delimited JSON (NDJSON). |
| `POST` | `/classify/service` | Hierarchical multi-service classify (service → severity) + anomaly score. |
| `POST` | `/classify/ab` | Classify via the A/B router (champion/challenger) with graceful fallback. |
| `POST` | `/train` | Kick off an on-demand background retrain (returns `202`; hot-swaps on success). |
| `GET` | `/train/status` | Poll the training lifecycle (status, version, `is_training`, last metrics). |
| `POST` | `/feedback` | Submit ground-truth for a log; records drift and may auto-retrain. |
| `GET` | `/adaptive/status` | Drift-monitor snapshot (recent accuracy, window, threshold) + `is_training`. |
| `GET` | `/models` | List registry versions with A/B annotations + per-version serving metrics. |
| `POST` | `/models/promote` | Promote a version to champion (group A). |
| `POST` | `/models/ab` | (Re)configure the A/B router (champion, challenger, split). |
| `GET` | `/services` | The services the multi-service model knows + per-service severity classes. |
| `GET` | `/feature-importance` | Top engineered features by RandomForest importance (`?top=N`). |
| `GET` | `/cache/stats` | Prediction-cache effectiveness (hits / misses / hit-rate / size / capacity). |
| `GET` | `/metrics` | The live-metrics snapshot as plain JSON (REST mirror of the WS feed). |
| `WS` | `/ws/metrics` | Live metrics stream for the dashboard. |

### Examples

Single classify (the spec's canonical example):

```bash
curl -s -X POST localhost:8000/classify \
  -H 'Content-Type: application/json' \
  -d '{"raw_log":"Database connection failed with timeout error"}'
```

```json
{
  "severity": "ERROR",
  "category": "SYSTEM",
  "confidence": 0.942,
  "severity_confidence": 0.95,
  "category_confidence": 0.93
}
```

> The spec's `/stats` shape:
>
> ```json
> {"total_classified": 0, "model_status": "ready"}
> ```

Hierarchical multi-service classify (Feature Area A):

```bash
curl -s -X POST localhost:8000/classify/service \
  -H 'Content-Type: application/json' \
  -d '{"raw_log":"Database connection failed with timeout error"}'
```

```json
{
  "service": "database",
  "service_confidence": 0.88,
  "severity": "ERROR",
  "severity_confidence": 0.95,
  "category": "SYSTEM",
  "category_confidence": 0.93,
  "confidence": 0.92,
  "anomaly_score": 0.12
}
```

---

## Configuration / Environment

Configuration is resolved as **dataclass defaults → `config/config.yaml` → environment variables** (env wins). The YAML keys map one-to-one to `Settings` fields:

| Knob | Default | Notes |
|---|---|---|
| `host` / `port` | `0.0.0.0` / `8000` | API bind. Compose maps host `PORT` (default 8000) and `DASHBOARD_PORT` (default 8080). |
| `model_dir` / `data_dir` | `/app/models` / `/app/data` | Mounted named volumes in Docker. |
| `sample_size` | `1000` | Generated training corpus size. |
| `random_seed` | `42` | Reproducible generation / fits. |
| `tfidf_max_features` | `5000` | TF-IDF vocabulary cap. |
| `tfidf_ngram_max` | `2` | n-gram upper bound (1–2 grams). |
| `rf_n_estimators` | `100` | RandomForest trees. |
| `gb_n_estimators` | `100` | GradientBoosting stages. |
| `ensemble_weights` | `[1, 2, 3]` | Soft-voting weights (NB / RF / GB). |
| `accuracy_retrain_threshold` | `0.90` | Adaptive loop retrains when recent accuracy drops below this. |
| `drift_window` | `100` | Rolling feedback window for drift detection. |
| `target_latency_ms` | `100` | Latency target (perf-test gate). |
| `cache_size` | `1024` | LRU prediction-cache capacity. |

Other env overrides: `LOG_LEVEL`, `CONFIG_PATH`. See `.env.example`.

---

## Dashboard

The dashboard (React + Vite + Chart.js, built to static assets and served by nginx with live WebSocket updates) shows:

- **Live stat cards** — total classified, throughput (logs/s), average confidence, model status/version.
- **Severity & category charts** — live distributions (Chart.js).
- **Feature importance** — the model's top engineered features by RandomForest importance.
- **Recent predictions table** — the streaming feed of the latest classifications.
- **Classify form** — submit a raw log and see its prediction rendered immediately.
- **Model / serving panel** — registry versions, A/B (champion/challenger) configuration, and adaptive/drift status.

nginx reverse-proxies `/api` and `/ws` to the backend, so the browser only ever talks to nginx (no CORS gymnastics, single origin).

---

## Success Criteria

| Criterion (spec §5) | Verified result |
|---|---|
| 90%+ classification accuracy | CV severity **1.0** / category **0.985**; held-out test severity **1.0** / category **1.0**. |
| Inference latency < 100 ms | p95 `/classify` ≈ **0.6 ms** (`scripts/perf_test.py`, hard gate). |
| 1000+ logs/second throughput | ≈ **6,600 logs/s** on the `/classify/batch` path (`scripts/load_test.py`). |
| > 50 requests/second | ≈ **3,100 req/s** concurrent `/classify` (hard gate). |
| Cross-validation > 85% | CV severity 1.0 / category 0.985 (reported during training). |
| 12+ unit tests pass | **223 tests** pass (`make test`). |
| Sample classification correct | `"Database connection failed with timeout error"` → **ERROR / SYSTEM**. |
| `/stats` returns `model_status: "ready"` | Verified after startup load-or-train. |
| Integration / E2E workflow | `make e2e` → `scripts/verify_e2e.py` exercises the full flow. |

> Throughput/latency figures are from black-box runs in Docker on the dev machine; they are reproducible via `make load`.

---

## What I Learned

- **Combining heterogeneous features for MultinomialNB.** TF-IDF is naturally sparse and non-negative, but the temporal/metadata columns are dense and can be negative — and MultinomialNB rejects negatives. A `ColumnTransformer` (TF-IDF on text, `MinMaxScaler` on the dense columns) fuses them into one fixed-length, non-negative matrix that all three ensemble members can consume. That `MinMaxScaler`-not-`StandardScaler` choice was the critical gotcha.
- **Soft-voting ensembles with confidence weighting.** A `VotingClassifier(voting='soft', weights=[1,2,3])` averages calibrated probabilities, and `predict_proba().max()` falls out as a natural confidence score — far more useful than a bare hard-vote label.
- **Keeping blocking sklearn off the event loop.** FastAPI runs **sync `def`** handlers in a worker threadpool, so the blocking `/classify` inference never stalls the loop; the one async route (`/classify/stream`) offloads each predict via `run_in_executor`; the WebSocket has a single background broadcaster as the only periodic sender (no concurrent sends on one socket).
- **In-process model versioning + zero-downtime hot-swap.** A small joblib + JSON registry plus a single atomic reference assignment means a retrain never takes the service down and a half-built model can never replace a good one.
- **Adaptive drift retraining.** A rolling-window correctness monitor, re-armed after each swap, separates *policy* (when to retrain) from *mechanism* (how), which made both trivially testable in isolation.
- **A/B serving with graceful fallback.** Splitting traffic across champion/challenger versions and transparently falling back to any healthy model turns "a model went bad mid-swap" from an outage into a non-event.
- **Prediction caching for log streams.** Real log traffic is extremely repetitive, so an LRU cache keyed on the preprocessed line yields a high hit-rate and a large latency win for free.

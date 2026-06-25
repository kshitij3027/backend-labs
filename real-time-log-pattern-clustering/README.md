# real-time-log-pattern-clustering

An engine that **discovers hidden patterns in streaming log data** using multiple **unsupervised clustering algorithms** and surfaces them through a **live web dashboard**. As logs flow in, the engine vectorizes them, clusters them across three algorithms concurrently, flags anomalies, mines recurring patterns, and streams the emerging pattern groups — their sizes, representative templates, quality, and outliers — to the browser in real time over a WebSocket.

---

## What It Does

- **Ingests streaming log text** over HTTP (single, batch, or live form) and groups semantically/structurally similar entries into **clusters** — without any labels.
- **Runs three unsupervised algorithms concurrently** so different pattern shapes are caught:
  - **KMeans / MiniBatchKMeans** — fast centroid-based partitioning that *always* assigns a cluster; softmax-over-distance confidence and a 3σ distance test for anomalies.
  - **DBSCAN** — density-based grouping (nearest-core-point assignment) that naturally flags sparse **noise points** (`cluster_id == -1`) as outliers.
  - **HDBSCAN** — hierarchical density clustering (`approximate_predict`, GLOSH-style membership) that finds variable-density groups without a fixed cluster count.
- **Surfaces each discovered pattern** with a representative/template log line, member count, and a sampling of examples, and **categorizes** patterns by type (security / performance / error / …).
- **Flags anomalies by consensus** — a log is alerted as anomalous when **≥2 of 3** algorithms agree (cuts false positives vs. any single detector).
- **Scores cluster quality** with internal metrics (silhouette, intra-cluster coherence, Davies-Bouldin) so the dashboard shows how well the current partition separates.
- **Mines extended patterns** from the historical corpus: temporal (time-of-day / weekday), performance (latency bands + bottleneck signatures), behavioral (user/IP cohorts), and sequence (sliding-window n-gram anomaly detection).
- **Streams results live** to a React dashboard over a WebSocket so you can watch clusters form, grow, and split as logs arrive.
- **One-shot demo mode** — a batch pass over a generated/sample corpus that clusters everything once and prints a summary, for a quick non-server demonstration.

---

## Tech Stack

- **Language:** Python 3.11
- **Web framework:** FastAPI + Uvicorn (REST + async streaming + WebSocket), API on port **8000**
- **Clustering / ML:** scikit-learn (MiniBatchKMeans, DBSCAN, TF-IDF, StandardScaler, PCA, silhouette / Davies-Bouldin), standalone **`hdbscan==0.8.40`**, NumPy / SciPy / pandas; NLTK for tokenization & stopword removal
- **State / streaming:** **Redis** (cluster state + anomaly history) with a transparent in-memory fallback
- **Dashboard:** **React + Vite + Chart.js**, served by **nginx** (reverse-proxies `/api` and `/ws/stream` to the app), on port **8080**
- **Validation / config:** Pydantic v2 with a layered config (defaults → YAML → env)
- **Testing:** pytest, pytest-asyncio, plus black-box E2E / load scripts (all run in Docker)
- **Packaging:** Docker + Docker Compose (`app`, `redis`, `dashboard`, profile-gated `test` / `loadtest` / `e2e`)

> See `requirements.txt` for pinned versions.

---

## Architecture

A warm-up-then-stream pipeline. The feature space and scalers are **fit once on a historical batch at startup**, then frozen; the hot path is **predict-only**, and a background task periodically **re-fits** on a sliding window so the model tracks drift without blocking ingest.

```
        ┌─────────── warm-up (startup, synchronous) ───────────┐
        │  historical corpus → fit TF-IDF + scalers + 3 models │
        └──────────────────────────────────────────────────────┘
                                  │ (frozen)
   POST /cluster ─► preprocess ─► feature extraction ─► [ KMeans ]
   (single/batch)   (mask          (TF-IDF content +     [ DBSCAN ]  ─► consensus anomaly
                     timestamps,     temporal +          [HDBSCAN ]      + new-pattern detect
                     IPs, UUIDs,     structural +              │         + categorization
                     hex, numbers,   network +                 │         + quality metrics
                     URLs)           behavioral; PCA(2)         ▼
                                     for scatter)         Redis state ──► WebSocket ──► React
                                                          (in-mem          /ws/stream    dashboard
                                                           fallback)                     (:8080)
                                  ▲
        background re-fit every realtime.update_interval (30s) on a sliding window
```

**Key `src/` modules**

| Module | Responsibility |
|---|---|
| `config.py` | Pydantic config model + layered loader (defaults → YAML → env). |
| `schemas.py` | Pure data contracts: `LogEntry`, `ClusterAssignment`, `AnomalyAlert`, `PatternRecord`, `StatsSnapshot`, `HealthResponse`. |
| `preprocessing.py` | Regex masking of variable tokens (timestamps, IPs, UUIDs, hex, numbers, URLs) → tokenization / stopword removal. |
| `features.py` | TF-IDF content + temporal + structural + network + behavioral features; `StandardScaler` (frozen after warm-up); PCA(2) for the scatter. |
| `clustering/base.py` | Common clusterer interface (warm-up fit, predict-only assign, anomaly test). |
| `clustering/kmeans.py` | MiniBatchKMeans — always assigns; softmax-distance confidence; 3σ distance anomaly. |
| `clustering/dbscan.py` | DBSCAN — nearest-core-point assignment; noise = `-1`. |
| `clustering/hdbscan_clusterer.py` | HDBSCAN — `approximate_predict`; GLOSH-style membership. |
| `engine.py` | Orchestrates the three clusterers, consensus anomaly / new-pattern detection, categorization, incremental stats, and periodic refit (one `RLock`, thread-safe). |
| `state.py` / `clients/redis.py` | `StateStore` over Redis (never raises; degrades to in-memory). |
| `metrics.py` | Quality metrics + WebSocket `ConnectionManager` + live snapshot payload builder. |
| `patterns/temporal.py` | Recurring time-of-day / weekday pattern mining. |
| `patterns/performance.py` | Latency-band clustering + bottleneck signatures. |
| `patterns/behavioral.py` | Per-entity behavior cohorts (normal / error-heavy / security-suspect). |
| `patterns/sequence.py` | Sliding-window n-gram sequence anomaly detection. |
| `demo.py` | One-shot batch demo + corpus loader (generates logs when the committed corpus is absent). |
| `api.py` | FastAPI app, REST surface, `/ws/stream`, lifespan (warm-up + state + broadcaster). |

`main.py` is the uvicorn entrypoint; `log_generator.py` synthesizes the realistic labeled corpus (planted spikes/bursts) used for warm-up and demos.

---

## How to Run

Docker-first. All commands run from this folder.

```bash
make ui      # app + React dashboard  → http://localhost:8080  (API: http://localhost:8000)
make up      # API only (detached)     → http://localhost:8000  (/docs for OpenAPI)
make logs    # tail the app logs
make down    # stop the stack
make clean   # down + remove volumes/orphans

make test       # full pytest suite in Docker (rebuilds first)
make test-unit  # unit tests only, in Docker
make test-int   # integration tests only, in Docker
make e2e        # black-box end-to-end verifier in Docker (against the live app)
make load       # latency + throughput gates in Docker (against the live app)
```

The app warms up synchronously on startup, so `/health` reports `warming` until the initial batch is fit, then `ok`.

**Quick try — cluster one log:**

```bash
curl -s -X POST http://localhost:8000/cluster \
  -H 'Content-Type: application/json' \
  -d '{"timestamp":"2026-06-25T02:00:00Z","service":"auth","level":"ERROR","message":"Multiple failed login attempts detected","source_ip":"10.0.0.5"}'
```

Returns the per-algorithm cluster assignments, confidences, and any anomaly flag.

**One-shot demo** (batch over the sample/generated corpus, prints a summary):

```bash
docker compose run --rm test python -m src.demo
```

---

## API

Base URL `http://localhost:8000`. Interactive OpenAPI at `/docs`.

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/health` | Liveness + readiness (`warming` / `ok`), version, algorithm list. |
| `GET`  | `/` | Service banner pointing at `/docs`. |
| `POST` | `/cluster` | Cluster one log across all three algorithms; returns the combined verdict. |
| `POST` | `/cluster/batch` | Cluster a batch of logs (throughput path). |
| `GET`  | `/stats` | Aggregate engine statistics (for the dashboard stat cards). |
| `GET`  | `/clusters` | Per-cluster summaries keyed by algorithm (all three). |
| `GET`  | `/clusters/{algorithm}` | One algorithm's cluster summaries (`404` if unknown). |
| `GET`  | `/clusters/{algorithm}/{cluster_id}` | Drill-down detail for one cluster (`-1` = noise). |
| `GET`  | `/patterns` | Discovered patterns (count descending). |
| `GET`  | `/patterns/temporal` | Mined recurring temporal patterns. |
| `GET`  | `/patterns/performance` | Mined latency bands + bottleneck signatures. |
| `GET`  | `/patterns/behavioral` | Behavior cohorts (normal / error-heavy / security-suspect). |
| `GET`  | `/patterns/sequence` | Anomalous event-sequence detection results. |
| `GET`  | `/anomalies` | Recent anomaly alerts (newest first, capped by `limit`). |
| `GET`  | `/scatter/{algorithm}` | Recent buffered points projected to 2-D, coloured by algorithm. |
| `GET`  | `/config` | The resolved application configuration. |
| `WS`   | `/ws/stream` | Live snapshot stream (stats / quality / patterns / anomalies) for the dashboard. |

---

## Configuration

All clustering parameters load from `config/config.yaml`. Precedence is **defaults (Pydantic model) → YAML → environment variables** — a small set of operational knobs (`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `API_HOST`, `API_PORT`, `API_DEBUG`, `LOG_LEVEL`) can be overridden via env and win over YAML.

| Parameter | Default |
|---|---|
| `kmeans.n_clusters` | 8 |
| `kmeans.max_iter` | 300 |
| `kmeans.random_state` | 42 |
| `dbscan.eps` | 0.3 |
| `dbscan.min_samples` | 5 |
| `hdbscan.min_cluster_size` | 10 |
| `hdbscan.min_samples` | 5 |
| `text_features.max_features` | 1000 |
| `text_features.ngram_range` | [1, 2] |
| `temporal_features.time_windows` | [1, 5, 15, 60] |
| `behavioral_features.frequency_threshold` | 0.01 |
| `realtime.batch_size` | 100 |
| `realtime.update_interval` | 30 |
| `realtime.max_clusters` | 50 |
| `redis.host` | localhost |
| `redis.port` | 6379 |
| `redis.db` | 0 |
| `api.host` | 0.0.0.0 |
| `api.port` | 8000 |
| `api.debug` | true |

---

## Dashboard

The React/Vite dashboard (`make ui`, http://localhost:8080) subscribes to `/ws/stream` and renders:

- **Live stat cards** — throughput, total clusters, patterns discovered, anomalies.
- **Cluster scatter plot** — 2-D PCA projection with per-algorithm tabs (`kmeans` / `dbscan` / `hdbscan`).
- **Pattern-evolution timeline** — how pattern groups grow and shift over time.
- **Cluster-quality tiles** — silhouette, coherence, Davies-Bouldin.
- **Live anomaly-alerts feed** — consensus anomalies as they're detected.
- **Cluster drill-down** — pick a cluster to see its template and member examples.
- **Ingest form** — push a log in by hand and watch it land in a cluster.
- **Discovered Patterns** — temporal / performance / behavioral / sequence tabs from the batch miners.

---

## Success Criteria

Restating `project_requirements.md` §5, with measured numbers from load/perf tests in Docker. *Met* items were verified; *design target* items are honest about what was not load-proven.

**Functional**

- ✅ **Three algorithms process logs concurrently** (KMeans, DBSCAN, HDBSCAN).
- ✅ **Real-time throughput ≥ 1000 logs/s** — measured **~4810 logs/s** under load.
- ✅ **Pattern discovery** automatically identifies new log patterns (new-cluster detection + batch miners).
- ✅ **Anomaly detection** flags unusual logs (consensus ≥2/3) and streams them within the broadcast cycle (well under the 30s target).
- ✅ **Interactive dashboard** with live cluster exploration, scatter, drill-down, and ingest.

**Performance**

- ✅ **Processing latency < 10 ms/log** — engine amortized **~0.67 ms/log**; single-request **p95 ~3 ms** (gate < 150 ms).
- ✅ **Throughput ≥ 1000 logs/s under load** — **~4810 logs/s** confirmed.
- 🎯 **Memory < 500 MB / 100k logs** — *design target* (predict-only hot path + bounded buffers); not separately load-proven at 100k.
- 🎯 **Linear scaling with volume** — *design intent* (batch path is per-log work); not formally benchmarked across volumes.

**Quality**

- ✅ **Intra-cluster coherence > 80%** — typically **~0.86**.
- ☑️ **Meaningful discovery / accuracy** — sequence anomaly detection scores **100%** on a balanced labeled test set (≥95% required); silhouette is modest (~0.21) and Davies-Bouldin ~1.5 on noisy log features (see notes below).
- ☑️ **Anomaly false-positive rate < 5%** — consensus voting is designed to suppress false positives; not measured as a single FP-rate number on a labeled stream.
- ✅ **Model stability across restarts** — fixed `random_state` + deterministic warm-up give consistent results.

**Verification:** ~232 unit + integration tests pass in Docker; E2E and load gates pass black-box in Docker; the dashboard was verified in Chrome.

---

## What I Learned

- **Streaming clustering = warm-up → predict → periodic refit.** Fitting TF-IDF, the scaler, and all three models once on a historical batch and then freezing them makes the hot path pure prediction (~0.67 ms/log). A background sliding-window refit every 30s tracks drift without blocking ingest.
- **"Always assign" vs. "noise" is a real design split.** KMeans always returns a cluster; DBSCAN and HDBSCAN return `-1` (noise) for points they can't place densely. That makes density methods natural outlier detectors but means they can refuse to cluster a lot of a live stream.
- **The curse of dimensionality bit DBSCAN hard.** The spec default `dbscan.eps = 0.3` is *very* tight in the ~1029-dimensional feature space, so DBSCAN flags **most** streamed points as noise. It's a textbook lesson: a Euclidean radius that's sensible in low dimensions is tiny once you have ~1000 TF-IDF + feature dims. **KMeans and HDBSCAN give the richer, more useful cluster views** in practice; DBSCAN mostly contributes a conservative noise signal.
- **Masking + TF-IDF normalizes variable tokens.** Replacing timestamps / IPs / UUIDs / hex / numbers / URLs with placeholders before vectorizing makes otherwise-identical events collapse onto the same template, which is what lets clusters form cleanly out of noisy text.
- **Consensus cuts false positives.** Requiring ≥2 of 3 algorithms to agree before raising an anomaly is far less jumpy than trusting any single detector — especially given DBSCAN's noise-happiness above.
- **Unsupervised anomaly = statistically rare, not necessarily "bad."** The engine flags *novel/rare* logs; rare ≠ malicious. Some flagged points are just unusual-but-fine events. Interpreting them still needs a human (or the downstream classifier).
- **Sequence detection is strong on planted bursts, quiet on the live stream.** The sliding-window n-gram detector hits **100%** on a balanced labeled set, but because the warm-up corpus (grouped per service) is mostly-normal, few live sequence anomalies actually surface — accuracy on a curated test set and yield on a real stream are different things.

# real-time-log-pattern-clustering

An engine that **discovers hidden patterns in streaming log data** using multiple **unsupervised clustering algorithms** and surfaces them through a **live web dashboard**. As logs flow in, the engine vectorizes them, clusters them on the fly, and shows the emerging pattern groups, their sizes, representative examples, and outliers — in real time.

> **Status:** scaffold only. This commit contains just the `README.md`, `requirements.txt`, and `.gitignore`. No application code, tests, or Docker files have been written yet.

---

## What It Does

- **Ingests streaming log text** over HTTP (single, batch, or streaming) and groups semantically/structurally similar entries into **clusters** — without any labels.
- **Runs multiple unsupervised algorithms** so different pattern shapes are caught:
  - **KMeans / MiniBatchKMeans** — fast, centroid-based partitioning for incremental streams.
  - **DBSCAN** — density-based grouping that naturally flags sparse **outliers** (noise points) as anomalies.
  - **HDBSCAN** — hierarchical density clustering that finds variable-density pattern groups without a fixed cluster count.
- **Surfaces each discovered pattern** with a representative/template log line, member count, and a sampling of examples.
- **Flags anomalies** — points that no algorithm can place in a dense cluster (DBSCAN/HDBSCAN noise) are surfaced as rare/novel events.
- **Scores cluster quality** with internal metrics (e.g. silhouette) so the dashboard can show how well the current partition separates.
- **Streams results live** to a dashboard over a WebSocket so you can watch clusters form, grow, and split as logs arrive.
- **One-shot demo mode** — a batch run over a generated/sample corpus that clusters everything once and prints a summary, for a quick non-server demonstration.

---

## How It Runs

Two modes:

1. **Server with API (primary)** — a long-lived **FastAPI** process exposing a **web dashboard on port 8000** with **WebSocket** connections for live updates. Logs are pushed in via HTTP and clustering results stream out to the browser.
2. **One-shot demo mode** — a single batch pass over a sample corpus that runs all clustering algorithms once, prints/serves a summary of the discovered patterns, and exits. Useful for demos and CI smoke checks.

---

## Tech Stack

- **Language:** Python 3.11
- **Web framework:** FastAPI + Uvicorn (REST + async streaming + WebSocket), dashboard on port **8000**
- **Clustering / ML:** scikit-learn (KMeans, MiniBatchKMeans, DBSCAN, TF-IDF, silhouette/quality metrics), **HDBSCAN**, NumPy / SciPy / pandas; NLTK for tokenization & stopword removal
- **Feature extraction:** log preprocessing (mask timestamps / IPs / UUIDs / numbers) → TF-IDF vectorization → clustering
- **Validation / config:** Pydantic v2 + a config layer (defaults → YAML → env)
- **Testing:** pytest, pytest-asyncio, plus black-box E2E / load scripts (run in Docker)

> See `requirements.txt` for pinned versions.

---

## Planned Project Layout

```
real-time-log-pattern-clustering/
├── README.md
├── requirements.txt
├── .gitignore
├── src/                # FastAPI app, clustering engine, feature pipeline, metrics  (to be built)
├── frontend/           # live dashboard (to be built)
├── tests/              # unit + integration tests (to be built)
├── scripts/            # E2E / load / demo scripts (to be built)
├── config/             # config.yaml (to be built)
├── data/               # sample corpus + generated logs (gitignored)
└── models/             # fitted clusterers / vectorizers (gitignored)
```

*(Docker, Makefile, and all code are intentionally not present yet — they will be added once the build is approved.)*

---

## How to Run

> Run instructions (Docker-first, `make` targets, endpoints, and dashboard URL) will be filled in once the implementation lands. Planned entry points:
> - **Server:** dashboard + API at `http://localhost:8000` (`/docs` for OpenAPI).
> - **Demo:** a one-shot batch clustering run over the sample corpus.

---

## What I Learned

*(To be written as the project is built — notes on unsupervised log clustering, choosing between centroid- vs density-based algorithms for streaming data, online/incremental clustering trade-offs, and feature engineering for noisy log text.)*

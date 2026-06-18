# ml-log-classifier

A streaming machine-learning pipeline that classifies raw log entries by **severity** and **category** in real time using an **ensemble of text-classification models**. It ships with an HTTP API (FastAPI) backing a web dashboard, on-demand batch training, a long-lived streaming inference service, and a Docker Compose deployment.

---

## What It Does

- **Ingests raw log lines** (unstructured text) over HTTP and/or a streaming endpoint.
- **Classifies each entry** along two axes:
  - **Severity** — e.g. `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`.
  - **Category** — e.g. `auth`, `network`, `database`, `application`, `system`.
- **Uses an ensemble** of text classifiers — **Multinomial Naive Bayes + Random Forest + Gradient Boosting** — combined with **soft voting and confidence weighting** for a robust prediction plus a confidence score. Features are **TF-IDF (1–2 grams)** plus **temporal** and **metadata** signals, combined into a single fixed-length matrix via a scikit-learn `ColumnTransformer`.
- **Streams predictions in real time** to a web dashboard so you can watch classifications as logs flow in.
- **Trains on demand** — kick off a batch training run from the API/dashboard against a labelled dataset; the freshly trained ensemble is hot-swapped into the running inference service.

---

## How It Runs

| Mode | Description |
|------|-------------|
| **API server** | FastAPI app exposing REST + WebSocket endpoints and serving the web dashboard. |
| **Training** | Triggered **on demand** (batch). Reads a labelled dataset, fits the ensemble, persists the model artifacts. |
| **Inference** | Runs as a **long-lived streaming service** — consumes incoming log entries and emits live classifications. |
| **Deployment** | Packaged for **Docker Compose** so the API, inference worker, and dashboard come up together. |

---

## Tech Stack

- **Language:** Python 3.11
- **Web framework:** FastAPI + Uvicorn (REST & WebSocket)
- **ML:** scikit-learn (TF-IDF + soft-voting ensemble of MultinomialNB / RandomForest / GradientBoosting), NumPy, SciPy, pandas; NLTK for tokenization + stopwords
- **Model persistence:** joblib / pickle
- **Dashboard:** React + Vite, built to static assets and served by nginx; live updates over the FastAPI WebSocket
- **Validation / config:** Pydantic v2 + a dataclass-based config (defaults → YAML → env)
- **Testing:** pytest, pytest-asyncio
- **Deployment:** Docker + Docker Compose

> Note: the project scaffold and infrastructure now exist — `requirements.txt`, `src/config.py` + `config/config.yaml` (defaults → YAML → env), `Dockerfile`, `Dockerfile.test`, `docker-compose.yml`, `Makefile`, and the test harness. The ML modules, API, and React dashboard are added in subsequent commits.

---

## Planned Architecture (high level)

```
          ┌──────────────┐      raw logs      ┌────────────────────┐
clients ─▶│  FastAPI API │ ─────────────────▶ │  Streaming Inference│
          │  + Dashboard │ ◀───────────────── │  (ensemble predict) │
          └──────┬───────┘   live predictions └─────────┬──────────┘
                 │                                       │
                 │ trigger batch training                │ loads
                 ▼                                       ▼
          ┌──────────────┐                       ┌────────────────┐
          │   Trainer    │ ───── persists ─────▶ │  Model artifacts│
          │  (batch job) │                       │  (joblib)       │
          └──────────────┘                       └────────────────┘
```

---

## How to Run

The Docker/test harness is in place (`Dockerfile`, `Dockerfile.test`, `docker-compose.yml`, `Makefile`).
Common targets: `make build`, `make up` (app on http://localhost:8000), `make test` (suite in Docker),
`make down`. Full step-by-step run instructions are filled in once the API and dashboard land.

<!-- filled in later: end-to-end run + dashboard walkthrough -->

---

## API / Usage (planned)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/classify` | Classify a single log entry, returns severity + category + confidence. |
| `POST` | `/classify/batch` | Classify a batch of log entries. |
| `WS`   | `/stream` | Stream log entries in and receive live classifications. |
| `POST` | `/train` | Trigger an on-demand batch training run. |
| `GET`  | `/model/status` | Current model version, metrics, last trained time. |
| `GET`  | `/` | Web dashboard. |

_(Exact contracts will be finalized during implementation.)_

---

## What I Learned

<!-- Fill in as the project evolves: ensemble voting strategies, real-time streaming with
     WebSockets in FastAPI, hot-swapping models without downtime, etc. -->

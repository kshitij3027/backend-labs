# Log Forecast Engine

Forecasts future system metrics — **response time, error rate, and throughput** — from log-derived time series using a **four-model ensemble** (ARIMA + Exponential Smoothing + Linear + XGBoost), and ships every prediction with a **confidence score**, **prediction intervals**, a **per-model contribution breakdown**, and an **alert tier**. Exposed through a REST API and a live React + Recharts dashboard, with periodic forecasting and retraining running independently of request traffic. Multi-service Docker Compose.

---

## What It Does

The engine turns a stream of log-derived metric observations into forward-looking forecasts:

1. **Ingest** — metric points (`metric_name`, `value`, `timestamp`) are validated and persisted to Postgres (`POST /metrics`). A synthetic generator can seed realistic series for a quick start.
2. **Feature engineering** — from the recent window the engine derives rate-of-change/derivatives, moving averages, lag features, seasonal/pattern indicators, a data-quality score, and a pattern-stability score.
3. **Ensemble forecast** — four complementary models run over those features:
   - **ARIMA** and **Exponential Smoothing** (Holt-Winters) — statistical trend + seasonality.
   - **Linear regression** and **XGBoost** — ML regressors over the engineered lag/rolling features.

   Their forecasts are blended by a **weighted average** with recursive multi-step prediction over the horizon.
4. **Confidence + alert tier** — a scalar confidence blends *model agreement* (inter-model spread), data quality, and pattern stability. It maps to an **alert tier**: `high` (> 85%), `medium` (65–85%), `low` (< 65%). Each step also carries a prediction interval (`lower` / `upper`).
5. **Cache + persist** — scheduled forecasts are cached in Redis and persisted to Postgres so the read API stays fast.
6. **Serve** — the REST API serves the latest forecast, custom-horizon on-demand forecasts, forecast history, model roster, and app metrics; the dashboard polls and visualizes them.

Two background loops keep the system fresh without touching the request path:

- **Periodic forecasting** (Celery Beat, every 5 min by default) regenerates and caches forecasts.
- **Periodic retraining** (every 6 hours by default) refits the ensemble on a sliding training window.

A **validation feedback loop** scores past forecasts against actuals and feeds **dynamic model weights** (better-performing members earn more weight); models below an accuracy threshold are not deployed. Weights, confidence thresholds, and alert settings are **runtime-configurable** via `PUT /config` (or the dashboard) — **no restart required**.

> **Lightweight by design:** LSTM and Prophet were intentionally dropped in favour of the four lighter models above, keeping the image small and CPU-only without a heavy deep-learning runtime.

---

## Architecture

Multi-service topology orchestrated by Docker Compose. The forecasting and retraining loops run independently of request traffic, so the API stays responsive while models stay fresh.

| Service | Role |
|---|---|
| **api** | FastAPI/Uvicorn REST service on `:8000`. Serves forecasts, confidence, model metadata, config, health, and app metrics; accepts metric ingestion. Applies Alembic migrations on start. |
| **worker** | Celery worker. Runs the forecast/retrain tasks dispatched by Beat (shares the API image; same entrypoint). |
| **beat** | Celery Beat scheduler. Emits periodic forecast + retrain tasks on the configured cadence (only one instance runs). |
| **postgres** | PostgreSQL 16 — durable store for metrics, forecasts, accuracy history, and model metadata. |
| **redis** | Redis 7 — prediction cache **and** Celery broker/backend. |
| **dashboard** | React + Vite + Recharts SPA built multi-stage and served by **nginx**, on `:8080`. nginx reverse-proxies `/api` → `api:8000`, so the browser never talks to the backend directly (no CORS, no hardcoded host). |

Profile-gated helper services never start on a bare `docker compose up`: **test** (pytest), **e2e** (black-box end-to-end verifier), and **loadtest** (latency + throughput + memory gates).

```
                       ┌──────── Celery Beat (every 5 min / 6 hr) ────────┐
                       │  forecast tasks            retrain tasks         │
                       ▼                                                  ▼
POST /metrics ─► ingest ─► [Postgres] ─► features ─► ensemble ─► confidence + alert tier
 (log-derived)             (history)    (lags, MAs,  ARIMA       (model agreement +
                                         seasonal,   ExpSmooth    data quality +
                                         quality)    Linear       pattern stability)
                                                     XGBoost            │
                                                       │                ▼
                              validation feedback ◄────┤        cache (Redis) + persist (Postgres)
                              (dynamic weights,        │                │
                               deploy gate)            ▼                ▼
                                                   GET /models      REST API (:8000)
                                                                        │
                                                   nginx /api ◄─── React + Recharts
                                                   (Docker DNS          dashboard (:8080)
                                                    round-robin)
```

**Key `src/` modules**

| Module | Responsibility |
|---|---|
| `config.py` | Pydantic settings + layered config (defaults → `config/config.yaml` → env). |
| `schemas.py` | Data contracts (`ForecastResponse`, `ModelInfo`, `HealthResponse`, `ConfigResponse`, ingest/query models). |
| `ingestion.py` / `generator.py` | Metric ingestion + validation; synthetic series generator (seed data). |
| `features.py` | Derivatives, moving averages, lags, seasonal/quality + pattern-stability scoring. |
| `models/` | Per-model forecasters (ARIMA, exponential smoothing, linear, XGBoost) with graceful per-model failure. |
| `ensemble.py` | Weighted blend, confidence, alert tiers, graceful degradation, multi-window. |
| `prediction_service.py` | Orchestrates feature → models → ensemble → cache/DB; produces the canonical forecast payload. |
| `feedback.py` / `validation.py` | Validation loop, dynamic weights, accuracy-threshold deploy gate. |
| `runtime_config.py` | Process-local runtime overrides for weights/thresholds/alerts (backs `/config`). |
| `celery_app.py` / `tasks.py` | Celery app + periodic forecast/retrain tasks. |
| `observability.py` | Structured logging + Prometheus metrics/middleware. |
| `routers/` | FastAPI routes: `system`, `forecast`, `config`, `metrics`. |

`main.py` / `src.api:app` is the Uvicorn entrypoint; `frontend/` holds the React + Vite + Recharts dashboard and its nginx config.

---

## Tech Stack

- **Language:** Python 3.11
- **API:** FastAPI + Uvicorn
- **Background work:** Celery + Celery Beat (Redis broker/backend)
- **Cache / broker:** Redis 7
- **Persistence:** PostgreSQL 16 via SQLAlchemy (Alembic migrations)
- **Forecasting:** statsmodels (ARIMA, Holt-Winters), scikit-learn (linear), XGBoost
- **Data:** pandas, NumPy
- **Observability:** prometheus-client, structlog
- **Frontend:** React + Vite + Recharts, served by nginx
- **Orchestration:** Docker + Docker Compose

> **LSTM and Prophet are intentionally not used** — the ensemble is kept to four lightweight, CPU-only models for a small footprint. See `requirements.txt` for pinned versions.

---

## How to Run

Docker-first. All commands run from this folder.

```bash
make ui       # api + React dashboard  → http://localhost:8080  (API: http://localhost:8000)
make up       # api + worker + beat (full runtime, detached)  → http://localhost:8000
make up-all   # alias for `up`
make seed     # generate + ingest a synthetic metric dataset in Docker
make logs     # tail the api logs
make down     # stop the stack
make clean    # down + remove volumes/orphans

make migrate  # apply Alembic migrations explicitly (the api also runs them on start)
make worker   # start the Celery worker only
make beat     # start the Celery Beat scheduler only (only ONE instance)
make dashboard# build + start the dashboard only

make test       # full pytest suite in Docker (rebuilds first)
make test-unit  # unit tests only, in Docker
make test-int   # integration tests only, in Docker
make e2e        # black-box end-to-end verifier in Docker (against the live api)
make load       # latency + throughput + memory gates in Docker (against the live api)

make scale N=3  # scale the api behind the dashboard's nginx (Docker DNS round-robin)
```

**Quick start:**

```bash
make ui                       # bring up api + dashboard
make seed                     # load a synthetic dataset (override: make seed ARGS="--days 3 --metric response_time")
open http://localhost:8080    # watch forecasts + confidence bands in the dashboard
```

Ingest your own data and ask for a forecast directly:

```bash
# Ingest a couple of points
curl -s -X POST http://localhost:8000/metrics \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"metric_name":"response_time","value":120.5,"timestamp":"2026-06-28T10:00:00Z"}]}'

# Latest ensemble forecast for the metric
curl -s "http://localhost:8000/predictions?metric=response_time"

# Custom-horizon forecast (e.g. 12 steps ahead), computed on demand
curl -s "http://localhost:8000/forecast/12?metric=response_time"
```

**Ports:** the API (`8000`) and dashboard (`8080`) host ports can be overridden via `API_PORT` and `DASHBOARD_PORT` if those are taken (e.g. `DASHBOARD_PORT=9090 make ui`). Migrations run automatically when the api starts; `make migrate` applies them explicitly.

**Horizontal scaling (`make scale N=3`):** the dashboard's nginx reverse-proxies `/api` to the `api` service *by name*, re-resolving via Docker's embedded DNS per request. When the api is scaled to N replicas, Docker DNS returns all N IPs and nginx round-robins across them — no separate load-balancer container needed. Clients reach the load-balanced API through the dashboard (`http://localhost:8080/api/health`).

---

## API

Base URL `http://localhost:8000`. Interactive OpenAPI at `/docs`.

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/health` | Model status, Redis + DB connectivity, performance snapshot (HTTP 200 always; `status` = `ok`/`degraded`). |
| `GET`  | `/metrics` | Application metrics **JSON** — prediction accuracy, processing times, resource usage, counts. |
| `GET`  | `/metrics/prometheus` | Prometheus **text** exposition. |
| `POST` | `/metrics` | Ingest a batch of metric points (`{"points": [...]}`). |
| `GET`  | `/metrics/{metric_name}` | Read recent stored points for one metric (`limit`, `since`). |
| `GET`  | `/predictions` | Latest ensemble forecast for a metric (cache → DB), with confidence + breakdowns. |
| `GET`  | `/forecast/{steps}` | Custom-horizon forecast (`1..288` steps), **computed on demand** (not persisted/cached). |
| `GET`  | `/forecast/{metric}/history` | Past forecasts for a metric + recent per-model accuracy. |
| `GET`  | `/models` | Ensemble roster: members, weights, accuracy, deploy flags. |
| `POST` | `/retrain` | Trigger an out-of-band retrain (async via Celery, else in-process background task); returns 202. |
| `GET`  | `/config` | Current runtime config — weights, confidence thresholds, alert settings + static context. |
| `PUT`  | `/config` | Update weights / thresholds / alert settings **without restart** (validated; 422 on invalid). |

**Sample `GET /predictions` / `GET /forecast/{steps}` response** (`ForecastResponse`, matching the spec §8 shape; arrays are parallel and `horizon_steps` long):

```json
{
  "metric_name": "response_time",
  "timestamp": "2026-06-28T10:05:00Z",
  "forecast_horizon_minutes": 60,
  "horizon_steps": 12,
  "step_timestamps": ["2026-06-28T10:10:00Z", "2026-06-28T10:15:00Z", "..."],
  "ensemble_prediction": [121.4, 123.0, 124.7, "..."],
  "ensemble_confidence": [0.91, 0.88, 0.85, "..."],
  "individual_forecasts": {
    "arima": [120.9, 122.5, 124.1, "..."],
    "exp_smoothing": [121.0, 122.8, 124.0, "..."],
    "linear": [122.1, 123.6, 125.2, "..."],
    "xgboost": [121.8, 123.2, 125.0, "..."]
  },
  "lower": [115.2, 116.0, 117.3, "..."],
  "upper": [127.6, 130.0, 132.1, "..."],
  "alert_level": "high",
  "confidence": 0.88,
  "weights_used": {"arima": 0.3, "exp_smoothing": 0.2, "linear": 0.2, "xgboost": 0.3},
  "failed_models": [],
  "cached": false,
  "note": null
}
```

If a model fails, its name appears in `failed_models`, its forecast is dropped, and the remaining weights are renormalized (graceful degradation). On insufficient data the service returns a degraded `200` with an explanatory `note`.

---

## Configuration

Layered config: **Pydantic defaults → `config/config.yaml` → environment variables** (env wins). Copy `.env.example` to `.env` to override.

| Setting | Env var | Default |
|---|---|---|
| API host port | `API_PORT` | `8000` |
| Dashboard host port | `DASHBOARD_PORT` | `8080` |
| Database URL | `DATABASE_URL` | `postgresql+psycopg2://forecast:forecast@postgres:5432/forecast` |
| Redis URL | `REDIS_URL` | `redis://redis:6379/0` |
| Forecasting cadence (min) | `PREDICTION_INTERVAL_MIN` | `5` |
| Retrain cadence (hr) | `RETRAIN_INTERVAL_HR` | `6` |
| Default forecast horizon (min) | `DEFAULT_HORIZON_MIN` | `60` |
| Custom-horizon bounds (steps) | `horizon_min_steps` / `horizon_max_steps` | `1` / `288` (= 5 min … 24 hr) |
| Training window (days) | `training_window_days` | `7` |
| High-confidence threshold | `HIGH_CONFIDENCE_THRESHOLD` | `0.85` |
| Medium-confidence threshold | `MEDIUM_CONFIDENCE_THRESHOLD` | `0.65` |
| Accuracy deploy gate | `ACCURACY_DEPLOY_THRESHOLD` | `0.6` |
| Ensemble weights (sum 1.0) | `weight_arima` / `weight_exp_smoothing` / `weight_linear` / `weight_xgboost` | `0.3` / `0.2` / `0.2` / `0.3` |
| Dashboard poll interval (sec) | `dashboard_poll_interval_sec` | `30` |

Model **weights, confidence thresholds, and alert settings** are also adjustable at runtime via `PUT /config` (or the dashboard) and take effect immediately — no restart. The validation feedback loop additionally adapts weights from recent accuracy over time.

---

## What I Learned

- **Ensembles need a confidence story, not just a number.** Deriving confidence from *model agreement* (inter-model spread) plus data-quality and pattern-stability signals makes the forecast self-aware: when the four models diverge, confidence drops and the alert tier reflects it.
- **Graceful degradation beats brittle accuracy.** Letting any single model fail, dropping it, and renormalizing the remaining weights keeps the engine producing a sensible forecast instead of 500-ing — far more useful in production than a fragile "all-or-nothing" ensemble.
- **A dynamic-weight feedback loop closes the ML loop cheaply.** Scoring past forecasts against actuals and nudging weights toward the better performers (with an accuracy deploy gate) gives continuous improvement without manual tuning.
- **Multi-service orchestration is mostly about boundaries.** FastAPI (serve), Celery worker + Beat (compute on a schedule), Redis (cache + broker), and Postgres (history) each own one job; keeping the forecast/retrain loops off the request path is what keeps the API fast.
- **nginx + Docker DNS = a free load balancer.** Reverse-proxying `/api` to a service *name* and re-resolving per request lets `--scale api=N` round-robin across replicas with zero extra infrastructure.
- **Config precedence matters.** A clean `defaults → YAML → env` order, plus a separate *runtime* override layer for the knobs the dashboard touches, avoids restarts for the common tuning cases while keeping deploy-time config declarative.
- **Lighter is often enough.** Dropping LSTM and Prophet for a four-model CPU-only ensemble kept the image small and the build fast without meaningfully hurting forecast quality for these metrics.
- **Docker-only testing keeps it honest.** Unit + integration in Docker, plus profile-gated black-box E2E and load gates against the live API, verify the real data flow (ingest → forecast → cache/DB → API → dashboard) rather than a host-only happy path.
</content>
</invoke>

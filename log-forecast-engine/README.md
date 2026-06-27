# Log Forecast Engine

Forecasts future system metrics — response times, error rates, and throughput — from historical log patterns using an **ensemble of time-series models**, exposing predictions and confidence scores through a REST API and a dashboard.

---

## What It Does

The engine ingests historical log-derived metrics, trains and periodically retrains an ensemble of forecasting models, and produces forward-looking predictions for key operational signals. Each prediction ships with a **confidence interval** and a **per-model contribution breakdown**, so consumers can see not just *what* is forecast but *how certain* the engine is and *why*.

Typical use cases:
- Anticipate latency degradation before it breaches SLOs.
- Predict error-rate spikes to pre-emptively scale or alert.
- Forecast throughput to drive capacity planning.

---

## How It Runs

A **multi-service architecture** orchestrated via Docker Compose:

| Service | Role |
| --- | --- |
| **API server** | Long-lived REST service. Serves forecasts, confidence scores, and model metadata; accepts metric ingestion. |
| **Forecast worker** | Background worker that runs periodic forecasting jobs on a schedule and writes predictions to storage. |
| **Retrain worker** | Background worker that periodically retrains the model ensemble on fresh historical data and updates model artifacts. |
| **Scheduler / broker** | Coordinates periodic jobs (scheduler) and queues work (message broker) for the workers. |
| **Frontend dashboard** | Separate web UI visualizing forecasts, confidence bands, and historical-vs-predicted comparisons. |
| **Database** | Persists historical metrics, forecasts, and model metadata. |

> The forecasting and retraining loops run **independently of request traffic**, so the API stays responsive while models stay fresh.

---

## Forecasting Approach

An **ensemble** combines complementary model families so no single model's blind spot dominates:

- **Statistical** — ARIMA / SARIMA and exponential smoothing for trend and seasonality.
- **Additive decomposition** — Prophet-style trend + seasonality + holiday effects.
- **Machine learning** — gradient-boosted and linear regressors over engineered lag/rolling features.

Individual forecasts are weighted and blended; the spread across models informs the reported **confidence score** and prediction intervals.

---

## Tech Stack

- **Language:** Python
- **API:** FastAPI + Uvicorn
- **Background work:** Celery + Redis, APScheduler for periodic triggers
- **Forecasting:** statsmodels, Prophet, scikit-learn, XGBoost
- **Data:** pandas, NumPy
- **Persistence:** PostgreSQL via SQLAlchemy (Alembic migrations)
- **Observability:** prometheus-client, structlog
- **Frontend:** separate dashboard service (web UI)
- **Orchestration:** Docker Compose

---

## How to Run

<!-- Filled in as development progresses. Will be Docker Compose based. -->

```bash
# (planned)
docker compose up --build
```

---

## API (Planned)

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/metrics` | Ingest historical metric data points. |
| `GET` | `/forecast/{metric}` | Get the latest forecast for a metric with confidence scores. |
| `GET` | `/forecast/{metric}/history` | Compare past forecasts against actuals. |
| `GET` | `/models` | List ensemble members, weights, and last-retrain timestamps. |
| `POST` | `/retrain` | Trigger an out-of-band retraining job. |
| `GET` | `/health` | Service health check. |

*Endpoints are indicative and will firm up during implementation.*

---

## What I Learned

<!-- Filled in as the project evolves. -->

---

## Status

🚧 **Scaffold only.** This commit contains the README, dependency manifest, and `.gitignore`. No application code or Docker configuration has been written yet.

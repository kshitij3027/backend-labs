"""Integration tests for the full C11 API surface (real Postgres + Redis).

Drives the FastAPI app through ``TestClient`` so the entire HTTP path is
exercised: routing, the ``/metrics`` path trio, runtime config, observability
middleware, and the forecast/predictions/models/retrain routes. These run
against the REAL Postgres + Redis supplied by the compose ``test`` profile
(``DATABASE_URL`` / ``REDIS_URL``).

Seeding strategy (kept modest to stay fast):
* Ingest ~5 days of ``response_time`` @ 300s directly via the repository bulk
  path (no HTTP per-point overhead).
* Generate + persist one forecast so ``/predictions`` and history have data.
* Upsert ``ModelMetadata`` for the four ensemble members so ``/models`` and the
  deployed-count have data.

Metric names are namespaced per module run with a unique suffix so reruns and
parallel rows never collide. Runtime config is process-local: within this one
TestClient process, ``PUT /config`` updates persist and are visible to ``GET``.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src import runtime_config
from src.api import create_app
from src.db import repository
from src.db.base import Base
from src.db.session import SessionLocal, get_engine
from src.prediction_service import generate_prediction

# Four ensemble members (src.config.Settings.model_weights keys).
MODEL_NAMES = ["arima", "exp_smoothing", "linear", "xgboost"]


@pytest.fixture(scope="module", autouse=True)
def db_schema() -> None:
    """Ensure the schema exists (alembic runs first; create_all is a fallback)."""
    Base.metadata.create_all(bind=get_engine())


@pytest.fixture(scope="module")
def metric_name() -> str:
    """A unique ``response_time`` metric name for this module run."""
    return f"response_time_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def seeded(metric_name: str) -> str:
    """Seed metrics + one persisted forecast + model metadata; return the metric.

    ~5 days @ 300s of a stable-with-noise series so the models can actually fit
    and produce a real (non-degraded) forecast.
    """
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        days = 5
        interval = 300
        n = (days * 24 * 3600) // interval  # 1440 points
        start = now - timedelta(seconds=interval * (n - 1))
        # A gently varying series: baseline + small diurnal-ish wobble + tiny noise.
        points = []
        for i in range(n):
            ts = start + timedelta(seconds=interval * i)
            val = 120.0 + 10.0 * math.sin(i / 50.0) + (i % 7) * 0.3
            points.append(
                {"metric_name": metric_name, "timestamp": ts, "value": float(val)}
            )
        repository.add_metrics_bulk(session, points)
        session.commit()

        # Persist one forecast so /predictions + history have data.
        generate_prediction(
            session, metric_name, horizon_minutes=60, persist=True, cache=True
        )

        # Seed ModelMetadata for the four members so /models has a roster.
        for idx, name in enumerate(MODEL_NAMES):
            repository.upsert_model_metadata(
                session,
                name,
                weight=0.25,
                accuracy=0.8 - idx * 0.05,
                is_deployed=True,
                last_trained_at=now,
            )
        session.commit()
    finally:
        session.close()
    return metric_name


@pytest.fixture(scope="module")
def client(seeded: str) -> TestClient:
    """A single TestClient for the module (runtime config is process-local)."""
    return TestClient(create_app())


# --------------------------------------------------------------------------- #
# 1. /health — always 200, degraded-safe
# --------------------------------------------------------------------------- #
def test_health_ok_shape(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] in {"ok", "degraded"}
    assert body["service"]
    assert body["version"]
    assert isinstance(body["deployed_models"], int)
    # DB is up under the test profile.
    assert body["subsystems"]["database"] is True
    assert isinstance(body["subsystems"]["redis"], bool)
    assert "rss_mb" in body["performance"]
    assert "uptime_seconds" in body["performance"]


# --------------------------------------------------------------------------- #
# 2. /metrics — application metrics JSON (does NOT collide with /metrics/{name})
# --------------------------------------------------------------------------- #
def test_app_metrics_json(client: TestClient) -> None:
    resp = client.get("/metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    for key in ("prediction_accuracy", "processing_times", "resource_usage", "counts"):
        assert key in body, f"missing {key}"
    assert "rss_mb" in body["resource_usage"]
    assert isinstance(body["counts"]["deployed_models"], int)


# --------------------------------------------------------------------------- #
# 3. /metrics/prometheus — text exposition with lfe_ metrics
# --------------------------------------------------------------------------- #
def test_prometheus_exposition(client: TestClient) -> None:
    # Make a couple of requests first so the counter has samples.
    client.get("/health")
    client.get("/metrics")
    resp = client.get("/metrics/prometheus")
    assert resp.status_code == 200, resp.text
    assert "text/plain" in resp.headers.get("content-type", "")
    text = resp.text
    assert "lfe_api_requests_total" in text


# --------------------------------------------------------------------------- #
# 4. Regression: POST /metrics + GET /metrics/{name} still work
# --------------------------------------------------------------------------- #
def test_metrics_ingest_readback_regression(client: TestClient) -> None:
    name = f"regress_{uuid.uuid4().hex[:8]}"
    ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
    resp = client.post(
        "/metrics",
        json={"points": [{"metric_name": name, "timestamp": ts.isoformat(), "value": 42.0}]},
    )
    assert resp.status_code == 201, resp.text
    got = client.get(f"/metrics/{name}")
    assert got.status_code == 200
    body = got.json()
    assert body["count"] == 1
    assert body["points"][0]["value"] == 42.0


# --------------------------------------------------------------------------- #
# 5. /predictions — ForecastResponse shape; 404 for unknown
# --------------------------------------------------------------------------- #
def test_predictions_latest(client: TestClient, seeded: str) -> None:
    resp = client.get("/predictions", params={"metric": seeded})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    for key in (
        "ensemble_prediction",
        "ensemble_confidence",
        "individual_forecasts",
        "alert_level",
        "confidence",
        "weights_used",
    ):
        assert key in body, f"missing {key}"
    assert isinstance(body["ensemble_prediction"], list)
    assert len(body["ensemble_prediction"]) > 0


def test_predictions_unknown_metric_404(client: TestClient) -> None:
    resp = client.get("/predictions", params={"metric": f"nope_{uuid.uuid4().hex[:8]}"})
    assert resp.status_code == 404


# --------------------------------------------------------------------------- #
# 6. /forecast/{steps} — on-demand compute + bounds 422
# --------------------------------------------------------------------------- #
def test_forecast_custom_horizon(client: TestClient, seeded: str) -> None:
    resp = client.get("/forecast/12", params={"metric": seeded})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["ensemble_prediction"]) == 12


@pytest.mark.parametrize("steps", [0, 289, 300])
def test_forecast_steps_out_of_range_422(client: TestClient, seeded: str, steps: int) -> None:
    resp = client.get(f"/forecast/{steps}", params={"metric": seeded})
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------- #
# 7. /forecast/{metric}/history — list of past forecasts (no mis-route)
# --------------------------------------------------------------------------- #
def test_forecast_history(client: TestClient, seeded: str) -> None:
    resp = client.get(f"/forecast/{seeded}/history", params={"limit": 5})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["metric_name"] == seeded
    assert body["count"] >= 1
    assert isinstance(body["items"], list)
    assert "recent_accuracy" in body
    # The string-metric history route must NOT be captured by /forecast/{int steps}.
    first = body["items"][0]
    for key in ("id", "created_at", "horizon_minutes", "alert_level"):
        assert key in first


# --------------------------------------------------------------------------- #
# 8. /models — ensemble roster
# --------------------------------------------------------------------------- #
def test_models_roster(client: TestClient, seeded: str) -> None:
    resp = client.get("/models")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    names = {m["model_name"] for m in body["models"]}
    for name in MODEL_NAMES:
        assert name in names, f"missing model {name}"
    one = next(m for m in body["models"] if m["model_name"] == "arima")
    assert "weight" in one and "accuracy" in one and "is_deployed" in one
    assert body["deployed_count"] >= 1


# --------------------------------------------------------------------------- #
# 9. POST /retrain — 202 Accepted (async or background fallback)
# --------------------------------------------------------------------------- #
def test_retrain_accepted(client: TestClient, seeded: str) -> None:
    resp = client.post("/retrain", params={"metric": seeded})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"]
    assert body["metric"] == seeded
    assert body["mode"] in {"async", "background"}


# --------------------------------------------------------------------------- #
# 10. /config GET + PUT — runtime config (validation + reflection)
# --------------------------------------------------------------------------- #
def test_config_get(client: TestClient) -> None:
    resp = client.get("/config")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "model_weights" in body
    assert "high_confidence_threshold" in body
    assert "medium_confidence_threshold" in body
    assert "horizon_max_steps" in body  # static context echoed


def test_config_put_thresholds_reflected(client: TestClient) -> None:
    runtime_config.reset()  # start from settings defaults
    resp = client.put(
        "/config",
        json={"high_confidence_threshold": 0.9, "medium_confidence_threshold": 0.5},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["high_confidence_threshold"] == 0.9
    assert body["medium_confidence_threshold"] == 0.5
    # GET reflects the update (process-local persistence).
    got = client.get("/config").json()
    assert got["high_confidence_threshold"] == 0.9
    assert got["medium_confidence_threshold"] == 0.5


@pytest.mark.parametrize(
    "bad_body",
    [
        {"high_confidence_threshold": 0.4, "medium_confidence_threshold": 0.6},  # high<medium
        {"high_confidence_threshold": 1.5},  # out of [0,1]
        {"model_weights": {"arima": -1.0}},  # negative weight
    ],
)
def test_config_put_invalid_422_unchanged(client: TestClient, bad_body: dict) -> None:
    runtime_config.reset()
    before = client.get("/config").json()
    resp = client.put("/config", json=bad_body)
    assert resp.status_code == 422, resp.text
    after = client.get("/config").json()
    assert after["high_confidence_threshold"] == before["high_confidence_threshold"]
    assert after["medium_confidence_threshold"] == before["medium_confidence_threshold"]
    assert after["model_weights"] == before["model_weights"]


def test_config_put_weights_reflected(client: TestClient) -> None:
    runtime_config.reset()
    new_weights = {"arima": 0.4, "exp_smoothing": 0.3, "linear": 0.2, "xgboost": 0.1}
    resp = client.put("/config", json={"model_weights": new_weights})
    assert resp.status_code == 200, resp.text
    got = client.get("/config").json()
    assert got["model_weights"] == new_weights


# --------------------------------------------------------------------------- #
# 11. Runtime config affects alerts — alert_level consistent with thresholds
# --------------------------------------------------------------------------- #
def test_runtime_thresholds_affect_alert_level(client: TestClient, seeded: str) -> None:
    runtime_config.reset()
    # Very high thresholds: confidence rarely exceeds them -> low/medium expected.
    client.put(
        "/config",
        json={"high_confidence_threshold": 0.99, "medium_confidence_threshold": 0.98},
    )
    resp = client.get("/forecast/6", params={"metric": seeded})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    conf = float(body["confidence"])
    level = body["alert_level"]
    # Derive the expected tier from the returned confidence + the new thresholds.
    if conf >= 0.99:
        expected = "high"
    elif conf >= 0.98:
        expected = "medium"
    else:
        expected = "low"
    assert level == expected, f"conf={conf} level={level} expected={expected}"
    runtime_config.reset()

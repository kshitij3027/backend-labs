"""Integration tests for the Celery task layer (C9).

These run against the REAL PostgreSQL and Redis services supplied by the compose
``test`` service. The task *bodies* are plain functions, so we call them
synchronously (no broker / worker required) and verify they:

* generate, persist (Postgres) and cache (Redis) forecasts,
* fan out across every known metric,
* (re)fit + validate models and upsert ``ModelMetadata`` (no duplicate rows on
  rerun),
* degrade gracefully on missing/empty metrics (never raise),

and that the Celery app + Beat schedule are configured correctly (pure config
asserts, no broker needed).

Per-test unique metric names keep reruns isolated. Compute is kept light: a
~5-day series at 5-minute spacing and a 60-minute horizon. Retrain validation
fits 4 models so it is the heaviest step — kept to 1-2 metrics per test.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from src.celery_app import celery_app
from src.clients import redis as redis_client
from src.db import repository as repo
from src.db.base import Base
from src.db.session import get_engine, get_session
from src.generator import generate_series
from src.tasks import (
    run_forecast,
    run_retrain,
    run_scheduled_forecasts,
    run_scheduled_retrain,
)

HORIZON_MIN = 60
INTERVAL_S = 300  # 5 minutes
DAYS = 5

# The four ensemble members retrain writes ModelMetadata for. ModelMetadata is
# keyed by model name globally (not per-metric), and other tests insert rows with
# arbitrary names, so assertions are scoped to these names rather than the whole
# table.
MODEL_NAMES = {"arima", "exp_smoothing", "linear", "xgboost"}


@pytest.fixture(scope="session", autouse=True)
def db_schema() -> None:
    """Ensure the schema exists (idempotent fallback to migrations)."""
    Base.metadata.create_all(bind=get_engine())


@pytest.fixture
def session() -> Session:
    with get_session() as s:
        yield s


@pytest.fixture
def unique() -> str:
    return uuid.uuid4().hex[:12]


@pytest.fixture(autouse=True)
def _reset_redis() -> None:
    redis_client.reset_client()
    yield
    redis_client.reset_client()


def _seed(
    session: Session,
    metric_name: str,
    *,
    profile: str = "response_time",
    days: int = DAYS,
    n_override: int | None = None,
) -> int:
    """Seed a deterministic synthetic series under ``metric_name``."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    pts = generate_series(profile, start, end, INTERVAL_S, seed=7)
    if n_override is not None:
        pts = pts[:n_override]
    rows = [
        {"metric_name": metric_name, "timestamp": p.timestamp, "value": p.value}
        for p in pts
    ]
    repo.add_metrics_bulk(session, rows, commit=True)
    return len(rows)


# --------------------------------------------------------------------------- #
# 1. run_forecast — persists + caches; degrades on too-little data
# --------------------------------------------------------------------------- #
def test_run_forecast_persists_and_caches(session: Session, unique: str) -> None:
    name = f"response_time_{unique}"
    n = _seed(session, name)
    assert n > 100

    summary = run_forecast(name, HORIZON_MIN)

    assert summary["metric_name"] == name
    assert "error" not in summary, summary
    assert summary["alert_level"] in {"high", "medium", "low"}
    assert summary["steps"] is not None and summary["steps"] > 0
    assert summary["persisted"] is True
    assert 0.0 <= summary["confidence"] <= 1.0
    assert isinstance(summary["failed_models"], list)

    # Forecast row persisted (committed in its own session by the task).
    row = repo.get_latest_forecast(session, name)
    assert row is not None
    assert row.metric_name == name
    assert int(row.horizon_minutes) == HORIZON_MIN

    # Prediction cached in Redis.
    cached = redis_client.get_cached_prediction(name, HORIZON_MIN)
    assert cached is not None
    assert cached["metric_name"] == name


def test_run_forecast_graceful_on_insufficient_data(
    session: Session, unique: str
) -> None:
    name = f"sparse_{unique}"
    _seed(session, name, n_override=2)  # below the model minimum
    summary = run_forecast(name, HORIZON_MIN)
    # Never raises; returns a summary (degraded forecast or error field).
    assert summary["metric_name"] == name
    assert ("error" in summary) or (summary.get("alert_level") in {"high", "medium", "low"})


def test_run_forecast_graceful_on_no_data(session: Session, unique: str) -> None:
    name = f"empty_{unique}"  # never seeded
    summary = run_forecast(name, HORIZON_MIN)
    assert summary["metric_name"] == name
    # Degraded (low) or an error field — but it must return, not raise.
    assert ("error" in summary) or (summary.get("alert_level") in {"high", "medium", "low"})


# --------------------------------------------------------------------------- #
# 2. run_scheduled_forecasts — covers every known metric
# --------------------------------------------------------------------------- #
def test_run_scheduled_forecasts_covers_all_metrics(
    session: Session, unique: str
) -> None:
    name_a = f"response_time_{unique}"
    name_b = f"throughput_{unique}"
    _seed(session, name_a, profile="response_time")
    _seed(session, name_b, profile="throughput")

    # list_metric_names returns the distinct seeded names (among others).
    names = repo.list_metric_names(session)
    assert name_a in names
    assert name_b in names

    result = run_scheduled_forecasts()
    assert result["count"] == len(result["metrics"])
    assert result["count"] >= 2

    covered = {s["metric_name"] for s in result["metrics"]}
    assert name_a in covered
    assert name_b in covered

    # A forecast was persisted for each of our seeded metrics.
    assert repo.get_latest_forecast(session, name_a) is not None
    assert repo.get_latest_forecast(session, name_b) is not None


# --------------------------------------------------------------------------- #
# 3. run_retrain — upserts ModelMetadata; idempotent on rerun
# --------------------------------------------------------------------------- #
def test_run_retrain_upserts_metadata_idempotent(
    session: Session, unique: str
) -> None:
    name = f"response_time_{unique}"
    _seed(session, name)

    result = run_retrain(name)
    assert result["metric_name"] == name
    assert "error" not in result, result
    assert isinstance(result["deployed"], list)
    assert isinstance(result["rejected"], list)
    assert isinstance(result["weights"], dict)

    # Deployed weights roughly sum to 1.0 (allow empty if nothing deployed).
    deployed = result["deployed"]
    if deployed:
        total = sum(float(result["weights"].get(m, 0.0)) for m in deployed)
        assert math.isclose(total, 1.0, rel_tol=0.05, abs_tol=0.05), result["weights"]

    # ModelMetadata rows upserted for the four ensemble members with expected
    # fields populated. (Scope to our model names: the table is shared across
    # tests/metrics and other tests insert rows with arbitrary names.)
    session.expire_all()
    ours = {m.model_name: m for m in repo.list_model_metadata(session)
            if m.model_name in MODEL_NAMES}
    assert ours, "expected ModelMetadata rows for the ensemble models after retrain"
    for m in ours.values():
        assert m.last_trained_at is not None
        assert isinstance(m.is_deployed, bool)
        assert m.weight is not None
        # Deployed models must carry an accuracy.
        if m.is_deployed:
            assert m.accuracy is not None

    first_trained_at = {n: m.last_trained_at for n, m in ours.items()}

    # Run again: upsert should UPDATE the same rows, not create duplicates.
    result2 = run_retrain(name)
    assert "error" not in result2, result2

    session.expire_all()
    all_meta2 = repo.list_model_metadata(session)
    # No duplicate model_name rows anywhere in the table.
    names_list = [m.model_name for m in all_meta2]
    assert len(names_list) == len(set(names_list)), names_list
    # The same ensemble-model rows are still present (no new ones created).
    ours2 = {m.model_name for m in all_meta2 if m.model_name in MODEL_NAMES}
    assert ours2 == set(ours.keys())
    # last_trained_at was refreshed (>= the first run's timestamp).
    by_name2 = {m.model_name: m for m in all_meta2 if m.model_name in MODEL_NAMES}
    for n, prev in first_trained_at.items():
        assert by_name2[n].last_trained_at >= prev


# --------------------------------------------------------------------------- #
# 4. run_scheduled_retrain — covers both metrics; metadata present
# --------------------------------------------------------------------------- #
def test_run_scheduled_retrain_covers_metrics(session: Session, unique: str) -> None:
    name_a = f"response_time_{unique}"
    name_b = f"throughput_{unique}"
    _seed(session, name_a, profile="response_time")
    _seed(session, name_b, profile="throughput")

    result = run_scheduled_retrain()
    assert result["count"] == len(result["metrics"])
    assert result["count"] >= 2
    covered = {s["metric_name"] for s in result["metrics"]}
    assert name_a in covered
    assert name_b in covered

    # ModelMetadata present after the scheduled retrain.
    assert repo.list_model_metadata(session)


# --------------------------------------------------------------------------- #
# 5. Graceful retrain on a nonexistent/empty metric (worker-safe)
# --------------------------------------------------------------------------- #
def test_run_retrain_graceful_on_missing_metric(
    session: Session, unique: str
) -> None:
    name = f"missing_{unique}"  # never seeded
    result = run_retrain(name)
    assert result["metric_name"] == name
    # Returns without raising: either an error field or an empty deploy list.
    if "error" in result:
        assert isinstance(result["error"], str)
    else:
        assert result["deployed"] == []


# --------------------------------------------------------------------------- #
# 6. Celery app + Beat schedule config (pure asserts — no broker)
# --------------------------------------------------------------------------- #
def test_celery_app_config() -> None:
    assert celery_app.main == "log_forecast"
    assert celery_app.conf.broker_url, "broker_url must be set"

    schedule = celery_app.conf.beat_schedule
    assert "forecast-all-metrics" in schedule
    assert "retrain-all-models" in schedule
    assert schedule["forecast-all-metrics"]["task"] == "tasks.run_scheduled_forecasts"
    assert schedule["retrain-all-models"]["task"] == "tasks.run_scheduled_retrain"

    # Tasks are registered under their declared names.
    assert "tasks.run_forecast" in celery_app.tasks
    assert "tasks.run_scheduled_forecasts" in celery_app.tasks
    assert "tasks.run_retrain" in celery_app.tasks
    assert "tasks.run_scheduled_retrain" in celery_app.tasks

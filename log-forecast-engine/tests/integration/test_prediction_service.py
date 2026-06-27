"""Integration tests for the prediction service (C8).

These run against the REAL PostgreSQL and Redis services supplied by the compose
``test`` service (``DATABASE_URL`` -> postgres, ``REDIS_URL`` -> redis). They
exercise the full forecast path end-to-end:

    seed metrics (Postgres) -> generate_prediction (fit 4 models + ensemble)
        -> persist Forecast (Postgres) + cache (Redis) -> ForecastResponse dict

and the fast read path (cache-first, Postgres fallback), insufficient-data
degradation, and graceful degradation when Redis is down.

Per-test unique metric names keep reruns isolated. Compute is kept light: a
~5-day series at 5-minute spacing and a 60-minute horizon (12 steps).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from src.clients import redis as redis_client
from src.db import repository as repo
from src.db.base import Base
from src.db.session import get_engine, get_session
from src.generator import generate_series
from src.prediction_service import generate_prediction, get_prediction
from src.schemas import ForecastResponse

HORIZON_MIN = 60
INTERVAL_S = 300  # 5 minutes -> 12 steps for a 60-min horizon
DAYS = 5


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
    """Reset the cached Redis client around each test (envs may be patched)."""
    redis_client.reset_client()
    yield
    redis_client.reset_client()


def _seed(session: Session, metric_name: str, *, days: int = DAYS, n_override=None) -> int:
    """Seed a synthetic ``response_time``-shaped series under ``metric_name``.

    Generates a deterministic series and rewrites the metric_name to the unique
    per-test name so rows don't collide across runs.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    pts = generate_series("response_time", start, end, INTERVAL_S, seed=7)
    if n_override is not None:
        pts = pts[:n_override]
    rows = [
        {"metric_name": metric_name, "timestamp": p.timestamp, "value": p.value}
        for p in pts
    ]
    repo.add_metrics_bulk(session, rows, commit=True)
    return len(rows)


# --------------------------------------------------------------------------- #
# Happy path: generate -> validate -> persist -> cache
# --------------------------------------------------------------------------- #
def test_generate_prediction_full_flow(session: Session, unique: str) -> None:
    name = f"response_time_{unique}"
    n = _seed(session, name)
    assert n > 100

    result = generate_prediction(session, name, horizon_minutes=HORIZON_MIN)

    # Validates against the public contract (must not raise).
    model = ForecastResponse(**result)
    assert model.metric_name == name

    steps = result["horizon_steps"]
    assert steps == len(result["ensemble_prediction"])
    assert len(result["ensemble_confidence"]) == steps
    assert len(result["step_timestamps"]) == steps
    assert len(result["lower"]) == steps
    assert len(result["upper"]) == steps

    # Survivors present and parallel to the ensemble.
    assert result["individual_forecasts"], "expected at least one surviving model"
    for vals in result["individual_forecasts"].values():
        assert len(vals) == steps

    assert result["alert_level"] in {"high", "medium", "low"}
    assert 0.0 <= result["confidence"] <= 1.0
    assert result["cached"] is False

    # Fully JSON-serialisable (it gets cached as JSON).
    json.dumps(result)

    # --- Persistence: a Forecast row was written ---
    row = repo.get_latest_forecast(session, name)
    assert row is not None
    assert row.metric_name == name
    assert int(row.horizon_minutes) == HORIZON_MIN
    assert int(row.horizon_steps) == steps
    assert row.alert_level == result["alert_level"]
    assert len(row.ensemble_prediction) == steps

    # --- Cache: horizon key + latest pointer both populated ---
    cached_horizon = redis_client.get_cached_prediction(name, HORIZON_MIN)
    assert cached_horizon is not None
    assert cached_horizon["ensemble_prediction"] == result["ensemble_prediction"]

    cached_latest = redis_client.get_cached_prediction(name)  # latest pointer
    assert cached_latest is not None
    assert cached_latest["ensemble_prediction"] == result["ensemble_prediction"]


# --------------------------------------------------------------------------- #
# get_prediction: cache-first, then DB fallback
# --------------------------------------------------------------------------- #
def test_get_prediction_cache_first_then_db_fallback(
    session: Session, unique: str
) -> None:
    name = f"response_time_{unique}"
    _seed(session, name)
    generate_prediction(session, name, horizon_minutes=HORIZON_MIN)

    # Cache hit -> cached=True.
    hit = get_prediction(session, name, horizon_minutes=HORIZON_MIN)
    assert hit is not None
    assert hit["cached"] is True

    # Drop the cache keys, forcing the DB fallback path.
    client = redis_client.get_redis()
    assert client is not None
    client.delete(f"forecast:{name}:{HORIZON_MIN}")
    client.delete(f"forecast:{name}:latest")

    miss = get_prediction(session, name, horizon_minutes=HORIZON_MIN)
    assert miss is not None
    assert miss["cached"] is False  # served from Postgres
    assert miss["metric_name"] == name
    assert len(miss["ensemble_prediction"]) == miss["horizon_steps"]
    # Still validates as a ForecastResponse.
    ForecastResponse(**miss)


def test_get_prediction_none_when_nothing(session: Session, unique: str) -> None:
    name = f"missing_{unique}"
    assert get_prediction(session, name, horizon_minutes=HORIZON_MIN) is None


# --------------------------------------------------------------------------- #
# Insufficient data -> degraded result (no crash)
# --------------------------------------------------------------------------- #
def test_generate_prediction_insufficient_data(session: Session, unique: str) -> None:
    name = f"sparse_{unique}"
    # Fewer than the 4-point minimum.
    _seed(session, name, n_override=2)

    result = generate_prediction(session, name, horizon_minutes=HORIZON_MIN)

    assert result["alert_level"] == "low"
    assert result["note"], "degraded result must carry an explanatory note"
    assert result["confidence"] == 0.0
    # Lengths remain self-consistent and it still validates.
    assert len(result["ensemble_prediction"]) == len(result["ensemble_confidence"])
    ForecastResponse(**result)


def test_generate_prediction_no_data(session: Session, unique: str) -> None:
    name = f"empty_{unique}"  # never seeded
    result = generate_prediction(session, name, horizon_minutes=HORIZON_MIN)
    assert result["alert_level"] == "low"
    assert result["note"]
    ForecastResponse(**result)


# --------------------------------------------------------------------------- #
# Redis-down graceful: still computes + persists, cache no-ops
# --------------------------------------------------------------------------- #
def test_generate_prediction_redis_down_still_persists(
    session: Session, unique: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.config import get_settings

    name = f"response_time_{unique}"
    _seed(session, name)

    # Point Redis at an unreachable host for the duration of this test. We patch
    # the resolved settings object (not the env var) because the config loader
    # currently lets YAML win over the environment (see implementation-bug note).
    settings = get_settings()
    monkeypatch.setattr(settings, "redis_url", "redis://192.0.2.1:6390/0")
    redis_client.reset_client()
    try:
        result = generate_prediction(session, name, horizon_minutes=HORIZON_MIN)
        # Compute + response still succeed.
        ForecastResponse(**result)
        assert len(result["ensemble_prediction"]) == result["horizon_steps"]
        # Persistence still happened despite Redis being down.
        row = repo.get_latest_forecast(session, name)
        assert row is not None
        assert row.metric_name == name
        # Cache genuinely no-op'd.
        assert redis_client.get_cached_prediction(name, HORIZON_MIN) is None
    finally:
        redis_client.reset_client()


def test_generate_prediction_no_cache_no_persist(session: Session, unique: str) -> None:
    name = f"response_time_{unique}"
    _seed(session, name)
    result = generate_prediction(
        session, name, horizon_minutes=HORIZON_MIN, persist=False, cache=False
    )
    ForecastResponse(**result)
    # Nothing persisted, nothing cached.
    assert repo.get_latest_forecast(session, name) is None
    assert redis_client.get_cached_prediction(name, HORIZON_MIN) is None

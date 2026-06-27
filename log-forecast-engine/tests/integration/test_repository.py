"""Integration tests for the persistence layer (C1).

These run against the REAL PostgreSQL service (JSONB + tz-aware datetimes require
it — SQLite is not used). ``DATABASE_URL`` is supplied by the compose ``test``
service and points at the ``postgres`` service; the schema is expected to already
exist (the test invocation runs ``alembic upgrade head`` first), but the
``db_schema`` fixture below falls back to ``Base.metadata.create_all`` so the file
is runnable even if the migration step was skipped.

Each test uses a unique, per-test name (via the ``unique`` fixture) so reruns are
safe and tests don't interfere with one another or with leftover data.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.db.base import Base
from src.db.session import get_engine, get_session


@pytest.fixture(scope="session", autouse=True)
def db_schema() -> None:
    """Ensure the schema exists.

    The recommended invocation runs ``alembic upgrade head`` before pytest, so the
    tables are already present. ``create_all`` is idempotent and acts purely as a
    fallback if the migration step was skipped — it never drops or recreates an
    existing table.
    """
    Base.metadata.create_all(bind=get_engine())


@pytest.fixture
def session() -> Session:
    """Yield a real DB session bound to the Postgres service; always closed."""
    with get_session() as s:
        yield s


@pytest.fixture
def unique() -> str:
    """A short unique suffix to namespace rows per test (safe reruns/isolation)."""
    return uuid.uuid4().hex[:12]


# --------------------------------------------------------------------------- #
# Metric
# --------------------------------------------------------------------------- #
def test_add_metric_and_get_metrics(session: Session, unique: str) -> None:
    name = f"cpu_{unique}"
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    repo.add_metric(session, name, base, 1.0, commit=True)
    repo.add_metric(session, name, base + timedelta(minutes=5), 2.0, commit=True)
    repo.add_metric(session, name, base + timedelta(minutes=10), 3.0, commit=True)

    rows = repo.get_metrics(session, name)
    assert [r.value for r in rows] == [1.0, 2.0, 3.0]  # oldest-first ordering

    # tz-aware datetimes survive the round-trip.
    assert rows[0].timestamp.tzinfo is not None
    assert rows[0].timestamp == base

    # `since` filter.
    later = repo.get_metrics(session, name, since=base + timedelta(minutes=5))
    assert [r.value for r in later] == [2.0, 3.0]

    # `limit`.
    limited = repo.get_metrics(session, name, limit=2)
    assert [r.value for r in limited] == [1.0, 2.0]


def test_add_metrics_bulk_and_get_latest(session: Session, unique: str) -> None:
    name = f"mem_{unique}"
    base = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    points = [
        {"metric_name": name, "timestamp": base + timedelta(minutes=i), "value": float(i)}
        for i in range(5)
    ]
    created = repo.add_metrics_bulk(session, points, commit=True)
    assert len(created) == 5
    assert all(m.id is not None for m in created)  # PKs assigned

    # get_latest_metrics returns the newest n, but chronologically ordered.
    latest = repo.get_latest_metrics(session, name, n=3)
    assert [m.value for m in latest] == [2.0, 3.0, 4.0]
    assert latest[0].timestamp < latest[-1].timestamp


# --------------------------------------------------------------------------- #
# Forecast — JSONB fidelity
# --------------------------------------------------------------------------- #
def test_save_and_get_latest_forecast_jsonb_roundtrip(
    session: Session, unique: str
) -> None:
    name = f"reqs_{unique}"
    created_at = datetime(2026, 3, 1, 8, 30, tzinfo=timezone.utc)
    ensemble_prediction = [10.5, 11.25, 12.0]
    ensemble_confidence = [0.91, 0.88, 0.7]
    individual = {
        "arima": [10.0, 11.0, 12.0],
        "xgboost": [11.0, 11.5, 12.0],
    }
    step_ts = [
        (created_at + timedelta(minutes=5 * i)).isoformat() for i in range(1, 4)
    ]

    repo.save_forecast(
        session,
        metric_name=name,
        created_at=created_at,
        horizon_minutes=15,
        horizon_steps=3,
        ensemble_prediction=ensemble_prediction,
        ensemble_confidence=ensemble_confidence,
        individual_forecasts=individual,
        alert_level="warning",
        step_timestamps=step_ts,
        commit=True,
    )

    got = repo.get_latest_forecast(session, name)
    assert got is not None
    # JSONB arrays / objects round-trip with exact fidelity.
    assert got.ensemble_prediction == ensemble_prediction
    assert got.ensemble_confidence == ensemble_confidence
    assert got.individual_forecasts == individual
    assert got.individual_forecasts["arima"] == [10.0, 11.0, 12.0]
    assert got.step_timestamps == step_ts
    assert got.alert_level == "warning"
    assert got.horizon_minutes == 15
    assert got.created_at == created_at
    assert got.created_at.tzinfo is not None


def test_get_latest_forecast_and_history_ordering(
    session: Session, unique: str
) -> None:
    name = f"hist_{unique}"
    base = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    for i in range(3):
        repo.save_forecast(
            session,
            metric_name=name,
            created_at=base + timedelta(hours=i),
            horizon_minutes=5,
            horizon_steps=1,
            ensemble_prediction=[float(i)],
            ensemble_confidence=[0.5],
            individual_forecasts={"linear": [float(i)]},
            alert_level="normal",
            step_timestamps=[(base + timedelta(hours=i)).isoformat()],
            commit=True,
        )

    latest = repo.get_latest_forecast(session, name)
    assert latest is not None
    assert latest.ensemble_prediction == [2.0]  # the most recent created_at

    history = repo.get_forecast_history(session, name, limit=2)
    # newest-first, capped at limit.
    assert [f.ensemble_prediction for f in history] == [[2.0], [1.0]]


def test_get_latest_forecast_missing_returns_none(
    session: Session, unique: str
) -> None:
    assert repo.get_latest_forecast(session, f"nope_{unique}") is None


# --------------------------------------------------------------------------- #
# ModelMetadata — upsert + JSONB params
# --------------------------------------------------------------------------- #
def test_upsert_model_metadata_insert_then_update(
    session: Session, unique: str
) -> None:
    name = f"arima_{unique}"
    trained = datetime(2026, 5, 1, 6, 0, tzinfo=timezone.utc)

    created = repo.upsert_model_metadata(
        session,
        name,
        version=1,
        last_trained_at=trained,
        accuracy=0.72,
        weight=0.3,
        is_deployed=True,
        params={"order": [2, 1, 2], "seasonal": False},
        artifact_path="/app/models/arima.pkl",
        commit=True,
    )
    assert created.id is not None
    assert created.params == {"order": [2, 1, 2], "seasonal": False}
    assert created.last_trained_at == trained
    assert created.last_trained_at.tzinfo is not None

    # Upsert again -> update in place (same row id), only supplied fields change.
    updated = repo.upsert_model_metadata(
        session,
        name,
        accuracy=0.81,
        weight=0.4,
        params={"order": [3, 1, 1]},
        commit=True,
    )
    assert updated.id == created.id  # updated, not inserted
    assert updated.accuracy == 0.81
    assert updated.weight == 0.4
    assert updated.params == {"order": [3, 1, 1]}
    assert updated.version == 1  # untouched field preserved
    assert updated.is_deployed is True

    fetched = repo.get_model_metadata(session, name)
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.accuracy == 0.81


def test_list_model_metadata_ordered_by_name(session: Session, unique: str) -> None:
    names = [f"z_{unique}", f"a_{unique}", f"m_{unique}"]
    for n in names:
        repo.upsert_model_metadata(session, n, weight=0.1, commit=True)

    listed = repo.list_model_metadata(session)
    listed_names = [m.model_name for m in listed]
    # our names appear in sorted (ascending) order within the full list.
    ours = [n for n in listed_names if n.endswith(unique)]
    assert ours == sorted(names)


def test_get_model_metadata_missing_returns_none(
    session: Session, unique: str
) -> None:
    assert repo.get_model_metadata(session, f"absent_{unique}") is None


# --------------------------------------------------------------------------- #
# AccuracyRecord
# --------------------------------------------------------------------------- #
def test_add_and_get_recent_accuracy(session: Session, unique: str) -> None:
    model = f"xgb_{unique}"
    metric_a = f"cpu_{unique}"
    metric_b = f"mem_{unique}"
    base = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)

    # three records for metric_a, one for metric_b.
    for i in range(3):
        repo.add_accuracy_record(
            session,
            model_name=model,
            metric_name=metric_a,
            evaluated_at=base + timedelta(minutes=i),
            horizon_minutes=5,
            predicted_value=float(i),
            actual_value=float(i) + 0.5,
            absolute_error=0.5,
            percentage_error=10.0,
            commit=True,
        )
    repo.add_accuracy_record(
        session,
        model_name=model,
        metric_name=metric_b,
        evaluated_at=base + timedelta(minutes=10),
        horizon_minutes=5,
        predicted_value=99.0,
        commit=True,
    )

    # all records for the model, newest-first.
    recent = repo.get_recent_accuracy(session, model)
    assert len(recent) == 4
    assert recent[0].metric_name == metric_b  # latest evaluated_at
    assert recent[0].evaluated_at.tzinfo is not None
    # nullable fields survive as None.
    assert recent[0].actual_value is None
    assert recent[0].absolute_error is None

    # newest-first ordering across the metric_a records.
    a_records = repo.get_recent_accuracy(session, model, metric_name=metric_a)
    assert len(a_records) == 3
    assert [r.predicted_value for r in a_records] == [2.0, 1.0, 0.0]

    # limit is respected.
    limited = repo.get_recent_accuracy(session, model, limit=2)
    assert len(limited) == 2


# --------------------------------------------------------------------------- #
# Transaction convention: commit=False flushes (PK assigned) but doesn't persist
# across sessions until the caller commits.
# --------------------------------------------------------------------------- #
def test_flush_only_then_caller_commits(session: Session, unique: str) -> None:
    name = f"flush_{unique}"
    ts = datetime(2026, 6, 15, 0, 0, tzinfo=timezone.utc)

    m = repo.add_metric(session, name, ts, 42.0)  # commit=False -> flush only
    assert m.id is not None  # flush assigned the PK within the transaction
    session.commit()  # caller commits

    with get_session() as other:
        rows = repo.get_metrics(other, name)
    assert [r.value for r in rows] == [42.0]

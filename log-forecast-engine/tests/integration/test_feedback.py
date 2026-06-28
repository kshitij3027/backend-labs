"""Integration tests for the C10 feedback loop (``src.feedback`` + ``src.tasks``).

These run against the REAL PostgreSQL + Redis services (JSONB + tz-aware
datetimes require Postgres). They construct fully *controlled* forecast + actual
data so that per-model accuracy, dynamic weighting and the retrain trigger are
deterministic — no model fitting is needed to exercise the feedback math.

Pattern (mirrors ``test_repository.py``):
* a session-scoped ``db_schema`` fixture ensures tables exist (alembic runs first),
* a per-test ``session`` bound to the Postgres service,
* a ``unique`` suffix namespaces every metric/model so reruns + parallel rows
  never collide.

The retrain task is never actually invoked: ``maybe_trigger_retrain`` /
``run_feedback_cycle`` are driven with an INJECTED fake ``retrain_fn`` that just
records its calls, so the loop is fast and synchronous (no Celery worker/broker).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.db.base import Base
from src.db.session import get_engine, get_session
from src.feedback import (
    adjust_weights,
    evaluate_forecast_accuracy,
    maybe_trigger_retrain,
    recent_model_accuracy,
    run_feedback_cycle,
    should_retrain,
)
from src.tasks import run_feedback, run_scheduled_feedback

# The 4 configured ensemble members (src.config.Settings.model_weights).
MODELS = ["arima", "exp_smoothing", "linear", "xgboost"]


@pytest.fixture(scope="session", autouse=True)
def db_schema() -> None:
    """Ensure the schema exists (alembic runs first; create_all is the fallback)."""
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
# Helpers to construct controlled forecast + actual data
# --------------------------------------------------------------------------- #
def _seed_elapsed_forecast(
    session: Session,
    metric_name: str,
    *,
    steps: int = 6,
    interval_min: int = 5,
    actuals: list[float] | None = None,
):
    """Insert one fully-ELAPSED forecast + matching actual Metric rows.

    Returns ``(step_timestamps, actuals)``. The forecast is created in the past so
    its whole horizon has elapsed. One model ("arima") is PERFECT (predicts the
    actuals exactly); "xgboost" is BAD (predicts ~2x the actual -> ~67% sMAPE).
    The ensemble equals the actuals (treated as perfect here).
    """
    if actuals is None:
        actuals = [10.0 + i for i in range(steps)]
    assert len(actuals) == steps

    now = datetime.now(timezone.utc)
    horizon_minutes = steps * interval_min
    # Place the last step a couple of intervals in the past so the horizon has
    # fully elapsed and the matching window is comfortably satisfied.
    last_step = now - timedelta(minutes=2 * interval_min)
    first_step = last_step - timedelta(minutes=(steps - 1) * interval_min)
    step_dts = [
        first_step + timedelta(minutes=i * interval_min) for i in range(steps)
    ]
    step_iso = [dt.isoformat() for dt in step_dts]
    created_at = first_step - timedelta(minutes=interval_min)

    perfect = list(actuals)            # arima -> exact
    bad = [a * 2.0 for a in actuals]   # xgboost -> ~67% sMAPE => low accuracy
    individual = {
        "arima": perfect,
        "exp_smoothing": [a * 1.05 for a in actuals],  # slightly off
        "linear": [a * 1.1 for a in actuals],          # a bit more off
        "xgboost": bad,
    }
    repo.save_forecast(
        session,
        metric_name=metric_name,
        created_at=created_at,
        horizon_minutes=horizon_minutes,
        horizon_steps=steps,
        ensemble_prediction=perfect,
        ensemble_confidence=[0.9] * steps,
        individual_forecasts=individual,
        alert_level="normal",
        step_timestamps=step_iso,
        commit=True,
    )
    # Insert actual metric points at exactly the step timestamps.
    repo.add_metrics_bulk(
        session,
        [
            {"metric_name": metric_name, "timestamp": dt, "value": v}
            for dt, v in zip(step_dts, actuals)
        ],
        commit=True,
    )
    return step_dts, actuals


def _seed_metadata(session: Session, models: list[str], *, weight: float) -> None:
    """Seed ModelMetadata rows for ``models`` with an equal weight + is_deployed."""
    for m in models:
        repo.upsert_model_metadata(
            session,
            m,
            weight=weight,
            is_deployed=True,
            accuracy=0.5,
            commit=True,
        )


# --------------------------------------------------------------------------- #
# 1. evaluate_forecast_accuracy: matching + scoring + persistence
# --------------------------------------------------------------------------- #
def test_evaluate_forecast_accuracy_scores_and_persists(
    session: Session, unique: str
) -> None:
    name = f"fb_test_{unique}"
    _seed_elapsed_forecast(session, name)

    summary = evaluate_forecast_accuracy(session, name)

    assert summary["metric_name"] == name
    assert summary["evaluated_forecasts"] >= 1
    assert summary["matched_points"] > 0

    per_acc = summary["per_model_accuracy"]
    assert "ensemble" in per_acc
    assert "arima" in per_acc and "xgboost" in per_acc
    # Perfect model near 1.0; bad model clearly lower.
    assert per_acc["arima"] > 0.95
    assert per_acc["xgboost"] < per_acc["arima"]
    assert per_acc["xgboost"] < 0.6

    # MAPE/RMSE present per model; perfect model ~0 error.
    assert "arima" in summary["per_model_mape"]
    assert summary["per_model_rmse"]["arima"] < 1e-6
    assert summary["per_model_rmse"]["xgboost"] > summary["per_model_rmse"]["arima"]

    # AccuracyRecords persisted with actual/abs/pct error populated.
    records = repo.get_recent_accuracy(session, "arima", metric_name=name, limit=50)
    assert records
    r = records[0]
    assert r.actual_value is not None
    assert r.absolute_error is not None
    assert r.percentage_error is not None
    # arima perfect -> near-zero absolute error.
    assert r.absolute_error < 1e-6

    ens_records = repo.get_recent_accuracy(
        session, "ensemble", metric_name=name, limit=50
    )
    assert ens_records  # ensemble pseudo-model persisted too


def test_evaluate_skips_future_horizon(session: Session, unique: str) -> None:
    """A forecast whose steps are all in the FUTURE is not scored."""
    name = f"fb_future_{unique}"
    now = datetime.now(timezone.utc)
    steps = 6
    step_dts = [now + timedelta(minutes=5 * (i + 1)) for i in range(steps)]
    repo.save_forecast(
        session,
        metric_name=name,
        created_at=now,
        horizon_minutes=30,
        horizon_steps=steps,
        ensemble_prediction=[1.0] * steps,
        ensemble_confidence=[0.9] * steps,
        individual_forecasts={"arima": [1.0] * steps},
        alert_level="normal",
        step_timestamps=[dt.isoformat() for dt in step_dts],
        commit=True,
    )
    summary = evaluate_forecast_accuracy(session, name)
    assert summary["evaluated_forecasts"] == 0
    assert summary["matched_points"] == 0


def test_evaluate_skips_when_no_matching_actuals(
    session: Session, unique: str
) -> None:
    """An elapsed forecast with NO nearby actuals is skipped without crashing."""
    name = f"fb_noactual_{unique}"
    now = datetime.now(timezone.utc)
    steps = 6
    last_step = now - timedelta(minutes=10)
    first_step = last_step - timedelta(minutes=5 * (steps - 1))
    step_dts = [first_step + timedelta(minutes=5 * i) for i in range(steps)]
    repo.save_forecast(
        session,
        metric_name=name,
        created_at=first_step - timedelta(minutes=5),
        horizon_minutes=30,
        horizon_steps=steps,
        ensemble_prediction=[1.0] * steps,
        ensemble_confidence=[0.9] * steps,
        individual_forecasts={"arima": [1.0] * steps},
        alert_level="normal",
        step_timestamps=[dt.isoformat() for dt in step_dts],
        commit=True,
    )
    # Deliberately insert no metrics -> no actuals to match.
    summary = evaluate_forecast_accuracy(session, name)
    assert summary["evaluated_forecasts"] == 0


# --------------------------------------------------------------------------- #
# 2. recent_model_accuracy
# --------------------------------------------------------------------------- #
def test_recent_model_accuracy_reflects_ledger(
    session: Session, unique: str
) -> None:
    name = f"fb_recent_{unique}"
    _seed_elapsed_forecast(session, name)
    evaluate_forecast_accuracy(session, name)

    acc = recent_model_accuracy(session, name)
    assert set(MODELS).issubset(acc.keys())
    for v in acc.values():
        assert 0.0 <= v <= 1.0
    assert acc["arima"] > acc["xgboost"]
    assert acc["arima"] > 0.95


def test_recent_model_accuracy_empty_is_neutral(
    session: Session, unique: str
) -> None:
    name = f"fb_empty_recent_{unique}"
    acc = recent_model_accuracy(session, name)
    # No data -> neutral 0.5 across the configured universe.
    assert acc  # never empty (configured members + ensemble)
    for m in MODELS + ["ensemble"]:
        assert acc.get(m) == 0.5


# --------------------------------------------------------------------------- #
# 3. adjust_weights (dynamic weighting — KEY)
# --------------------------------------------------------------------------- #
def test_adjust_weights_shifts_toward_accurate_model(
    session: Session, unique: str
) -> None:
    name = f"fb_weights_{unique}"
    _seed_metadata(session, MODELS, weight=0.25)
    _seed_elapsed_forecast(session, name)
    evaluate_forecast_accuracy(session, name)

    weights = adjust_weights(session, name, persist=True)

    # The metadata table is global (not metric-scoped), so other tests' leftover
    # ModelMetadata rows may also appear in the deployed universe. Assert on the
    # contract that matters: our 4 members are present and the deployed weights
    # sum to ~1.0.
    assert set(MODELS).issubset(weights.keys())
    assert abs(sum(weights.values()) - 1.0) < 1e-6
    # The accurate model (arima) outweighs the inaccurate one (xgboost).
    assert weights["arima"] > weights["xgboost"]

    # New weights persisted to ModelMetadata.
    md_arima = repo.get_model_metadata(session, "arima")
    md_xgb = repo.get_model_metadata(session, "xgboost")
    assert md_arima is not None and md_xgb is not None
    assert abs(md_arima.weight - weights["arima"]) < 1e-6
    assert md_arima.weight > md_xgb.weight


def test_adjust_weights_no_data_does_not_raise(
    session: Session, unique: str
) -> None:
    name = f"fb_weights_empty_{unique}"
    # No metadata, no ledger -> falls back to settings/prior without raising.
    weights = adjust_weights(session, name, persist=False)
    assert isinstance(weights, dict)
    # With no accuracy data it still returns the deployed (configured) members
    # summing to ~1.0 (blend of prior * neutral accuracy, renormalised).
    if weights:
        assert abs(sum(weights.values()) - 1.0) < 1e-6


# --------------------------------------------------------------------------- #
# 4. should_retrain / maybe_trigger_retrain
# --------------------------------------------------------------------------- #
def _seed_accuracy_records(
    session: Session,
    metric_name: str,
    *,
    abs_off: float,
    n: int = 8,
) -> None:
    """Seed AccuracyRecords for every model with a controlled error level.

    ``abs_off`` is the multiplicative offset of predicted vs actual; small ->
    high accuracy, large -> low accuracy. Includes the ``ensemble`` pseudo-model
    so should_retrain can read the ensemble source.
    """
    now = datetime.now(timezone.utc)
    actual = 100.0
    predicted = actual * (1.0 + abs_off)
    for model in MODELS + ["ensemble"]:
        for i in range(n):
            repo.add_accuracy_record(
                session,
                model_name=model,
                metric_name=metric_name,
                evaluated_at=now - timedelta(minutes=i),
                horizon_minutes=30,
                predicted_value=predicted,
                actual_value=actual,
                absolute_error=abs(predicted - actual),
                percentage_error=abs_off,
                commit=True,
            )


def test_should_retrain_below_threshold(session: Session, unique: str) -> None:
    name = f"fb_retrain_low_{unique}"
    # Large offset -> very low accuracy -> below default threshold 0.6.
    _seed_accuracy_records(session, name, abs_off=2.0)
    do, info = should_retrain(session, name)
    assert info["has_data"] is True
    assert info["accuracy"] is not None
    assert info["accuracy"] < info["threshold"]
    assert do is True


def test_should_retrain_above_threshold(session: Session, unique: str) -> None:
    name = f"fb_retrain_high_{unique}"
    # Tiny offset -> very high accuracy -> above threshold.
    _seed_accuracy_records(session, name, abs_off=0.01)
    do, info = should_retrain(session, name)
    assert info["has_data"] is True
    assert info["accuracy"] > info["threshold"]
    assert do is False


def test_should_retrain_no_data(session: Session, unique: str) -> None:
    name = f"fb_retrain_nodata_{unique}"
    do, info = should_retrain(session, name)
    assert do is False
    assert info["has_data"] is False


def test_maybe_trigger_retrain_fires_below(session: Session, unique: str) -> None:
    name = f"fb_trigger_low_{unique}"
    _seed_accuracy_records(session, name, abs_off=2.0)
    calls: list[str] = []
    result = maybe_trigger_retrain(
        session, name, retrain_fn=lambda m: calls.append(m) or {"ok": True}
    )
    assert result["retrained"] is True
    assert calls == [name]
    assert result.get("retrain_result") == {"ok": True}


def test_maybe_trigger_retrain_not_above(session: Session, unique: str) -> None:
    name = f"fb_trigger_high_{unique}"
    _seed_accuracy_records(session, name, abs_off=0.01)
    calls: list[str] = []
    result = maybe_trigger_retrain(
        session, name, retrain_fn=lambda m: calls.append(m)
    )
    assert result["retrained"] is False
    assert calls == []


# --------------------------------------------------------------------------- #
# 5. run_feedback_cycle (end-to-end with injected fake retrain_fn)
# --------------------------------------------------------------------------- #
def test_run_feedback_cycle_end_to_end(session: Session, unique: str) -> None:
    name = f"fb_cycle_{unique}"
    _seed_metadata(session, MODELS, weight=0.25)
    _seed_elapsed_forecast(session, name)

    calls: list[str] = []
    result = run_feedback_cycle(
        session, name, retrain_fn=lambda m: calls.append(m) or {"ok": True}
    )

    assert result["metric_name"] == name
    assert set(result.keys()) == {
        "metric_name",
        "accuracy_summary",
        "new_weights",
        "retrain",
    }
    # Evaluated + persisted accuracy.
    assert result["accuracy_summary"]["evaluated_forecasts"] >= 1
    # Weights adjusted + persisted (sum ~1, arima dominant).
    nw = result["new_weights"]
    assert abs(sum(nw.values()) - 1.0) < 1e-6
    assert nw["arima"] > nw["xgboost"]
    md = repo.get_model_metadata(session, "arima")
    assert md is not None and abs(md.weight - nw["arima"]) < 1e-6

    # Retrain decision present. In this controlled data the ensemble is perfect
    # so accuracy is high -> no retrain; assert it didn't fire blindly.
    assert result["retrain"]["retrained"] is False
    assert calls == []


def test_run_feedback_cycle_triggers_retrain_on_bad_data(
    session: Session, unique: str
) -> None:
    """A metric whose ensemble accuracy is below threshold triggers retrain."""
    name = f"fb_cycle_bad_{unique}"
    # Seed a BAD ledger directly (ensemble far off) so should_retrain fires.
    _seed_accuracy_records(session, name, abs_off=2.0)
    calls: list[str] = []
    result = run_feedback_cycle(
        session, name, retrain_fn=lambda m: calls.append(m) or {"ok": True}
    )
    assert result["retrain"]["retrained"] is True
    assert calls == [name]


# --------------------------------------------------------------------------- #
# 6. Graceful: every function on an empty metric returns safe results
# --------------------------------------------------------------------------- #
def test_all_functions_graceful_on_empty_metric(
    session: Session, unique: str
) -> None:
    name = f"fb_graceful_{unique}"

    summary = evaluate_forecast_accuracy(session, name)
    assert summary["evaluated_forecasts"] == 0
    assert summary["matched_points"] == 0
    assert summary["per_model_accuracy"] == {}

    acc = recent_model_accuracy(session, name)
    assert all(v == 0.5 for v in acc.values())

    weights = adjust_weights(session, name, persist=False)
    assert isinstance(weights, dict)

    do, info = should_retrain(session, name)
    assert do is False and info["has_data"] is False

    calls: list[str] = []
    mt = maybe_trigger_retrain(session, name, retrain_fn=lambda m: calls.append(m))
    assert mt["retrained"] is False and calls == []

    cycle = run_feedback_cycle(session, name, retrain_fn=lambda m: calls.append(m))
    assert cycle["metric_name"] == name
    assert calls == []


# --------------------------------------------------------------------------- #
# 7. Celery task bodies (src.tasks) — synchronously callable, own their session
# --------------------------------------------------------------------------- #
def test_run_feedback_task_body(session: Session, unique: str) -> None:
    """``tasks.run_feedback`` runs the cycle end-to-end on committed data.

    The task owns its own ``SessionLocal`` session, so the seed data must be
    committed first (it is — the helpers commit). The seeded ensemble is perfect
    here, so accuracy stays high and the real ``run_retrain`` is never invoked.
    """
    name = f"fb_task_{unique}"
    _seed_metadata(session, MODELS, weight=0.25)
    _seed_elapsed_forecast(session, name)

    result = run_feedback(name)

    assert result["metric_name"] == name
    assert set(result.keys()) == {
        "metric_name",
        "accuracy_summary",
        "new_weights",
        "retrain",
    }
    assert result["accuracy_summary"]["evaluated_forecasts"] >= 1
    nw = result["new_weights"]
    assert abs(sum(nw.values()) - 1.0) < 1e-6
    assert nw["arima"] > nw["xgboost"]
    # High ensemble accuracy -> no blind retrain.
    assert result["retrain"]["retrained"] is False

    # Accuracy records were persisted by the task's own session; visible here.
    records = repo.get_recent_accuracy(session, "arima", metric_name=name, limit=50)
    assert records


def test_run_feedback_task_graceful_on_empty_metric(unique: str) -> None:
    """``tasks.run_feedback`` on an unknown metric returns a safe summary, no raise."""
    name = f"fb_task_empty_{unique}"
    result = run_feedback(name)
    assert result["metric_name"] == name
    # Either the normal cycle shape (no forecasts -> 0 evaluated) or an error dict;
    # never raises and never triggers a retrain blindly.
    if "error" not in result:
        assert result["accuracy_summary"]["evaluated_forecasts"] == 0
        assert result["retrain"]["retrained"] is False


def test_run_scheduled_feedback_fans_out(session: Session, unique: str) -> None:
    """``tasks.run_scheduled_feedback`` iterates known metrics without raising.

    A seeded metric guarantees at least one metric name is discovered; the result
    is a fan-out summary whose count matches the number of summaries returned.
    """
    name = f"fb_sched_{unique}"
    _seed_metadata(session, MODELS, weight=0.25)
    _seed_elapsed_forecast(session, name)

    result = run_scheduled_feedback()
    assert isinstance(result, dict)
    assert "metrics" in result and "count" in result
    assert result["count"] == len(result["metrics"])
    assert result["count"] >= 1
    # Our seeded metric is among those processed.
    names = {s.get("metric_name") for s in result["metrics"]}
    assert name in names

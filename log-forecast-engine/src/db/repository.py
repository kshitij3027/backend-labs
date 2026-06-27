"""Repository helpers: plain functions over a :class:`~sqlalchemy.orm.Session`.

These are thin, well-typed CRUD operations the rest of the system calls. They
keep the ORM details in one place so services / tasks / the API never build raw
queries.

Transaction convention
-----------------------
Every mutating helper accepts ``commit: bool = False``:

* ``commit=False`` (default) — the helper :meth:`Session.flush`-es so the row is
  assigned a primary key and is visible within the transaction, but leaves the
  final ``COMMIT`` to the caller. This lets a caller batch several writes into
  one transaction.
* ``commit=True`` — the helper commits before returning, refreshing the instance
  so all server-side defaults are populated.

Read helpers never write.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import AccuracyRecord, Forecast, Metric, ModelMetadata


def _finalize(session: Session, instance: Any, commit: bool) -> Any:
    """Flush (always) and optionally commit + refresh ``instance``."""
    session.add(instance)
    if commit:
        session.commit()
        session.refresh(instance)
    else:
        session.flush()
    return instance


# --------------------------------------------------------------------------- #
# Metric (historical time-series points)
# --------------------------------------------------------------------------- #
def add_metric(
    session: Session,
    metric_name: str,
    timestamp: datetime,
    value: float,
    *,
    commit: bool = False,
) -> Metric:
    """Insert a single metric observation and return it."""
    metric = Metric(metric_name=metric_name, timestamp=timestamp, value=value)
    return _finalize(session, metric, commit)


def add_metrics_bulk(
    session: Session,
    points: Iterable[Mapping[str, Any]],
    *,
    commit: bool = False,
) -> list[Metric]:
    """Insert many metric points.

    Each item in ``points`` must provide ``metric_name``, ``timestamp`` and
    ``value`` keys. Returns the created instances (in input order).
    """
    metrics = [
        Metric(
            metric_name=p["metric_name"],
            timestamp=p["timestamp"],
            value=p["value"],
        )
        for p in points
    ]
    session.add_all(metrics)
    if commit:
        session.commit()
        for m in metrics:
            session.refresh(m)
    else:
        session.flush()
    return metrics


def get_metrics(
    session: Session,
    metric_name: str,
    since: datetime | None = None,
    limit: int | None = None,
) -> list[Metric]:
    """Return metric points for ``metric_name`` ordered oldest-first.

    ``since`` filters to ``timestamp >= since``; ``limit`` caps the row count.
    """
    stmt = select(Metric).where(Metric.metric_name == metric_name)
    if since is not None:
        stmt = stmt.where(Metric.timestamp >= since)
    stmt = stmt.order_by(Metric.timestamp.asc())
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt).all())


def list_metric_names(session: Session) -> list[str]:
    """Return the distinct metric names present in the metrics table, sorted.

    Used by the scheduled Celery tasks to discover which metrics to forecast /
    retrain. An empty table yields an empty list (the tasks then no-op).
    """
    stmt = select(Metric.metric_name).distinct().order_by(Metric.metric_name.asc())
    return list(session.scalars(stmt).all())


def get_latest_metrics(session: Session, metric_name: str, n: int) -> list[Metric]:
    """Return the ``n`` most recent metric points, ordered oldest-first.

    Useful for feeding the most recent window into a model: the query fetches the
    newest ``n`` rows then reverses them so the series is chronological.
    """
    stmt = (
        select(Metric)
        .where(Metric.metric_name == metric_name)
        .order_by(Metric.timestamp.desc())
        .limit(n)
    )
    rows = list(session.scalars(stmt).all())
    rows.reverse()
    return rows


# --------------------------------------------------------------------------- #
# Forecast
# --------------------------------------------------------------------------- #
def save_forecast(
    session: Session,
    *,
    metric_name: str,
    created_at: datetime,
    horizon_minutes: int,
    horizon_steps: int,
    ensemble_prediction: Sequence[float],
    ensemble_confidence: Sequence[float],
    individual_forecasts: Mapping[str, Sequence[float]],
    alert_level: str,
    step_timestamps: Sequence[str],
    commit: bool = False,
) -> Forecast:
    """Persist a forecast snapshot and return it."""
    forecast = Forecast(
        metric_name=metric_name,
        created_at=created_at,
        horizon_minutes=horizon_minutes,
        horizon_steps=horizon_steps,
        ensemble_prediction=list(ensemble_prediction),
        ensemble_confidence=list(ensemble_confidence),
        individual_forecasts={k: list(v) for k, v in individual_forecasts.items()},
        alert_level=alert_level,
        step_timestamps=list(step_timestamps),
    )
    return _finalize(session, forecast, commit)


def get_latest_forecast(session: Session, metric_name: str) -> Forecast | None:
    """Return the most recently created forecast for ``metric_name`` (or None)."""
    stmt = (
        select(Forecast)
        .where(Forecast.metric_name == metric_name)
        .order_by(Forecast.created_at.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()


def get_forecast_history(
    session: Session, metric_name: str, limit: int = 50
) -> list[Forecast]:
    """Return recent forecasts for ``metric_name``, newest-first, capped at ``limit``."""
    stmt = (
        select(Forecast)
        .where(Forecast.metric_name == metric_name)
        .order_by(Forecast.created_at.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt).all())


# --------------------------------------------------------------------------- #
# ModelMetadata
# --------------------------------------------------------------------------- #
def upsert_model_metadata(
    session: Session,
    model_name: str,
    *,
    commit: bool = False,
    **fields: Any,
) -> ModelMetadata:
    """Insert or update the metadata row for ``model_name``.

    Only the supplied ``fields`` are written on update; the row is created with
    those fields (plus model defaults) if it does not yet exist.
    """
    existing = get_model_metadata(session, model_name)
    if existing is None:
        existing = ModelMetadata(model_name=model_name, **fields)
        session.add(existing)
    else:
        for key, val in fields.items():
            setattr(existing, key, val)
    if commit:
        session.commit()
        session.refresh(existing)
    else:
        session.flush()
    return existing


def get_model_metadata(session: Session, model_name: str) -> ModelMetadata | None:
    """Return the metadata row for ``model_name`` (or None)."""
    stmt = select(ModelMetadata).where(ModelMetadata.model_name == model_name)
    return session.scalars(stmt).first()


def list_model_metadata(session: Session) -> list[ModelMetadata]:
    """Return all model-metadata rows ordered by model name."""
    stmt = select(ModelMetadata).order_by(ModelMetadata.model_name.asc())
    return list(session.scalars(stmt).all())


# --------------------------------------------------------------------------- #
# AccuracyRecord
# --------------------------------------------------------------------------- #
def add_accuracy_record(
    session: Session,
    *,
    model_name: str,
    metric_name: str,
    evaluated_at: datetime,
    horizon_minutes: int,
    predicted_value: float,
    actual_value: float | None = None,
    absolute_error: float | None = None,
    percentage_error: float | None = None,
    commit: bool = False,
) -> AccuracyRecord:
    """Insert a predicted-vs-actual accuracy record and return it."""
    record = AccuracyRecord(
        model_name=model_name,
        metric_name=metric_name,
        evaluated_at=evaluated_at,
        horizon_minutes=horizon_minutes,
        predicted_value=predicted_value,
        actual_value=actual_value,
        absolute_error=absolute_error,
        percentage_error=percentage_error,
    )
    return _finalize(session, record, commit)


def get_recent_accuracy(
    session: Session,
    model_name: str,
    metric_name: str | None = None,
    limit: int = 100,
) -> list[AccuracyRecord]:
    """Return recent accuracy records for ``model_name`` newest-first.

    Optionally narrowed to a single ``metric_name``; capped at ``limit``.
    """
    stmt = select(AccuracyRecord).where(AccuracyRecord.model_name == model_name)
    if metric_name is not None:
        stmt = stmt.where(AccuracyRecord.metric_name == metric_name)
    stmt = stmt.order_by(AccuracyRecord.evaluated_at.desc()).limit(limit)
    return list(session.scalars(stmt).all())

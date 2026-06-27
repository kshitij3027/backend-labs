"""SQLAlchemy 2.0 typed ORM models for the Predictive Log Analytics Engine.

Four tables back the system:

* :class:`Metric` — the historical metric time-series points (one row per
  ``(metric_name, timestamp)`` observation).
* :class:`Forecast` — a generated forecast snapshot: ensemble + per-model
  predictions, confidence array, alert level and the timestamps the steps map
  to. Mirrors the §8 sample output in ``project_requirements.md``.
* :class:`ModelMetadata` — per-model state: version, last-trained time, recent
  validation accuracy, current ensemble weight, deploy-gate flag, params and the
  on-disk artifact path.
* :class:`AccuracyRecord` — the validation/feedback ledger: predicted vs actual
  per model/metric/horizon, with absolute and percentage error.

All datetime columns are timezone-aware (``DateTime(timezone=True)``). JSON
payloads use PostgreSQL ``JSONB`` (the deployment target) for efficient storage
and indexing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Metric(Base):
    """A single historical metric observation (a time-series point)."""

    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True, nullable=False
    )
    value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        # Time-series reads are almost always "this metric, ordered by time".
        Index("ix_metrics_name_timestamp", "metric_name", "timestamp"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Metric(id={self.id!r}, metric_name={self.metric_name!r}, "
            f"timestamp={self.timestamp!r}, value={self.value!r})"
        )


class Forecast(Base):
    """A generated forecast snapshot for one metric at one point in time."""

    __tablename__ = "forecasts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True, nullable=False
    )
    horizon_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    horizon_steps: Mapped[int] = mapped_column(Integer, nullable=False)

    # Ensemble point predictions and matching confidence scores (parallel arrays).
    ensemble_prediction: Mapped[list[float]] = mapped_column(JSONB, nullable=False)
    ensemble_confidence: Mapped[list[float]] = mapped_column(JSONB, nullable=False)
    # model_name -> [predicted value per step]
    individual_forecasts: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    # ISO-8601 datetimes each prediction step corresponds to.
    step_timestamps: Mapped[list[str]] = mapped_column(JSONB, nullable=False)

    alert_level: Mapped[str] = mapped_column(String(16), nullable=False)

    __table_args__ = (
        # "latest forecast(s) for this metric" — the dominant read pattern.
        Index("ix_forecasts_name_created_at", "metric_name", "created_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Forecast(id={self.id!r}, metric_name={self.metric_name!r}, "
            f"created_at={self.created_at!r}, horizon_minutes={self.horizon_minutes!r}, "
            f"alert_level={self.alert_level!r})"
        )


class ModelMetadata(Base):
    """Per-model state: version, training time, accuracy, weight, deploy flag."""

    __tablename__ = "model_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_name: Mapped[str] = mapped_column(
        String(64), index=True, unique=True, nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_trained_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_deployed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    params: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    artifact_path: Mapped[str | None] = mapped_column(String(512), nullable=True)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"ModelMetadata(id={self.id!r}, model_name={self.model_name!r}, "
            f"version={self.version!r}, accuracy={self.accuracy!r}, "
            f"weight={self.weight!r}, is_deployed={self.is_deployed!r})"
        )


class AccuracyRecord(Base):
    """A predicted-vs-actual validation/feedback ledger entry."""

    __tablename__ = "accuracy_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    metric_name: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True, nullable=False
    )
    horizon_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    predicted_value: Mapped[float] = mapped_column(Float, nullable=False)
    actual_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    absolute_error: Mapped[float | None] = mapped_column(Float, nullable=True)
    percentage_error: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        # Feedback queries: "recent accuracy for this model (and metric)".
        Index(
            "ix_accuracy_model_metric_evaluated",
            "model_name",
            "metric_name",
            "evaluated_at",
        ),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"AccuracyRecord(id={self.id!r}, model_name={self.model_name!r}, "
            f"metric_name={self.metric_name!r}, evaluated_at={self.evaluated_at!r}, "
            f"predicted_value={self.predicted_value!r}, actual_value={self.actual_value!r})"
        )

"""Pydantic v2 request/response contracts for the Predictive Log Analytics Engine.

These models are the public shape of the HTTP API. They are intentionally small
and reusable: later commits (forecast / predictions / metrics endpoints) build on
the same :class:`MetricPoint` primitive used here for ingestion and read-back.

Conventions
-----------
* All timestamps are timezone-aware ``datetime`` objects. On input a naive
  datetime is assumed to be UTC and coerced; on output we always carry tzinfo.
* Metric values must be finite real numbers (NaN / +-inf are rejected at the
  schema boundary so they can never reach the database).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _ensure_utc(ts: datetime) -> datetime:
    """Return ``ts`` as a timezone-aware UTC datetime (naive is assumed UTC)."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class MetricPoint(BaseModel):
    """A single metric observation: ``(metric_name, timestamp, value)``.

    Used both as a stored/returned point and as the canonical shape consumed by
    the ingestion path. ``timestamp`` is required here; the more lenient
    :class:`MetricIngest` allows it to be omitted on input.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime
    value: float

    @field_validator("metric_name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("metric_name must not be empty")
        return v

    @field_validator("value")
    @classmethod
    def _value_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be a finite number (no NaN/inf)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class MetricIngest(BaseModel):
    """An ingestable point where ``timestamp`` is optional.

    When ``timestamp`` is omitted the ingestion layer defaults it to ``now`` in
    UTC. Validation of name/value mirrors :class:`MetricPoint`.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime | None = None
    value: float

    @field_validator("metric_name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("metric_name must not be empty")
        return v

    @field_validator("value")
    @classmethod
    def _value_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be a finite number (no NaN/inf)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _ensure_utc(v)


class MetricIngestRequest(BaseModel):
    """Request body for ``POST /metrics``.

    The canonical shape is ``{"points": [<MetricIngest>, ...]}``. At least one
    point is required.
    """

    points: list[MetricIngest] = Field(..., min_length=1)


class MetricIngestResponse(BaseModel):
    """Response for a successful ingestion."""

    ingested: int
    metric_names: list[str]


class MetricQueryResponse(BaseModel):
    """Response for ``GET /metrics/{metric_name}`` — recent stored points."""

    metric_name: str
    count: int
    points: list[MetricPoint]


class ForecastResponse(BaseModel):
    """A generated forecast, matching ``project_requirements.md`` §8.

    This is the canonical shape produced by
    :func:`src.prediction_service.generate_prediction`, persisted (column-by-column)
    via :func:`src.db.repository.save_forecast`, cached in Redis by
    :mod:`src.clients.redis`, and returned directly by the forecast API (C11).

    The §8 sample shows the core fields (``timestamp``, ``forecast_horizon_minutes``,
    ``ensemble_prediction``, ``ensemble_confidence``, ``individual_forecasts``,
    ``alert_level``); the remaining fields surface the ensemble internals
    (per-step future ``step_timestamps``, prediction-interval ``lower`` / ``upper``,
    aggregate scalar ``confidence``, ``weights_used``, ``failed_models``) plus a
    ``cached`` flag the read path sets when serving from Redis.

    All arrays (``ensemble_prediction``, ``ensemble_confidence``, ``lower``,
    ``upper``, ``step_timestamps`` and each list in ``individual_forecasts``) are
    parallel and ``horizon_steps`` long.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str
    # When the forecast was generated (ISO-8601 UTC on serialisation).
    timestamp: datetime
    forecast_horizon_minutes: int
    horizon_steps: int
    # ISO-8601 timestamp string for each predicted step.
    step_timestamps: list[str] = Field(default_factory=list)

    ensemble_prediction: list[float] = Field(default_factory=list)
    ensemble_confidence: list[float] = Field(default_factory=list)
    individual_forecasts: dict[str, list[float]] = Field(default_factory=dict)

    # Ensemble prediction interval (parallel to ``ensemble_prediction``).
    lower: list[float] = Field(default_factory=list)
    upper: list[float] = Field(default_factory=list)

    alert_level: str = "low"
    # Aggregate scalar confidence used to derive ``alert_level``.
    confidence: float = 0.0
    weights_used: dict[str, float] = Field(default_factory=dict)
    failed_models: list[str] = Field(default_factory=list)

    # True when this payload was served from the Redis cache (set by the API /
    # read path); the generation path leaves it False.
    cached: bool = False
    # Optional human-readable note (e.g. why a degraded result was returned).
    note: str | None = None

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


# --------------------------------------------------------------------------- #
# C11 API surface: models / config / health / retrain / app-metrics
# --------------------------------------------------------------------------- #
class ModelInfo(BaseModel):
    """One ensemble member's current state (for ``GET /models``)."""

    # protected_namespaces=() silences Pydantic v2's ``model_*`` field warning.
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    model_name: str
    version: int = 1
    weight: float = 0.0
    accuracy: float | None = None
    is_deployed: bool = False
    last_trained_at: datetime | None = None


class ModelsResponse(BaseModel):
    """Response for ``GET /models`` — the ensemble roster + deployed count."""

    count: int
    deployed_count: int
    models: list[ModelInfo] = Field(default_factory=list)


class ForecastHistoryItem(BaseModel):
    """A compact summary of one past forecast (``GET /forecast/{metric}/history``)."""

    id: int
    created_at: datetime
    horizon_minutes: int
    horizon_steps: int
    alert_level: str
    ensemble_prediction: list[float] = Field(default_factory=list)
    step_timestamps: list[str] = Field(default_factory=list)


class ForecastHistoryResponse(BaseModel):
    """Response for ``GET /forecast/{metric}/history``."""

    metric_name: str
    count: int
    items: list[ForecastHistoryItem] = Field(default_factory=list)
    # Recent per-model accuracy from the feedback ledger (best-effort; may be empty).
    recent_accuracy: dict[str, float] = Field(default_factory=dict)


class RetrainResponse(BaseModel):
    """Response for ``POST /retrain`` (202 Accepted)."""

    status: str
    metric: str | None = None
    # Set when enqueued via a Celery broker; ``None`` when run inline (no broker).
    task_id: str | None = None
    mode: str  # "async" (broker) | "background" (FastAPI BackgroundTasks)


class ConfigResponse(BaseModel):
    """Response for ``GET /config`` and ``PUT /config``.

    Carries the *mutable* runtime overrides (weights / thresholds / alert
    settings) plus the *static* settings the dashboard needs (intervals + horizon
    bounds) for context. Only the mutable fields can be changed via ``PUT``.
    """

    model_config = ConfigDict(protected_namespaces=())

    model_weights: dict[str, float]
    high_confidence_threshold: float
    medium_confidence_threshold: float
    alert_settings: dict[str, object] = Field(default_factory=dict)
    # Static, read-only context (echoed for the dashboard; not updatable here).
    prediction_interval_min: int
    default_horizon_min: int
    horizon_min_steps: int
    horizon_max_steps: int


class ConfigUpdateRequest(BaseModel):
    """Partial update body for ``PUT /config`` (all fields optional)."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model_weights: dict[str, float] | None = None
    high_confidence_threshold: float | None = None
    medium_confidence_threshold: float | None = None
    alert_settings: dict[str, object] | None = None


class SubsystemHealth(BaseModel):
    """Per-subsystem health booleans for ``GET /health``."""

    database: bool = False
    redis: bool = False


class HealthResponse(BaseModel):
    """Enhanced ``GET /health`` payload (always 200; degraded reported in-body)."""

    status: str  # "ok" | "degraded"
    service: str
    version: str
    deployed_models: int = 0
    subsystems: SubsystemHealth
    performance: dict[str, object] = Field(default_factory=dict)


class AppMetricsResponse(BaseModel):
    """Application metrics JSON for ``GET /metrics`` (accuracy / timings / resource)."""

    prediction_accuracy: dict[str, float] = Field(default_factory=dict)
    processing_times: dict[str, object] = Field(default_factory=dict)
    resource_usage: dict[str, object] = Field(default_factory=dict)
    counts: dict[str, int] = Field(default_factory=dict)

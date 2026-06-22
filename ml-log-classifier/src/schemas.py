"""Pydantic v2 request/response models for the FastAPI service (Commits 8–9).

These models are the **API contract** between the classifier service and its
clients (the React dashboard, the E2E scripts, ``curl``). They are intentionally
thin: every field mirrors either an input the model needs or a key the
:class:`src.ensemble.LogClassifier` already emits, so there is no translation
layer between the model output and the JSON on the wire.

The models map onto the spec's Input/Output spec (project requirements §8):

* :class:`ClassifyResponse` mirrors :meth:`LogClassifier.classify`'s dict exactly
  (``severity`` / ``category`` / ``confidence`` plus the two per-axis
  confidences). The spec's headline sample output
  (``{"severity": "ERROR", "category": "SYSTEM", "confidence": 0.942}``) is a
  subset of these keys.
* :class:`StatsResponse` is the ``/stats`` shape: ``{"total_classified": 0,
  "model_status": "ready"}``.

Commit 9 adds the contracts for on-demand training and bulk/streaming inference:

* :class:`TrainRequest` / :class:`TrainStatusResponse` — the body and status
  snapshot for ``POST /train`` (kick off a background retrain) and
  ``GET /train/status`` (poll its progress + the live model lifecycle).
* :class:`BatchClassifyRequest` / :class:`BatchClassifyResponse` — a list of logs
  in, a list of results (+ a count) out, shared by ``POST /classify/batch`` and
  the NDJSON streaming endpoint ``POST /classify/stream`` (which streams a
  :class:`ClassifyResponse`-shaped line per input rather than returning the batch
  envelope).

The live-metrics WebSocket, the metrics aggregator and the feedback/A-B schemas
arrive in later commits.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class ClassifyRequest(BaseModel):
    """A single classification request body for ``POST /classify``.

    Only the raw text is required; ``timestamp`` is an optional ISO-8601 string
    used by the feature pipeline for temporal features (it falls back to neutral
    values when absent).

    Attributes:
        raw_log: The raw log line / message to classify (must be non-empty).
        timestamp: Optional ISO-8601 timestamp for temporal features.
    """

    raw_log: str = Field(
        ...,
        min_length=1,
        description="Raw log line / message to classify.",
        examples=["Database connection failed with timeout error"],
    )
    timestamp: Optional[str] = Field(
        default=None,
        description="Optional ISO-8601 timestamp for temporal features.",
        examples=["2026-06-18T12:34:56"],
    )


class ClassifyResponse(BaseModel):
    """The structured classification result returned by ``POST /classify``.

    Mirrors :meth:`src.ensemble.LogClassifier.classify` key-for-key so the
    model's output dict validates directly against this model with no remapping.

    Attributes:
        severity: Predicted severity label (e.g. ``"ERROR"``).
        category: Predicted category label (e.g. ``"SYSTEM"``).
        confidence: Overall confidence (mean of the two per-axis confidences),
            rounded to 4 decimals.
        severity_confidence: Max soft-voting probability for the severity axis.
        category_confidence: Max soft-voting probability for the category axis.
    """

    severity: str = Field(..., description="Predicted severity label.")
    category: str = Field(..., description="Predicted category label.")
    confidence: float = Field(
        ..., description="Overall confidence (mean of the two per-axis confidences)."
    )
    severity_confidence: float = Field(
        ..., description="Max soft-voting probability for the severity axis."
    )
    category_confidence: float = Field(
        ..., description="Max soft-voting probability for the category axis."
    )


class StatsResponse(BaseModel):
    """The aggregate stats payload returned by ``GET /stats``.

    Matches the spec's sample (project requirements §8):
    ``{"total_classified": 0, "model_status": "ready"}``.

    Attributes:
        total_classified: Number of logs classified since the process started.
        model_status: ``"ready"`` once a model is loaded/trained, else
            ``"untrained"``.
    """

    total_classified: int = Field(
        ..., description="Number of logs classified since process start."
    )
    model_status: str = Field(
        ..., description='Model lifecycle status, e.g. "ready" or "untrained".'
    )


class HealthResponse(BaseModel):
    """The liveness payload returned by ``GET /health``.

    Attributes:
        status: ``"healthy"`` when the process is up and serving. Because the
            FastAPI lifespan startup (which loads/trains the model) completes
            *before* the server accepts requests, a ``"healthy"`` response
            implies the model is loaded.
        model_status: Optional mirror of :attr:`StatsResponse.model_status` for
            convenience.
    """

    status: str = Field(..., description='Liveness status, e.g. "healthy".')
    model_status: Optional[str] = Field(
        default=None, description="Optional model lifecycle status."
    )


class TrainRequest(BaseModel):
    """The (optional) body for ``POST /train`` — kick off a background retrain.

    Both fields are optional; when omitted the endpoint falls back to the process
    configuration (``cfg.sample_size`` for the corpus size and a default of 5
    folds for cross-validation). An empty body (``{}``) is therefore valid and
    trains on the configured defaults.

    Attributes:
        count: Number of synthetic logs to train on. ``None`` -> ``cfg.sample_size``.
        cv: Cross-validation fold count. ``None`` -> 5.
    """

    count: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of synthetic logs to train on (default: cfg.sample_size).",
        examples=[200],
    )
    cv: Optional[int] = Field(
        default=None,
        ge=2,
        description="Cross-validation fold count (default: 5).",
        examples=[3],
    )


class TrainStatusResponse(BaseModel):
    """A snapshot of the training lifecycle, returned by ``POST /train`` (202) and
    ``GET /train/status``.

    Lets a client poll until a background retrain finishes: when ``is_training``
    flips back to ``False`` and ``model_status`` is ``"ready"`` (and, after a
    successful run, ``current_version`` has advanced), the new model is live.

    Attributes:
        model_status: Model lifecycle status — one of ``"ready"``, ``"training"``
            or ``"untrained"``.
        current_version: The registry's active version id (e.g. ``"v2"``), or
            ``None`` if nothing has been trained/persisted yet.
        is_training: ``True`` while a background retrain thread is running.
        last_metrics: The metrics dict from the most recent successful training in
            this process, or ``None`` if none has completed yet.
    """

    model_status: str = Field(
        ..., description='Model lifecycle status: "ready" | "training" | "untrained".'
    )
    current_version: Optional[str] = Field(
        default=None, description="Active registry version id, or None."
    )
    is_training: bool = Field(
        ..., description="True while a background retrain is in progress."
    )
    last_metrics: Optional[dict[str, Any]] = Field(
        default=None, description="Metrics from the last successful training, if any."
    )


class BatchClassifyRequest(BaseModel):
    """A batch of logs to classify, for ``POST /classify/batch`` and
    ``POST /classify/stream``.

    Reuses :class:`ClassifyRequest` per item so each entry is a
    ``{raw_log, timestamp?}`` object, exactly like the single-classify body. At
    least one log is required.

    Attributes:
        logs: Non-empty list of per-log classification requests.
    """

    logs: list[ClassifyRequest] = Field(
        ...,
        min_length=1,
        description="Non-empty list of logs to classify ({raw_log, timestamp?}).",
    )


class BatchClassifyResponse(BaseModel):
    """The envelope returned by ``POST /classify/batch``.

    (The streaming endpoint ``POST /classify/stream`` instead emits one
    :class:`ClassifyResponse`-shaped JSON object per line and does not use this
    wrapper.)

    Attributes:
        results: One :class:`ClassifyResponse` per input log, in input order.
        count: ``len(results)`` — a convenience mirror for clients.
    """

    results: list[ClassifyResponse] = Field(
        ..., description="One classification result per input log, in order."
    )
    count: int = Field(..., description="Number of results (== len(results)).")

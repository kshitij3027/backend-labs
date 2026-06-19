"""Pydantic v2 request/response models for the FastAPI service (Commit 8).

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

Only the surface needed for ``/health`` + ``/stats`` + ``POST /classify`` lives
here; streaming, training and feedback schemas arrive in later commits.
"""

from __future__ import annotations

from typing import Optional

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

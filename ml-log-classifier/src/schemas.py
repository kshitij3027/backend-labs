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


class MultiServiceResponse(BaseModel):
    """The hierarchical multi-service result returned by ``POST /classify/service``.

    Mirrors :meth:`src.multiservice.MultiServiceClassifier.classify` key-for-key
    (Feature Area A): the predicted service, the **service-specific** severity, the
    global category, each with its own soft-voting confidence, plus the overall
    confidence and a cross-service anomaly score.

    Attributes:
        service: Predicted source service (``"web"`` / ``"database"`` / ``"cache"``).
        service_confidence: Max soft-voting probability for the service axis.
        severity: Predicted severity from the predicted service's own model.
        severity_confidence: Max soft-voting probability for the severity axis.
        category: Predicted (global) category label.
        category_confidence: Max soft-voting probability for the category axis.
        confidence: Overall confidence (mean of the three per-axis confidences),
            rounded to 4 decimals.
        anomaly_score: Cross-service anomaly score in ``[0, 1]`` (high when the
            service is ambiguous and/or the per-service severity models disagree),
            rounded to 4 decimals.
    """

    service: str = Field(..., description="Predicted source service.")
    service_confidence: float = Field(
        ..., description="Max soft-voting probability for the service axis."
    )
    severity: str = Field(
        ..., description="Predicted severity (from the predicted service's model)."
    )
    severity_confidence: float = Field(
        ..., description="Max soft-voting probability for the severity axis."
    )
    category: str = Field(..., description="Predicted (global) category label.")
    category_confidence: float = Field(
        ..., description="Max soft-voting probability for the category axis."
    )
    confidence: float = Field(
        ..., description="Overall confidence (mean of the three per-axis confidences)."
    )
    anomaly_score: float = Field(
        ...,
        description="Cross-service anomaly score in [0, 1] (service ambiguity + "
        "per-service severity disagreement).",
    )


# -- Commit 12: adaptive learning loop (Feature Area B) --------------------


class FeedbackRequest(BaseModel):
    """Ground-truth feedback for one log, the body for ``POST /feedback``.

    Ops submits the *correct* label for a previously-seen (or replayed) log so the
    :class:`src.adaptive.DriftMonitor` can measure how well the live model is
    doing and, if recent accuracy has slipped, fold the example into the next
    retrain corpus. Only the raw text and the true severity are required; the true
    category (when known) improves the retrain signal, and ``timestamp`` feeds the
    temporal features exactly as in :class:`ClassifyRequest`.

    Attributes:
        raw_log: The raw log line the feedback is about (must be non-empty).
        true_severity: The ground-truth severity label from ops.
        true_category: Optional ground-truth category label. When omitted the
            service falls back to the model's predicted category for the retrain
            record.
        timestamp: Optional ISO-8601 timestamp for temporal features.
    """

    raw_log: str = Field(
        ...,
        min_length=1,
        description="Raw log line the ground-truth feedback applies to.",
        examples=["Database connection failed with timeout error"],
    )
    true_severity: str = Field(
        ...,
        description="Ground-truth severity label supplied by ops.",
        examples=["ERROR"],
    )
    true_category: Optional[str] = Field(
        default=None,
        description="Optional ground-truth category label (falls back to the "
        "predicted category when omitted).",
        examples=["SYSTEM"],
    )
    timestamp: Optional[str] = Field(
        default=None,
        description="Optional ISO-8601 timestamp for temporal features.",
        examples=["2026-06-18T12:34:56"],
    )


class FeedbackResponse(BaseModel):
    """The result of recording one ground-truth feedback (``POST /feedback``).

    Echoes what the live model predicted for the log versus the truth ops
    supplied, the post-update recent accuracy of the drift monitor, and whether
    this submission pushed accuracy below the threshold and consequently kicked
    off a background retrain.

    Attributes:
        recorded: ``True`` once the feedback has been folded into the monitor.
        predicted_severity: The severity the current model assigned to the log.
        true_severity: The ground-truth severity from the request (echoed back).
        correct: ``True`` if ``predicted_severity == true_severity``.
        recent_accuracy: The drift monitor's recent-window accuracy *after* this
            feedback (``1.0`` while the window is still empty).
        retrain_triggered: ``True`` if this feedback caused a background retrain to
            be launched (recent accuracy dropped below the threshold with a full
            window and no retrain already running).
    """

    recorded: bool = Field(
        ..., description="True once the feedback was recorded in the monitor."
    )
    predicted_severity: str = Field(
        ..., description="Severity the current model predicted for the log."
    )
    true_severity: str = Field(
        ..., description="Ground-truth severity from the request (echoed)."
    )
    correct: bool = Field(
        ..., description="True if the prediction matched the ground truth."
    )
    recent_accuracy: float = Field(
        ..., description="Drift-monitor recent accuracy after this feedback."
    )
    retrain_triggered: bool = Field(
        ..., description="True if this feedback launched a background retrain."
    )


class AdaptiveStatusResponse(BaseModel):
    """The drift-monitor snapshot plus training flag, for ``GET /adaptive/status``.

    Mirrors :meth:`src.adaptive.DriftMonitor.snapshot` field-for-field and adds
    ``is_training`` so a client can see, in one call, both the current drift signal
    and whether an (auto- or manually-triggered) retrain is in flight.

    Attributes:
        recent_accuracy: Mean correctness over the monitor's current window
            (``1.0`` when the window is empty).
        window_size: Number of feedback bits currently held in the window.
        window_capacity: The configured window size (``cfg.drift_window``).
        threshold: The recent-accuracy floor below which a retrain is triggered.
        total_feedback: Lifetime count of feedback submissions recorded.
        retrains_triggered: Lifetime count of retrains the monitor has signalled.
        is_window_full: ``True`` once ``window_size == window_capacity``.
        is_training: ``True`` while a background retrain is in progress.
    """

    recent_accuracy: float = Field(
        ..., description="Mean correctness over the current window (1.0 if empty)."
    )
    window_size: int = Field(
        ..., description="Number of feedback bits currently in the window."
    )
    window_capacity: int = Field(
        ..., description="Configured window size (cfg.drift_window)."
    )
    threshold: float = Field(
        ..., description="Recent-accuracy floor that triggers a retrain."
    )
    total_feedback: int = Field(
        ..., description="Lifetime number of feedback submissions recorded."
    )
    retrains_triggered: int = Field(
        ..., description="Lifetime number of retrains signalled by the monitor."
    )
    is_window_full: bool = Field(
        ..., description="True once the window has reached its capacity."
    )
    is_training: bool = Field(
        ..., description="True while a background retrain is in progress."
    )


# -- Commit 13: A/B testing + graceful fallback (Feature Area C) ------------


class ABClassifyResponse(BaseModel):
    """The result of an A/B-routed classification (``POST /classify/ab``).

    The five :class:`ClassifyResponse` fields (so the underlying model output
    validates directly) **plus** three serving fields describing *which* version of
    the A/B pair actually answered and whether a graceful fallback kicked in.

    Attributes:
        severity: Predicted severity label.
        category: Predicted category label.
        confidence: Overall confidence (mean of the two per-axis confidences).
        severity_confidence: Max soft-voting probability for the severity axis.
        category_confidence: Max soft-voting probability for the category axis.
        model_version: The registry version id that actually served this request
            (the assigned version, or the fallback version when it was used).
        ab_group: The A/B group the request was routed to — ``"A"`` (champion) or
            ``"B"`` (challenger).
        fallback_used: ``True`` if the assigned version could not serve and another
            version answered instead (graceful fallback).
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
    model_version: str = Field(
        ..., description="Registry version id that actually served the request."
    )
    ab_group: str = Field(
        ..., description='A/B group the request was routed to: "A" or "B".'
    )
    fallback_used: bool = Field(
        ...,
        description="True if the assigned version failed and a fallback answered.",
    )


class PromoteRequest(BaseModel):
    """The body for ``POST /models/promote`` — promote a version to champion.

    Attributes:
        version: The registry version id to promote (make ``current`` and group A).
    """

    version: str = Field(
        ...,
        min_length=1,
        description="Registry version id to promote to champion.",
        examples=["v2"],
    )


class ABConfigRequest(BaseModel):
    """The body for ``POST /models/ab`` — (re)configure the A/B router.

    Every field is optional; a ``None`` field leaves that part of the router's
    configuration unchanged. So ``{"split_b": 0.2}`` only shifts the split while
    keeping the current champion/challenger.

    Attributes:
        a_version: New champion (group A) version id, or ``None`` to leave as-is.
        b_version: New challenger (group B) version id, or ``None`` to leave as-is.
        split_b: New fraction of traffic to route to B (challenger), in ``[0, 1]``,
            or ``None`` to leave as-is.
    """

    a_version: Optional[str] = Field(
        default=None, description="New champion (group A) version id."
    )
    b_version: Optional[str] = Field(
        default=None, description="New challenger (group B) version id."
    )
    split_b: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="New fraction of traffic to route to group B (0..1).",
        examples=[0.5],
    )


class FeatureImportance(BaseModel):
    """A single ``(feature, importance)`` pair for the dashboard viz.

    Mirrors one entry of :meth:`src.ensemble.LogClassifier.feature_importance`.

    Attributes:
        name: The engineered feature name (e.g. ``"tfidf__timeout"`` or a dense
            column like ``"msg_len"``).
        importance: The RandomForest Gini importance for that feature (``>= 0``).
    """

    name: str = Field(..., description="Engineered feature name.")
    importance: float = Field(
        ..., description="RandomForest feature importance (>= 0)."
    )


class FeatureImportanceResponse(BaseModel):
    """The payload returned by ``GET /feature-importance``.

    The top-N most important engineered features (by the severity ensemble's
    RandomForest ``feature_importances_``), already sorted descending, plus the
    model version they were read from. Empty ``features`` when the model exposes
    no importances (e.g. mid-train or a degenerate fit).

    Attributes:
        features: Top features sorted by importance descending (``<= top``).
        model_version: The registry's active (champion) version id, or ``None``.
    """

    features: list[FeatureImportance] = Field(
        ..., description="Top features by importance, sorted descending."
    )
    model_version: Optional[str] = Field(
        default=None, description="Active registry version id the importances came from."
    )


class ModelsResponse(BaseModel):
    """The A/B + registry view returned by ``GET /models`` and the model-admin routes.

    Lists every registry version (each annotated with ``is_champion`` / ``ab_group``
    / ``serving_metrics`` by :meth:`src.serving.ABRouter.models`) alongside the
    current A/B configuration (champion, the two group versions, and the split).

    Attributes:
        models: Annotated per-version dicts (plain dicts; see
            :meth:`src.serving.ABRouter.models`). Empty when no model is trained.
        champion: The registry's current (champion) version id, or ``None``.
        a_version: The version id serving as group A, or ``None`` if unconfigured.
        b_version: The version id serving as group B, or ``None`` if unconfigured.
        split_b: Fraction of traffic routed to group B (challenger), in ``[0, 1]``.
    """

    models: list[dict[str, Any]] = Field(
        ..., description="Annotated registry versions (is_champion/ab_group/metrics)."
    )
    champion: Optional[str] = Field(
        default=None, description="Current champion (registry 'current') version id."
    )
    a_version: Optional[str] = Field(
        default=None, description="Version id serving as group A, or None."
    )
    b_version: Optional[str] = Field(
        default=None, description="Version id serving as group B, or None."
    )
    split_b: float = Field(
        ..., description="Fraction of traffic routed to group B (challenger)."
    )

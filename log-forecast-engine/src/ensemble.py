"""Ensemble blending, confidence scoring, alert tiering, and multi-window forecasts.

This module (C7) is the layer that turns the individual fitted forecasters
(ARIMA, exponential smoothing, linear, XGBoost — all :class:`BaseForecaster`
members) into a single, decision-ready forecast. It implements every ensemble
requirement from ``project_requirements.md``:

* **Weighted averaging** — :func:`combine_forecasts` blends per-step point
  forecasts using configured (or supplied) per-model weights, renormalised over
  only the models actually available.
* **Confidence scoring** — :func:`compute_confidence` produces a per-step
  confidence in ``[0, 1]`` from FOUR signals: cross-model *agreement*, recent
  *accuracy*, *data quality*, and *pattern stability* (the four signals the
  requirements name), with a mild horizon taper so further-ahead steps are less
  certain.
* **Alert tiering** — :func:`alert_level` maps a representative confidence scalar
  to ``"high"`` (>0.85 -> auto-action), ``"medium"`` (0.65–0.85 -> notify), or
  ``"low"`` (<0.65 -> no alert), using the thresholds from
  :class:`~src.config.Settings` (overridable for runtime config in C11).
* **Graceful degradation** — :func:`ensemble_forecast` catches a per-model
  :class:`~src.models.base.ForecastError` (or any exception), drops that member,
  renormalises the weights over the survivors, and still returns a forecast. If
  *every* model fails it returns a safe zeroed result (alert ``"low"``,
  confidence ``0.0``) and **never raises**.
* **Multi-window strategy** (Feature Area C) — :func:`multi_window_ensemble`
  trains the ensemble on several training windows simultaneously (a short recent
  window for near-term precision plus a longer window for trend awareness) and
  blends the per-window ensemble forecasts.

Design contract
---------------
* **Pure logic + model calls.** No DB, no API, no Celery, no Redis (that is C8).
  The core scoring functions take ``data_quality`` / ``pattern_stability`` as
  plain scalars so they are trivially testable; callers compute them with
  :func:`src.features.data_quality_score` /
  :func:`src.features.pattern_stability_score`.
* **All confidence values are strictly in ``[0, 1]``** and alert tiers are
  exactly ``"high"`` / ``"medium"`` / ``"low"``.
* **Deterministic** given the same fitted models and inputs.
* **JSON-serialisable output.** :func:`ensemble_forecast` returns plain Python
  ``float`` / ``list`` values (numpy is converted) so C8 can persist/cache and
  the API can return them directly, matching ``project_requirements.md`` §8.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Sequence

import numpy as np

from src import features
from src.config import get_settings
from src.models.base import BaseForecaster, ForecastError

# Small epsilon to guard divisions by (near-)zero magnitudes.
_EPS = 1e-9

# ---------------------------------------------------------------------------
# Confidence-blend weights (document the exact formula here).
#
# Final per-step confidence is an ARITHMETIC weighted mean of the four signals
# the requirements name, then tapered by a mild horizon decay:
#
#     base[t] = W_AGREE * agreement[t]
#             + W_ACCURACY * recent_accuracy
#             + W_QUALITY  * data_quality
#             + W_STABILITY * pattern_stability
#
#     confidence[t] = clamp01( base[t] * taper[t] )
#
# where agreement[t] is per-step (cross-model spread, see _model_agreement),
# the other three signals are scalars broadcast across steps, and
#
#     taper[t] = HORIZON_FLOOR + (1 - HORIZON_FLOOR) * HORIZON_DECAY ** t
#
# (t = 0 for the first step). taper starts at 1.0 for step 0 and decays
# geometrically towards HORIZON_FLOOR, so confidence gently drops for
# further-ahead steps but never collapses to zero from the taper alone.
# ---------------------------------------------------------------------------
W_AGREE = 0.40
W_ACCURACY = 0.30
W_QUALITY = 0.15
W_STABILITY = 0.15

# Neutral prior used when recent accuracy is not supplied.
DEFAULT_RECENT_ACCURACY = 0.7

# Fallback agreement for the single-model case (agreement across models is
# undefined with one member, so we use a moderate prior rather than 1.0).
SINGLE_MODEL_AGREEMENT = 0.6

# Horizon taper parameters (mild): step-0 multiplier is 1.0, decaying towards
# HORIZON_FLOOR with ratio HORIZON_DECAY per step.
HORIZON_DECAY = 0.995
HORIZON_FLOOR = 0.6

# Number of leading steps averaged by aggregate_confidence for the alert tier.
AGGREGATE_HEAD_STEPS = 12


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _clamp01(x: float) -> float:
    """Clamp ``x`` to ``[0, 1]``; map non-finite values to ``0.0``."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _to_float_list(arr: object) -> list[float]:
    """Convert an array-like to a JSON-serialisable list of finite floats."""
    a = np.asarray(arr, dtype=float).ravel()
    a = np.where(np.isfinite(a), a, 0.0)
    return [float(v) for v in a]


def _common_models(
    individual_forecasts: dict[str, np.ndarray], weights: dict[str, float]
) -> list[str]:
    """Names present in BOTH dicts with a positive, finite weight, sorted.

    Sorting keeps the output deterministic regardless of dict insertion order.
    """
    names = []
    for name in individual_forecasts:
        if name not in weights:
            continue
        w = weights.get(name)
        try:
            wf = float(w)
        except (TypeError, ValueError):
            continue
        if math.isfinite(wf) and wf > 0.0:
            names.append(name)
    return sorted(names)


def _renormalise(weights: dict[str, float], names: Sequence[str]) -> dict[str, float]:
    """Renormalise ``weights`` over ``names`` so they sum to 1.0.

    Falls back to a uniform split when the selected weights are all zero / the
    total is non-finite, so the result always sums to 1.0 (or is empty).
    """
    names = list(names)
    if not names:
        return {}
    selected = {n: max(0.0, float(weights.get(n, 0.0))) for n in names}
    total = float(sum(selected.values()))
    if not math.isfinite(total) or total <= 0.0:
        uniform = 1.0 / len(names)
        return {n: uniform for n in names}
    return {n: selected[n] / total for n in names}


def _min_length(forecasts: dict[str, np.ndarray], names: Sequence[str]) -> int:
    """Minimum forecast length across ``names`` (0 if none / any empty)."""
    lengths = []
    for n in names:
        arr = np.asarray(forecasts[n], dtype=float).ravel()
        lengths.append(arr.size)
    if not lengths:
        return 0
    return int(min(lengths))


def _stacked(
    forecasts: dict[str, np.ndarray], names: Sequence[str], length: int
) -> np.ndarray:
    """Stack the named forecasts into a ``(len(names), length)`` float matrix."""
    rows = [np.asarray(forecasts[n], dtype=float).ravel()[:length] for n in names]
    return np.vstack(rows) if rows else np.empty((0, length))


# ---------------------------------------------------------------------------
# 1. Weighted ensemble combination
# ---------------------------------------------------------------------------
def combine_forecasts(
    individual_forecasts: dict[str, np.ndarray], weights: dict[str, float]
) -> np.ndarray:
    """Weighted per-step average of the member forecasts.

    Only models present in **both** ``individual_forecasts`` and ``weights``
    (with a positive, finite weight) are included; their weights are
    renormalised to sum to 1.0 over that available subset. Forecasts of
    differing lengths are **truncated to the minimum length** across the included
    models so every member contributes at every retained step.

    Args:
        individual_forecasts: ``{model_name: point_forecast_array}``.
        weights: ``{model_name: weight}`` (renormalised internally).

    Returns:
        The ensemble point-forecast array (length == the min member length, or an
        empty array if no models are usable).
    """
    names = _common_models(individual_forecasts, weights)
    if not names:
        return np.empty(0, dtype=float)

    length = _min_length(individual_forecasts, names)
    if length == 0:
        return np.empty(0, dtype=float)

    norm = _renormalise(weights, names)
    matrix = _stacked(individual_forecasts, names, length)  # (m, length)
    w_vec = np.array([norm[n] for n in names], dtype=float).reshape(-1, 1)
    ensemble = np.sum(matrix * w_vec, axis=0)
    # Guard any stray non-finite entries from a misbehaving member.
    ensemble = np.where(np.isfinite(ensemble), ensemble, 0.0)
    return ensemble.astype(float)


# ---------------------------------------------------------------------------
# 2. Confidence scoring
# ---------------------------------------------------------------------------
def _model_agreement(
    individual_forecasts: dict[str, np.ndarray], weights: dict[str, float]
) -> np.ndarray:
    """Per-step model-agreement signal in ``[0, 1]`` (higher = models agree).

    For each step, agreement is ``1 / (1 + cv)`` where ``cv`` is the
    weight-aware coefficient of variation of the member forecasts at that step:
    ``cv = weighted_std / (|weighted_mean| + eps)``. When all models predict the
    same value the spread is zero and agreement is ``1.0``; as the models diverge
    relative to the magnitude, agreement falls towards ``0``.

    With a **single** usable model the spread is undefined, so a moderate prior
    (:data:`SINGLE_MODEL_AGREEMENT`) is broadcast across all steps. With no
    usable model an empty array is returned.
    """
    names = _common_models(individual_forecasts, weights)
    if not names:
        return np.empty(0, dtype=float)

    length = _min_length(individual_forecasts, names)
    if length == 0:
        return np.empty(0, dtype=float)

    if len(names) == 1:
        return np.full(length, SINGLE_MODEL_AGREEMENT, dtype=float)

    norm = _renormalise(weights, names)
    matrix = _stacked(individual_forecasts, names, length)  # (m, length)
    w_vec = np.array([norm[n] for n in names], dtype=float).reshape(-1, 1)

    weighted_mean = np.sum(matrix * w_vec, axis=0)  # (length,)
    diff = matrix - weighted_mean.reshape(1, -1)
    weighted_var = np.sum(w_vec * diff**2, axis=0)
    weighted_std = np.sqrt(np.maximum(weighted_var, 0.0))

    denom = np.abs(weighted_mean) + _EPS
    cv = weighted_std / denom
    agreement = 1.0 / (1.0 + cv)
    agreement = np.where(np.isfinite(agreement), agreement, 0.0)
    return np.clip(agreement, 0.0, 1.0).astype(float)


def _interval_agreement(
    intervals: dict[str, tuple[np.ndarray, np.ndarray]],
    ensemble: np.ndarray,
    weights: dict[str, float],
) -> np.ndarray | None:
    """Fallback per-step agreement derived from interval width (single-model case).

    Narrow prediction intervals relative to the point magnitude imply higher
    certainty. Returns ``1 / (1 + relative_half_width)`` per step, or ``None`` if
    no usable intervals are present.
    """
    if not intervals:
        return None
    length = ensemble.size
    if length == 0:
        return None

    names = [n for n in intervals if n in weights]
    if not names:
        names = list(intervals)
    if not names:
        return None

    half_widths = []
    for n in names:
        try:
            lower, upper = intervals[n]
        except (TypeError, ValueError):
            continue
        lo = np.asarray(lower, dtype=float).ravel()[:length]
        hi = np.asarray(upper, dtype=float).ravel()[:length]
        if lo.size < length or hi.size < length:
            continue
        half_widths.append((hi - lo) / 2.0)
    if not half_widths:
        return None

    mean_hw = np.mean(np.vstack(half_widths), axis=0)
    denom = np.abs(ensemble[:length]) + _EPS
    rel = np.abs(mean_hw) / denom
    agreement = 1.0 / (1.0 + rel)
    agreement = np.where(np.isfinite(agreement), agreement, 0.0)
    return np.clip(agreement, 0.0, 1.0).astype(float)


def _weighted_recent_accuracy(
    recent_accuracy: dict[str, float] | None, weights: dict[str, float]
) -> float:
    """Collapse per-model recent accuracy into one ``[0, 1]`` scalar.

    Weighted by the (renormalised) model weights over the models that appear in
    ``recent_accuracy``. Returns :data:`DEFAULT_RECENT_ACCURACY` (a neutral
    prior) when no recent-accuracy data is supplied.
    """
    if not recent_accuracy:
        return DEFAULT_RECENT_ACCURACY

    names = [n for n in recent_accuracy if n in weights]
    if not names:
        # No overlap with weights -> simple mean of supplied accuracies.
        vals = [_clamp01(v) for v in recent_accuracy.values()]
        return float(np.mean(vals)) if vals else DEFAULT_RECENT_ACCURACY

    norm = _renormalise(weights, names)
    acc = sum(norm[n] * _clamp01(recent_accuracy[n]) for n in names)
    return _clamp01(acc)


def compute_confidence(
    individual_forecasts: dict[str, np.ndarray],
    ensemble: np.ndarray,
    weights: dict[str, float],
    *,
    recent_accuracy: dict[str, float] | None = None,
    data_quality: float = 1.0,
    pattern_stability: float = 1.0,
    intervals: dict[str, tuple[np.ndarray, np.ndarray]] | None = None,
) -> np.ndarray:
    """Per-step ensemble confidence in ``[0, 1]`` (length == ``len(ensemble)``).

    Blends the four signals named in ``project_requirements.md`` —
    cross-model **agreement** (per step), recent **accuracy**, **data quality**,
    and **pattern stability** — via an arithmetic weighted mean, then applies a
    mild horizon taper. The exact formula (and the weights
    :data:`W_AGREE` / :data:`W_ACCURACY` / :data:`W_QUALITY` /
    :data:`W_STABILITY`) is documented at the top of this module.

    Args:
        individual_forecasts: ``{model_name: point_forecast_array}``.
        ensemble: The ensemble point forecast (defines the output length).
        weights: ``{model_name: weight}`` used for the weighted agreement /
            accuracy blend.
        recent_accuracy: Optional ``{model_name: accuracy in [0,1]}`` from recent
            backtests; defaults to a neutral prior when omitted.
        data_quality: Scalar in ``[0, 1]`` (e.g.
            :func:`src.features.data_quality_score` of the input series).
        pattern_stability: Scalar in ``[0, 1]`` (e.g.
            :func:`src.features.pattern_stability_score`).
        intervals: Optional ``{model_name: (lower, upper)}`` used to derive an
            agreement fallback when only one model is available.

    Returns:
        A numpy array of per-step confidence values, each clamped to ``[0, 1]``.
        Returns an empty array when ``ensemble`` is empty.
    """
    ensemble = np.asarray(ensemble, dtype=float).ravel()
    n = ensemble.size
    if n == 0:
        return np.empty(0, dtype=float)

    # Signal 1: per-step model agreement.
    agreement = _model_agreement(individual_forecasts, weights)
    usable = _common_models(individual_forecasts, weights)
    if agreement.size == 0:
        agreement = np.full(n, SINGLE_MODEL_AGREEMENT, dtype=float)
    elif len(usable) == 1:
        # Single model: prefer an interval-width-based agreement if available.
        iv = _interval_agreement(intervals or {}, ensemble, weights)
        if iv is not None and iv.size > 0:
            agreement = iv
    # Align agreement length to the ensemble length.
    agreement = _fit_length(agreement, n, fill=SINGLE_MODEL_AGREEMENT)

    # Signals 2-4: scalars broadcast across steps.
    recent_acc = _weighted_recent_accuracy(recent_accuracy, weights)
    dq = _clamp01(data_quality)
    ps = _clamp01(pattern_stability)

    base = (
        W_AGREE * agreement
        + W_ACCURACY * recent_acc
        + W_QUALITY * dq
        + W_STABILITY * ps
    )

    # Horizon taper: gently decay confidence for further-ahead steps.
    steps = np.arange(n, dtype=float)
    taper = HORIZON_FLOOR + (1.0 - HORIZON_FLOOR) * (HORIZON_DECAY**steps)

    confidence = base * taper
    return np.clip(np.where(np.isfinite(confidence), confidence, 0.0), 0.0, 1.0)


def _fit_length(arr: np.ndarray, length: int, *, fill: float) -> np.ndarray:
    """Truncate or edge/fill-pad ``arr`` to exactly ``length`` entries."""
    arr = np.asarray(arr, dtype=float).ravel()
    if arr.size == length:
        return arr
    if arr.size > length:
        return arr[:length]
    pad_value = arr[-1] if arr.size > 0 else fill
    pad = np.full(length - arr.size, pad_value, dtype=float)
    return np.concatenate([arr, pad]) if arr.size > 0 else np.full(length, fill)


# ---------------------------------------------------------------------------
# 3. Alert tiering
# ---------------------------------------------------------------------------
def aggregate_confidence(confidence_array: object) -> float:
    """Collapse a per-step confidence array into one scalar for tiering.

    Uses the **mean of the first** :data:`AGGREGATE_HEAD_STEPS` steps (falling
    back to the whole array when shorter). Near-term steps drive alerting because
    they are the most actionable and least horizon-discounted. Returns ``0.0``
    for an empty array.
    """
    arr = np.asarray(confidence_array, dtype=float).ravel()
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    head = arr[: min(AGGREGATE_HEAD_STEPS, arr.size)]
    return _clamp01(float(np.mean(head)))


def alert_level(
    confidence_scalar: float,
    *,
    high: float | None = None,
    medium: float | None = None,
) -> str:
    """Map a confidence scalar to an alert tier: ``"high"`` / ``"medium"`` / ``"low"``.

    Tiers (per ``project_requirements.md``): ``confidence > high`` -> ``"high"``
    (auto-action), ``medium <= confidence <= high`` -> ``"medium"`` (notify),
    ``confidence < medium`` -> ``"low"`` (no alert).

    Thresholds default to ``get_settings().high_confidence_threshold`` (0.85) and
    ``medium_confidence_threshold`` (0.65) but are overridable via ``high`` /
    ``medium`` for runtime config (C11).
    """
    if high is None or medium is None:
        try:
            settings = get_settings()
            if high is None:
                high = settings.high_confidence_threshold
            if medium is None:
                medium = settings.medium_confidence_threshold
        except Exception:  # noqa: BLE001 - never fail on config access
            high = 0.85 if high is None else high
            medium = 0.65 if medium is None else medium

    try:
        c = float(confidence_scalar)
    except (TypeError, ValueError):
        c = 0.0
    if not math.isfinite(c):
        c = 0.0

    if c > float(high):
        return "high"
    if c >= float(medium):
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# 4. High-level ensemble forecast (with graceful degradation)
# ---------------------------------------------------------------------------
def _resolve_weights(weights: dict[str, float] | None) -> dict[str, float]:
    """Return supplied weights, or the configured ``model_weights`` defaults."""
    if weights:
        return dict(weights)
    try:
        return dict(get_settings().model_weights)
    except Exception:  # noqa: BLE001
        return {}


def _empty_result(
    steps: int, failed: list[str], weights: dict[str, float]
) -> dict[str, Any]:
    """Safe, zeroed result used when every model fails (never raises)."""
    zeros = [0.0] * max(0, int(steps))
    return {
        "steps": int(steps),
        "ensemble_prediction": list(zeros),
        "ensemble_confidence": list(zeros),
        "individual_forecasts": {},
        "alert_level": "low",
        "confidence": 0.0,
        "weights_used": {},
        "failed_models": list(failed),
        "lower": list(zeros),
        "upper": list(zeros),
    }


def ensemble_forecast(
    models: list[BaseForecaster],
    steps: int,
    *,
    weights: dict[str, float] | None = None,
    recent_accuracy: dict[str, float] | None = None,
    data_quality: float = 1.0,
    pattern_stability: float = 1.0,
) -> dict[str, Any]:
    """Blend fitted models into one ensemble forecast with confidence + alert tier.

    Each model's :meth:`~src.models.base.BaseForecaster.predict` and
    :meth:`~src.models.base.BaseForecaster.predict_interval` are called inside a
    try/except: a model that raises :class:`~src.models.base.ForecastError` (or
    any other exception, or is not fitted) is **dropped** and recorded in
    ``failed_models`` — the surviving models still produce a forecast (graceful
    degradation). Weights are renormalised over the survivors. If **every** model
    fails, a safe zeroed result is returned (alert ``"low"``, confidence ``0.0``)
    and no exception is raised.

    Args:
        models: Fitted :class:`BaseForecaster` instances (the ensemble members).
        steps: Number of future steps to forecast (>= 1).
        weights: Optional ``{name: weight}``; defaults to
            ``get_settings().model_weights``.
        recent_accuracy: Optional ``{name: accuracy}`` feeding the confidence
            accuracy signal.
        data_quality: Scalar ``[0, 1]`` data-quality signal.
        pattern_stability: Scalar ``[0, 1]`` pattern-stability signal.

    Returns:
        A JSON-serialisable dict (see module docstring / ``project_requirements``
        §8) with keys: ``steps``, ``ensemble_prediction``, ``ensemble_confidence``,
        ``individual_forecasts``, ``alert_level``, ``confidence``,
        ``weights_used``, ``failed_models``, ``lower``, ``upper``.
    """
    try:
        steps = int(steps)
    except (TypeError, ValueError):
        steps = 0
    base_weights = _resolve_weights(weights)

    if steps < 1 or not models:
        failed = [getattr(m, "name", m.__class__.__name__) for m in (models or [])]
        return _empty_result(max(0, steps), failed, base_weights)

    individual: dict[str, np.ndarray] = {}
    intervals: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    failed: list[str] = []

    for model in models:
        name = getattr(model, "name", model.__class__.__name__)
        try:
            point = np.asarray(model.predict(steps), dtype=float).ravel()
            if point.size == 0 or not np.all(np.isfinite(point)):
                # Treat an unusable point forecast as a failure.
                raise ForecastError(f"{name}: produced an empty/non-finite forecast")
            individual[name] = point
            try:
                lower, upper = model.predict_interval(steps)
                intervals[name] = (
                    np.asarray(lower, dtype=float).ravel(),
                    np.asarray(upper, dtype=float).ravel(),
                )
            except Exception:  # noqa: BLE001 - intervals are best-effort
                pass
        except ForecastError:
            failed.append(name)
        except Exception:  # noqa: BLE001 - a bad member must never crash the ensemble
            failed.append(name)

    if not individual:
        return _empty_result(steps, failed, base_weights)

    survivors = _common_models(individual, base_weights)
    if not survivors:
        # Models produced forecasts but none have a configured positive weight ->
        # fall back to a uniform blend over all surviving forecasts.
        survivors = sorted(individual)
        base_weights = {n: 1.0 for n in survivors}

    weights_used = _renormalise(base_weights, survivors)

    ensemble = combine_forecasts(individual, weights_used)
    confidence = compute_confidence(
        individual,
        ensemble,
        weights_used,
        recent_accuracy=recent_accuracy,
        data_quality=data_quality,
        pattern_stability=pattern_stability,
        intervals=intervals,
    )

    lower, upper = _combine_intervals(intervals, weights_used, ensemble)

    conf_scalar = aggregate_confidence(confidence)
    level = alert_level(conf_scalar)

    return {
        "steps": int(steps),
        "ensemble_prediction": _to_float_list(ensemble),
        "ensemble_confidence": _to_float_list(confidence),
        "individual_forecasts": {
            n: _to_float_list(individual[n]) for n in sorted(individual)
        },
        "alert_level": level,
        "confidence": float(conf_scalar),
        "weights_used": {n: float(w) for n, w in weights_used.items()},
        "failed_models": list(failed),
        "lower": _to_float_list(lower),
        "upper": _to_float_list(upper),
    }


def _combine_intervals(
    intervals: dict[str, tuple[np.ndarray, np.ndarray]],
    weights: dict[str, float],
    ensemble: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Weighted blend of member prediction intervals.

    Blends the lower / upper bounds with the same (renormalised) weights as the
    point forecast, over the models that have both a weight and an interval. When
    no member supplied an interval the ensemble point forecast is returned for
    both bounds (a zero-width band).
    """
    n = ensemble.size
    if n == 0:
        return np.empty(0, dtype=float), np.empty(0, dtype=float)

    names = [k for k in intervals if k in weights]
    if not names:
        return ensemble.copy(), ensemble.copy()

    norm = _renormalise(weights, names)
    lo_acc = np.zeros(n, dtype=float)
    hi_acc = np.zeros(n, dtype=float)
    total = 0.0
    for name in names:
        lower, upper = intervals[name]
        lo = np.asarray(lower, dtype=float).ravel()[:n]
        hi = np.asarray(upper, dtype=float).ravel()[:n]
        if lo.size < n or hi.size < n:
            continue
        w = norm[name]
        lo_acc += w * lo
        hi_acc += w * hi
        total += w

    if total <= 0.0:
        return ensemble.copy(), ensemble.copy()
    lo_acc /= total
    hi_acc /= total
    # Guarantee lower <= upper element-wise.
    lower_final = np.minimum(lo_acc, hi_acc)
    upper_final = np.maximum(lo_acc, hi_acc)
    return lower_final, upper_final


# ---------------------------------------------------------------------------
# 5. Multi-window strategy (Feature Area C)
# ---------------------------------------------------------------------------
def multi_window_ensemble(
    model_factory: Callable[[], list[BaseForecaster]],
    series: object,
    steps: int,
    windows: list[int],
    *,
    weights: dict[str, float] | None = None,
    recent_accuracy: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Train + forecast the ensemble on several recent-data windows and blend them.

    Implements the "use multiple training windows simultaneously (short-term
    precision + long-term trend awareness) and combine them" requirement. Each
    entry in ``windows`` is a **number of most-recent points** to keep; the
    series is sliced to that tail, a fresh set of models from ``model_factory``
    is fitted on the slice, and an :func:`ensemble_forecast` is produced.

    Blending strategy
    -----------------
    The per-window ensemble forecasts are combined per step with a **horizon-aware
    weight**: the *shortest* window (most recent, highest precision) dominates the
    earliest steps, while *longer* windows (more trend context) gain influence for
    further-ahead steps. Concretely, for window ``i`` with length-rank
    ``r_i`` (0 = shortest) the per-step weight is

        w_i[t] = base_i * (1 + r_i * t / steps)

    renormalised across windows at each step. ``base_i`` is uniform across
    surviving windows. Confidence and intervals are blended with the same
    per-step weights; the alert tier is recomputed from the blended confidence.
    Data-quality / pattern-stability are computed on each window's slice and
    folded into that window's confidence inside :func:`ensemble_forecast`.

    Robustness: a window longer than the series uses the full series; a window
    whose ensemble fully fails is dropped. If **all** windows fail, a safe zeroed
    result is returned (never raises).

    Args:
        model_factory: Callable returning a fresh list of *unfitted*
            :class:`BaseForecaster` instances (one fresh set per window).
        series: Any input accepted by :func:`src.features.to_series`.
        steps: Forecast horizon in steps (>= 1).
        windows: List of window sizes (number of most-recent points).
        weights: Optional model weights passed through to each window's ensemble.
        recent_accuracy: Optional per-model recent accuracy passed through.

    Returns:
        The same dict shape as :func:`ensemble_forecast`, plus a ``windows_used``
        field listing the effective (clamped) window sizes that succeeded.
    """
    try:
        steps = int(steps)
    except (TypeError, ValueError):
        steps = 0
    base_weights = _resolve_weights(weights)

    if steps < 1 or not windows:
        return {**_empty_result(max(0, steps), [], base_weights), "windows_used": []}

    # Normalise the input series once.
    try:
        s = features.to_series(series)
    except (ValueError, TypeError):
        return {**_empty_result(steps, [], base_weights), "windows_used": []}

    n = len(s)
    # Clamp windows to the series length and de-duplicate while preserving order.
    clamped: list[int] = []
    seen: set[int] = set()
    for w in windows:
        try:
            wi = int(w)
        except (TypeError, ValueError):
            continue
        if wi < 2:
            continue
        eff = min(wi, n)
        if eff not in seen:
            seen.add(eff)
            clamped.append(eff)
    if not clamped:
        return {**_empty_result(steps, [], base_weights), "windows_used": []}

    # Run one ensemble per window. Rank windows by size for horizon-aware blending.
    order = sorted(range(len(clamped)), key=lambda i: clamped[i])
    rank = {idx: r for r, idx in enumerate(order)}

    per_window: list[dict[str, Any]] = []
    used_windows: list[int] = []
    failed_all: set[str] = set()

    for i, win in enumerate(clamped):
        slice_s = s.iloc[-win:]
        try:
            models = model_factory()
        except Exception:  # noqa: BLE001
            models = []
        fitted: list[BaseForecaster] = []
        for model in models:
            try:
                model.fit(slice_s)
                fitted.append(model)
            except Exception:  # noqa: BLE001 - drop a model that won't fit
                failed_all.add(getattr(model, "name", model.__class__.__name__))

        if not fitted:
            continue

        dq = features.data_quality_score(slice_s)
        ps = features.pattern_stability_score(slice_s)
        result = ensemble_forecast(
            fitted,
            steps,
            weights=base_weights,
            recent_accuracy=recent_accuracy,
            data_quality=dq,
            pattern_stability=ps,
        )
        for fm in result.get("failed_models", []):
            failed_all.add(fm)
        # Only keep windows that produced a real (non-empty) forecast.
        if result.get("individual_forecasts"):
            result["_rank"] = rank[i]
            per_window.append(result)
            used_windows.append(win)

    if not per_window:
        return {
            **_empty_result(steps, sorted(failed_all), base_weights),
            "windows_used": [],
        }

    blended = _blend_windows(per_window, steps)
    blended["failed_models"] = sorted(failed_all)
    blended["windows_used"] = used_windows
    return blended


def _blend_windows(per_window: list[dict[str, Any]], steps: int) -> dict[str, Any]:
    """Combine per-window ensemble results with horizon-aware weights.

    See :func:`multi_window_ensemble` for the weighting rationale. Returns a dict
    in the :func:`ensemble_forecast` shape (``windows_used`` / ``failed_models``
    are filled by the caller).
    """
    m = len(per_window)
    t = np.arange(steps, dtype=float)
    frac = t / steps if steps > 0 else t  # 0..~1 across the horizon

    # Per-window, per-step weight matrix (m, steps).
    weight_rows = []
    for res in per_window:
        r = float(res.get("_rank", 0))
        base = 1.0 / m
        # Longer windows (higher rank) gain weight further into the horizon.
        weight_rows.append(base * (1.0 + r * frac))
    wmat = np.vstack(weight_rows)  # (m, steps)
    col_sums = np.sum(wmat, axis=0)
    col_sums = np.where(col_sums > 0.0, col_sums, 1.0)
    wmat = wmat / col_sums.reshape(1, -1)

    def _stack(field: str) -> np.ndarray:
        rows = []
        for res in per_window:
            arr = np.asarray(res.get(field, []), dtype=float).ravel()
            rows.append(_fit_length(arr, steps, fill=0.0))
        return np.vstack(rows) if rows else np.zeros((m, steps))

    ens = np.sum(wmat * _stack("ensemble_prediction"), axis=0)
    conf = np.sum(wmat * _stack("ensemble_confidence"), axis=0)
    lower = np.sum(wmat * _stack("lower"), axis=0)
    upper = np.sum(wmat * _stack("upper"), axis=0)
    conf = np.clip(np.where(np.isfinite(conf), conf, 0.0), 0.0, 1.0)
    lo = np.minimum(lower, upper)
    hi = np.maximum(lower, upper)

    # Merge individual forecasts across windows (average a model that appears in
    # multiple windows, so the per-member breakdown stays representative).
    merged_individual: dict[str, list[np.ndarray]] = {}
    for res in per_window:
        for name, vals in res.get("individual_forecasts", {}).items():
            arr = _fit_length(np.asarray(vals, dtype=float).ravel(), steps, fill=0.0)
            merged_individual.setdefault(name, []).append(arr)
    individual = {
        name: _to_float_list(np.mean(np.vstack(rows), axis=0))
        for name, rows in sorted(merged_individual.items())
    }

    # Merge weights_used across windows (mean of each model's blend weight).
    merged_weights: dict[str, list[float]] = {}
    for res in per_window:
        for name, w in res.get("weights_used", {}).items():
            merged_weights.setdefault(name, []).append(float(w))
    weights_used = {
        name: float(np.mean(ws)) for name, ws in sorted(merged_weights.items())
    }

    conf_scalar = aggregate_confidence(conf)
    level = alert_level(conf_scalar)

    return {
        "steps": int(steps),
        "ensemble_prediction": _to_float_list(ens),
        "ensemble_confidence": _to_float_list(conf),
        "individual_forecasts": individual,
        "alert_level": level,
        "confidence": float(conf_scalar),
        "weights_used": weights_used,
        "failed_models": [],
        "lower": _to_float_list(lo),
        "upper": _to_float_list(hi),
    }


__all__ = [
    "combine_forecasts",
    "compute_confidence",
    "alert_level",
    "aggregate_confidence",
    "ensemble_forecast",
    "multi_window_ensemble",
]

"""Short-horizon load forecasting via Holt's linear trend method.

This module is **pure math** — it depends only on :mod:`numpy` and the standard
library, performs no I/O, and imports nothing else from :mod:`src`. It exposes:

* :func:`holt_forecast` — double exponential smoothing (level + trend) that
  projects a series ``horizon_steps`` into the future.
* :func:`confidence` — a 0-1 score combining series stability (inverse
  coefficient of variation) with recent forecast accuracy.
* :func:`build_forecast` — an assembler that merges the two into the canonical
  forecast payload consumed by the scaler and dashboard.

All functions are defensive: empty, single-element, or otherwise degenerate
inputs return sensible neutral values rather than raising.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

__all__ = ["holt_forecast", "confidence", "build_forecast"]

# Deadband used to classify a slope as effectively flat. Scaled by the level so
# that "flat" is judged relative to the magnitude of the signal, not absolutely.
_FLAT_REL_EPS = 1e-6

# Small constant guarding divisions by (near-)zero magnitudes.
_DIV_EPS = 1e-6


def _as_float_array(series: Sequence[float] | np.ndarray) -> np.ndarray:
    """Coerce ``series`` to a 1-D float ``ndarray`` (empty allowed)."""
    arr = np.asarray(series, dtype=float).ravel()
    return arr


def _classify_trend(slope: float, level: float) -> str:
    """Return ``"rising"`` / ``"falling"`` / ``"flat"`` for ``slope``.

    The flat deadband scales with the level so a slope is called flat when it is
    negligible relative to the magnitude of the signal (``|slope| <
    1e-6 * max(1, |level|)``).
    """
    deadband = _FLAT_REL_EPS * max(1.0, abs(level))
    if abs(slope) < deadband:
        return "flat"
    return "rising" if slope > 0.0 else "falling"


def holt_forecast(
    series: Sequence[float] | np.ndarray,
    horizon_steps: int,
    alpha: float = 0.25,
    beta: float = 0.10,
) -> dict:
    """Forecast ``horizon_steps`` ahead with Holt's double exponential smoothing.

    The recurrence (for observations ``x_0 .. x_{n-1}``) is::

        s_0 = x_0
        b_0 = x_1 - x_0                              # 0.0 when n < 2
        s_t = alpha * x_t + (1 - alpha) * (s_{t-1} + b_{t-1})
        b_t = beta  * (s_t - s_{t-1}) + (1 - beta) * b_{t-1}
        F   = s_last + horizon_steps * b_last

    Args:
        series: Observed values, oldest first. May be empty.
        horizon_steps: Number of steps ahead to project the trend.
        alpha: Level smoothing factor in ``[0, 1]``.
        beta: Trend smoothing factor in ``[0, 1]``.

    Returns:
        A dict with keys ``current``, ``predicted``, ``level``, ``slope`` and
        ``trend``. ``predicted`` is floored at ``0.0`` (load cannot be negative).
        Degenerate inputs never raise:

        * empty series -> all-zero values, ``trend="flat"``.
        * single element -> ``level``/``current``/``predicted`` equal that value,
          ``slope=0.0``, ``trend="flat"``.
    """
    x = _as_float_array(series)
    n = x.size

    if n == 0:
        return {
            "current": 0.0,
            "predicted": 0.0,
            "level": 0.0,
            "slope": 0.0,
            "trend": "flat",
        }

    current = float(x[-1])

    if n == 1:
        return {
            "current": current,
            "predicted": current,
            "level": current,
            "slope": 0.0,
            "trend": "flat",
        }

    # Initialise level and trend from the first two observations.
    level = float(x[0])
    slope = float(x[1] - x[0])

    for t in range(1, n):
        prev_level = level
        level = alpha * float(x[t]) + (1.0 - alpha) * (prev_level + slope)
        slope = beta * (level - prev_level) + (1.0 - beta) * slope

    forecast = level + horizon_steps * slope
    predicted = max(0.0, forecast)

    return {
        "current": current,
        "predicted": float(predicted),
        "level": float(level),
        "slope": float(slope),
        "trend": _classify_trend(slope, level),
    }


def confidence(
    series: Sequence[float] | np.ndarray,
    recent_residuals: Sequence[float] | None = None,
) -> float:
    """Compute a forecast confidence score in ``[0, 1]``.

    The score blends two components:

    * **Stability** — inverse coefficient of variation of the series:
      ``cv = std / |mean|`` (``cv = 0`` when ``std == 0`` and ``mean == 0``,
      else ``1.0`` when only ``mean == 0``); ``stability = 1 / (1 + cv)``. A
      perfectly flat series scores ``stability = 1.0``; a noisy one scores lower.
    * **Residual accuracy** — when ``recent_residuals`` provides at least two
      forecast-error magnitudes, ``resid_score = 1 / (1 + mean(|residuals|) /
      (|mean(series)| + eps))``. Smaller recent errors -> higher score.

    Blending: ``0.6 * stability + 0.4 * resid_score`` when residuals are present,
    otherwise ``0.6 * stability + 0.4 * 0.5`` (a neutral residual prior).

    Args:
        series: The observed series the forecast was built from.
        recent_residuals: Optional list of recent forecast-error magnitudes. Only
            used when it contains two or more entries.

    Returns:
        A Python float clipped to ``[0, 1]``. Series with fewer than two points
        return ``0.0`` (insufficient data for a meaningful estimate).
    """
    x = _as_float_array(series)
    if x.size < 2:
        return 0.0

    mean = float(np.mean(x))
    std = float(np.std(x))

    if mean != 0.0:
        cv = std / abs(mean)
    else:
        cv = 0.0 if std == 0.0 else 1.0
    stability = 1.0 / (1.0 + cv)

    residuals = _as_float_array(recent_residuals) if recent_residuals is not None else None
    if residuals is not None and residuals.size >= 2:
        mean_abs_resid = float(np.mean(np.abs(residuals)))
        resid_score = 1.0 / (1.0 + mean_abs_resid / (abs(mean) + _DIV_EPS))
        result = 0.6 * stability + 0.4 * resid_score
    else:
        result = 0.6 * stability + 0.4 * 0.5

    return float(np.clip(result, 0.0, 1.0))


def build_forecast(
    series: Sequence[float] | np.ndarray,
    horizon_steps: int,
    *,
    metric: str,
    horizon_minutes: int,
    alpha: float = 0.25,
    beta: float = 0.10,
    recent_residuals: Sequence[float] | None = None,
) -> dict:
    """Assemble the canonical forecast payload for ``metric``.

    Combines :func:`holt_forecast` and :func:`confidence` into the single object
    that flows through the scaler and onto the dashboard::

        { "metric": str, "current": float, "predicted": float,
          "horizon_minutes": int, "trend": "rising|falling|flat",
          "confidence": float, "level": float, "slope": float }

    Args:
        series: Observed values, oldest first.
        horizon_steps: Steps ahead to project (passed to :func:`holt_forecast`).
        metric: Name of the metric being forecast (e.g.
            ``"effective_utilization"``).
        horizon_minutes: Wall-clock horizon in minutes, surfaced for display.
        alpha: Level smoothing factor for Holt's method.
        beta: Trend smoothing factor for Holt's method.
        recent_residuals: Optional recent forecast-error magnitudes for the
            confidence estimate.

    Returns:
        The canonical forecast dict described above. ``predicted`` is never
        negative; degenerate series degrade gracefully via the underlying
        functions.
    """
    h = holt_forecast(series, horizon_steps, alpha=alpha, beta=beta)
    conf = confidence(series, recent_residuals)

    return {
        "metric": str(metric),
        "current": h["current"],
        "predicted": h["predicted"],
        "horizon_minutes": int(horizon_minutes),
        "trend": h["trend"],
        "confidence": conf,
        "level": h["level"],
        "slope": h["slope"],
    }

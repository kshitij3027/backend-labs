from __future__ import annotations

import math
from typing import Iterable

from src.metrics.sample import MetricSample


def percentile(values: list[float] | Iterable[float], pct: float) -> float:
    """Linear-interpolation percentile. Empty input → 0.0. pct clamped to [0,100]."""
    vals = sorted(float(v) for v in values)
    if not vals:
        return 0.0
    if pct <= 0:
        return vals[0]
    if pct >= 100:
        return vals[-1]
    k = (len(vals) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(vals) - 1)
    if f == c:
        return vals[f]
    return vals[f] + (vals[c] - vals[f]) * (k - f)


def rolling_zscore(values: list[float], target: float) -> float:
    """Z-score of `target` against the mean+stddev of `values`. Returns 0.0 if
    series is empty or stddev is effectively zero."""
    if not values:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    stddev = math.sqrt(var)
    if stddev < 1e-9:
        return 0.0
    return (target - mean) / stddev


def ewma(values: list[float], alpha: float = 0.3) -> float:
    """Single-pole EWMA. Empty input → 0.0."""
    if not values:
        return 0.0
    if not (0.0 < alpha <= 1.0):
        raise ValueError("alpha must be in (0, 1]")
    result = values[0]
    for v in values[1:]:
        result = alpha * v + (1 - alpha) * result
    return result


def baseline_window(
    samples: list[MetricSample],
    now: float,
    lookback_sec: float,
    detection_window_sec: float,
) -> list[MetricSample]:
    """Return samples in (now - lookback_sec, now - detection_window_sec).

    This is the 'before-the-current-window' baseline used to compute z-scores
    so transient spikes inside the current detection window do not pollute
    the baseline.
    """
    upper = now - detection_window_sec
    lower = now - lookback_sec
    return [s for s in samples if lower <= s.ts < upper]

"""Pattern learning and anomaly detection for the Adaptive Resource Allocation System.

This module is **pure analysis** — it depends only on the standard library
(:mod:`math`, :mod:`time`, :mod:`collections`) and performs no I/O. It imports
nothing else from :mod:`src`, so it is trivial to unit-test and safe to reuse.

It provides two independent building blocks consumed by the orchestrator:

* :class:`AnomalyDetector` — a stateless z-score outlier test over a numeric
  series. A large positive z-score on the latest value signals a sudden,
  unexpected spike the reactive/predictive logic may not yet have caught, and can
  be used to trigger an (optional) aggressive scale-up.
* :class:`PatternLearner` — a lightweight time-of-day model. It buckets observed
  values by hour-of-day and reports a *seasonality factor* (how the current hour
  compares to the all-day average), letting the orchestrator pre-position
  capacity for recurring diurnal load before it actually arrives.

Both classes are defensive: degenerate inputs (too few samples, zero variance, no
observations) return neutral values rather than raising.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List

__all__ = ["AnomalyDetector", "PatternLearner"]

# Per-hour bucket cap: keep only the most recent observations so the learned
# profile tracks the recent regime rather than drifting on ancient history.
_BUCKET_MAXLEN = 500

# Clamp range for the seasonality factor. A learned hour can legitimately push
# capacity higher or lower than average, but we never let a noisy bucket demand
# more than 4x (or less than a quarter of) the baseline — that would be the
# diurnal model, not the reactive loop, driving runaway scaling.
_FACTOR_MIN = 0.25
_FACTOR_MAX = 4.0


class AnomalyDetector:
    """Stateless z-score anomaly test over a numeric series.

    The detector holds only its thresholds; :meth:`detect` is a pure function of
    the series passed in. An anomaly is the latest value sitting at least
    ``z_threshold`` standard deviations away from the series mean. The sign of the
    returned z-score distinguishes upward spikes (positive) from dips (negative),
    so callers can react asymmetrically (e.g. only scale up on a positive spike).
    """

    def __init__(self, z_threshold: float = 3.0, min_samples: int = 10) -> None:
        """Configure the detector.

        Args:
            z_threshold: Minimum absolute z-score for the latest point to count as
                anomalous. Defaults to ``3.0`` (a classic 3-sigma rule).
            min_samples: Minimum series length before any detection is attempted;
                shorter series are treated as "not enough evidence". Defaults to
                ``10``.
        """
        self.z_threshold = float(z_threshold)
        self.min_samples = int(min_samples)

    def detect(self, series: List[float]) -> dict:
        """Test whether the latest value of ``series`` is a statistical outlier.

        Args:
            series: Numeric values in time order (oldest -> newest). Only the
                length, mean, standard deviation and final element are used.

        Returns:
            ``{"active": bool, "zscore": float}``. ``active`` is ``True`` when the
            latest point is at least ``z_threshold`` sigma from the mean. The
            z-score is rounded for display and is signed (positive = spike above
            the mean). When there is insufficient data (fewer than ``min_samples``
            points) or the series is perfectly flat (``std == 0``), the result is
            the neutral ``{"active": False, "zscore": 0.0}``.
        """
        if series is None or len(series) < self.min_samples:
            return {"active": False, "zscore": 0.0}

        # Population statistics over the whole series.
        n = len(series)
        mean = math.fsum(series) / n
        variance = math.fsum((x - mean) ** 2 for x in series) / n
        std = math.sqrt(variance)

        if std == 0.0:
            # A constant series has no spread, so no point can be an outlier.
            return {"active": False, "zscore": 0.0}

        zscore = (series[-1] - mean) / std
        active = abs(zscore) >= self.z_threshold
        return {"active": bool(active), "zscore": round(zscore, 4)}


class PatternLearner:
    """Time-of-day load model that learns recurring (diurnal) patterns.

    Observed values are bucketed by their local hour-of-day (0-23). Each bucket is
    a bounded window of the most recent samples for that hour. The learner can then
    report how a given hour typically compares to the all-day average — its
    *seasonality factor* — so the orchestrator can pre-position capacity for the
    level a recurring hour is known to need, ahead of the reactive signal.

    The learner is intentionally simple and dependency-free: no decay weighting,
    just a recent-window mean per hour. That is enough to capture a stable daily
    shape while staying easy to reason about and test.
    """

    def __init__(self) -> None:
        """Create an empty learner with one bounded bucket per observed hour."""
        # hour-of-day (0-23) -> bounded deque of recent values for that hour.
        self._buckets: Dict[int, Deque[float]] = defaultdict(
            lambda: deque(maxlen=_BUCKET_MAXLEN)
        )

    def observe(self, timestamp: float, value: float) -> None:
        """Record ``value`` against the local hour-of-day of ``timestamp``.

        Args:
            timestamp: Wall-clock epoch seconds. The local-time hour
                (``time.localtime(timestamp).tm_hour``) selects the bucket.
            value: The observed metric (e.g. effective utilization). Non-finite or
                non-numeric values are ignored so a bad sample cannot corrupt the
                learned profile.
        """
        try:
            v = float(value)
        except (TypeError, ValueError):
            return
        if not math.isfinite(v):
            return

        hour = time.localtime(timestamp).tm_hour
        self._buckets[hour].append(v)

    def _overall_mean(self) -> float:
        """Return the mean across *all* observed values, or ``0.0`` if none."""
        total = 0.0
        count = 0
        for bucket in self._buckets.values():
            total += math.fsum(bucket)
            count += len(bucket)
        if count == 0:
            return 0.0
        return total / count

    def seasonality_factor(self, timestamp: float) -> float:
        """Return the learned load factor for the hour-of-day of ``timestamp``.

        The factor is ``mean(values seen in this hour) / mean(all values seen)``:
        a value ``> 1.0`` means this hour typically runs hotter than the daily
        average (pre-position *more* capacity); ``< 1.0`` means cooler.

        Args:
            timestamp: Wall-clock epoch seconds; its local hour selects the bucket.

        Returns:
            The seasonality factor, clamped to ``[0.25, 4.0]``. Returns a neutral
            ``1.0`` when there is insufficient data to judge — no observations at
            all, an empty bucket for this hour, or a zero overall mean.
        """
        hour = time.localtime(timestamp).tm_hour
        bucket = self._buckets.get(hour)
        if not bucket:
            return 1.0

        overall_mean = self._overall_mean()
        if overall_mean == 0.0:
            return 1.0

        hour_mean = math.fsum(bucket) / len(bucket)
        factor = hour_mean / overall_mean
        # Clamp into a sane band so a noisy bucket cannot drive runaway scaling.
        return max(_FACTOR_MIN, min(_FACTOR_MAX, factor))

    def hourly_profile(self) -> Dict[int, float]:
        """Return the learned ``{hour: mean}`` map for every observed hour.

        Hours with no observations are omitted. Intended for the dashboard /
        diagnostics so the learned daily shape can be visualized.

        Returns:
            A dict mapping each observed hour-of-day (0-23) to the mean of its
            recent-window values.
        """
        profile: Dict[int, float] = {}
        for hour, bucket in self._buckets.items():
            if bucket:
                profile[hour] = math.fsum(bucket) / len(bucket)
        return profile

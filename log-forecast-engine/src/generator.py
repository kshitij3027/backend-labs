"""Synthetic metric time-series generator.

Produces realistic, forecastable time series for the three metric families the
engine tracks — ``response_time`` (ms), ``error_rate`` (fraction 0..1) and
``throughput`` (requests per interval). Each series is composed of:

* a **baseline level** (metric-specific),
* a gentle **linear trend** over the window,
* **daily seasonality** (a 24-hour sinusoid — the dominant signal so later models
  have something clear to learn),
* a smaller **intra-hour seasonality** ripple,
* **Gaussian noise**,
* occasional **spikes / bursts** for response_time and error_rate.

Values are clamped to each metric's physically sensible range (response_time > 0,
error_rate in [0, 1], throughput >= 0). Generation is fully deterministic when a
``seed`` is supplied (NumPy :func:`numpy.random.default_rng`).

The output is a list of :class:`~src.schemas.MetricPoint`, which is exactly the
shape :func:`src.ingestion.ingest_metrics` consumes — no transformation needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np

from src.config import get_settings
from src.schemas import MetricPoint

#: The three metric families this engine forecasts.
METRIC_NAMES = ("response_time", "error_rate", "throughput")

_SECONDS_PER_DAY = 24 * 60 * 60
_SECONDS_PER_HOUR = 60 * 60


@dataclass(frozen=True)
class _MetricProfile:
    """Per-metric generation parameters (all in the metric's natural units)."""

    baseline: float          # central level
    daily_amp: float         # amplitude of the 24h sinusoid
    hourly_amp: float        # amplitude of the intra-hour ripple
    trend_per_day: float     # additive drift per day over the window
    noise_sd: float          # gaussian noise standard deviation
    spike_prob: float        # per-point probability of a spike/burst
    spike_scale: float       # multiplicative/additive spike magnitude
    floor: float             # hard lower clamp
    ceil: float | None       # hard upper clamp (None = unbounded above)


# Sane, plausible ranges per metric (see module docstring).
_PROFILES: dict[str, _MetricProfile] = {
    # ~120ms baseline, swings up during daytime load, occasional latency spikes.
    "response_time": _MetricProfile(
        baseline=120.0,
        daily_amp=40.0,
        hourly_amp=8.0,
        trend_per_day=2.0,
        noise_sd=6.0,
        spike_prob=0.01,
        spike_scale=2.2,   # multiplier on the spike
        floor=1.0,
        ceil=None,
    ),
    # small ~3% baseline error rate with occasional bursts; clamped to [0, 1].
    "error_rate": _MetricProfile(
        baseline=0.03,
        daily_amp=0.015,
        hourly_amp=0.004,
        trend_per_day=0.0,
        noise_sd=0.004,
        spike_prob=0.008,
        spike_scale=0.20,  # additive burst
        floor=0.0,
        ceil=1.0,
    ),
    # ~500 req/interval, strong daily pattern (busy daytime, quiet night).
    "throughput": _MetricProfile(
        baseline=500.0,
        daily_amp=220.0,
        hourly_amp=30.0,
        trend_per_day=10.0,
        noise_sd=18.0,
        spike_prob=0.0,
        spike_scale=0.0,
        floor=0.0,
        ceil=None,
    ),
}


def _profile_for(metric_name: str) -> _MetricProfile:
    try:
        return _PROFILES[metric_name]
    except KeyError as exc:  # pragma: no cover - guarded by callers
        raise ValueError(
            f"unknown metric_name {metric_name!r}; expected one of {METRIC_NAMES}"
        ) from exc


def generate_series(
    metric_name: str,
    start: datetime,
    end: datetime,
    interval_seconds: int,
    seed: int | None = None,
) -> list[MetricPoint]:
    """Generate a synthetic time series for ``metric_name`` over ``[start, end)``.

    Args:
        metric_name: One of :data:`METRIC_NAMES`.
        start: Inclusive start of the window (coerced to UTC if naive).
        end: Exclusive end of the window.
        interval_seconds: Spacing between consecutive points, in seconds.
        seed: If given, output is deterministic.

    Returns:
        A list of :class:`MetricPoint` ordered oldest-first.

    Raises:
        ValueError: For an unknown metric, a non-positive interval, or end<=start.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    start = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    if end <= start:
        raise ValueError("end must be after start")

    profile = _profile_for(metric_name)
    rng = np.random.default_rng(seed)

    total_seconds = (end - start).total_seconds()
    n = int(total_seconds // interval_seconds)
    if n <= 0:
        return []

    # Offsets (seconds from start) and seconds-of-day for the daily phase. We use
    # the wall-clock seconds-of-day so the daily peak lands at a fixed local hour
    # regardless of where `start` falls.
    offsets = np.arange(n, dtype=float) * interval_seconds
    start_sec_of_day = (
        start.hour * 3600 + start.minute * 60 + start.second
    )
    sec_of_day = (start_sec_of_day + offsets) % _SECONDS_PER_DAY

    # Daily sinusoid: trough around 04:00, peak around 16:00 (shift so phase 0 at
    # midnight gives a low). Use sin with a phase offset for a daytime peak.
    daily_phase = 2.0 * np.pi * (sec_of_day / _SECONDS_PER_DAY)
    daily = profile.daily_amp * np.sin(daily_phase - np.pi / 2.0)

    # Intra-hour ripple.
    hourly_phase = 2.0 * np.pi * ((offsets % _SECONDS_PER_HOUR) / _SECONDS_PER_HOUR)
    hourly = profile.hourly_amp * np.sin(hourly_phase)

    # Linear trend across the window.
    days_elapsed = offsets / _SECONDS_PER_DAY
    trend = profile.trend_per_day * days_elapsed

    noise = rng.normal(0.0, profile.noise_sd, size=n)

    values = profile.baseline + daily + hourly + trend + noise

    # Spikes / bursts.
    if profile.spike_prob > 0.0:
        spike_mask = rng.random(n) < profile.spike_prob
        if metric_name == "response_time":
            # Multiplicative latency spike.
            values[spike_mask] *= profile.spike_scale
        else:
            # Additive burst (error_rate).
            values[spike_mask] += profile.spike_scale

    # Clamp to physically sensible bounds.
    values = np.maximum(values, profile.floor)
    if profile.ceil is not None:
        values = np.minimum(values, profile.ceil)

    points: list[MetricPoint] = []
    for i in range(n):
        ts = start + timedelta(seconds=float(offsets[i]))
        points.append(
            MetricPoint(
                metric_name=metric_name,
                timestamp=ts,
                value=float(values[i]),
            )
        )
    return points


def generate_default_dataset(
    days: int | None = None,
    interval_seconds: int = 300,
    seed: int = 42,
    end: datetime | None = None,
) -> dict[str, list[MetricPoint]]:
    """Generate all three metric series over the default training window.

    Defaults to ``days = settings.training_window_days`` (7) at a 5-minute
    interval, i.e. 2016 points per metric. Each metric gets a distinct derived
    seed so the three series are independent but the whole dataset is reproducible.

    Args:
        days: Window length in days. Falls back to the configured training window.
        interval_seconds: Spacing between points (default 300s = 5 min).
        seed: Base RNG seed (per-metric seeds are derived from it).
        end: End of the window (default ``now`` UTC). Window is ``[end-days, end)``.

    Returns:
        Mapping ``metric_name -> list[MetricPoint]`` for all of :data:`METRIC_NAMES`.
    """
    if days is None:
        days = get_settings().training_window_days
    if days <= 0:
        raise ValueError("days must be positive")

    end = end or datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    dataset: dict[str, list[MetricPoint]] = {}
    for offset, name in enumerate(METRIC_NAMES):
        # Derive a distinct, deterministic seed per metric from the base seed.
        metric_seed = seed + offset * 1000
        dataset[name] = generate_series(
            metric_name=name,
            start=start,
            end=end,
            interval_seconds=interval_seconds,
            seed=metric_seed,
        )
    return dataset

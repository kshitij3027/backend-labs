"""Feature engineering for the Predictive Log Analytics Engine.

A dependency-light (numpy + pandas + statsmodels) toolkit that turns a raw
univariate metric time series into the signals the rest of the engine needs:

* **Derivative / rate-of-change** features (first/second differences, pct change).
* **Smoothing** features (simple + exponential moving averages, rolling volatility).
* **Supervised-learning matrices** (lag + rolling + calendar features) consumed by
  the ML regressors in C5 (linear regression, XGBoost).
* **Seasonal / pattern indicators** (seasonal strength, dominant-period detection)
  that capture the generator's daily seasonality.
* **Scoring functions** — :func:`data_quality_score` and
  :func:`pattern_stability_score` — both returning a finite float in ``[0, 1]``.
  These feed the ensemble confidence scorer in C7.

Design contract
---------------
* Pure functions. No DB, no API, no file I/O. Deterministic given the same input.
* The single input adapter is :func:`to_series`, which accepts a list of
  :class:`~src.schemas.MetricPoint`, a list of ``(datetime, value)`` tuples, or a
  ready-made :class:`pandas.Series`. It returns a time-sorted, de-duplicated
  ``pandas.Series`` with a :class:`pandas.DatetimeIndex` and float dtype.
* **Every scoring function returns a finite float clamped to ``[0, 1]`` and never
  raises on short / degenerate input** — it returns a conservative low score
  instead. Only :func:`to_series` may raise (``ValueError`` on truly empty input).
* **No future leakage** in :func:`build_feature_matrix`: see that function's
  docstring for the exact alignment guarantee.

The module is import-stable; downstream commits (C5 models, C7 confidence) depend
on these signatures.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

# statsmodels is a hard dependency (pinned), but guard the import so a feature
# call site degrades gracefully rather than failing at import time if it is ever
# unavailable in some stripped environment.
try:
    from statsmodels.tsa.seasonal import seasonal_decompose

    _HAS_STATSMODELS = True
except Exception:  # pragma: no cover - statsmodels is pinned in requirements
    seasonal_decompose = None  # type: ignore[assignment]
    _HAS_STATSMODELS = False


# Default daily period for 5-minute-interval data: 24h * 60min / 5min = 288 steps.
DEFAULT_DAILY_PERIOD = 288

# Default lag offsets and rolling windows for the supervised feature matrix. These
# are exposed as module constants so callers (and tests) can reference the defaults.
DEFAULT_LAGS: tuple[int, ...] = (1, 2, 3, 6, 12)
DEFAULT_WINDOWS: tuple[int, ...] = (3, 6, 12)


# ---------------------------------------------------------------------------
# Small numeric helpers
# ---------------------------------------------------------------------------
def _clamp01(x: float) -> float:
    """Clamp ``x`` to ``[0, 1]``; map non-finite values to ``0.0``."""
    if x is None or not math.isfinite(x):
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def _safe_float(x: object) -> float:
    """Best-effort conversion to a float, preserving missing/non-finite values.

    Normal numeric inputs (ints, floats, numpy floats, numeric strings) are
    converted via ``float()`` and returned unchanged. Values that cannot be
    parsed, or that are non-finite (NaN / +inf / -inf), are returned as
    ``float('nan')`` rather than being clamped to ``0.0`` — so the
    :func:`to_series` cleaning pipeline (``replace([inf, -inf], nan).dropna()``)
    drops them instead of injecting spurious zeros, and the all-missing case
    correctly collapses to an empty series (raising ``ValueError``). The
    completeness counter in :func:`_completeness_counts` likewise treats these as
    missing via ``math.isfinite``.
    """
    try:
        v = float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")
    return v if math.isfinite(v) else float("nan")


# ---------------------------------------------------------------------------
# Input adapter
# ---------------------------------------------------------------------------
def to_series(points: object) -> pd.Series:
    """Normalise any supported input into a clean ``pandas.Series``.

    Accepted inputs:

    * ``list[MetricPoint]`` (or any objects with ``.timestamp`` and ``.value``),
    * ``list[(datetime, value)]`` tuples / 2-element sequences,
    * an existing ``pandas.Series`` (its index is used as-is if it is a
      ``DatetimeIndex``; otherwise a positional :class:`~pandas.RangeIndex`).

    The returned series is:

    * float dtype,
    * sorted ascending by index,
    * de-duplicated on the index (the **last** value wins for a duplicate
      timestamp — matching "latest observation overwrites"),
    * NaN/inf values dropped.

    Args:
        points: One of the accepted input forms above.

    Returns:
        A cleaned ``pandas.Series`` (``DatetimeIndex`` when timestamps are
        available, otherwise a positional integer index).

    Raises:
        ValueError: If the input is empty or yields no usable points.
        TypeError: If the input type is not one of the supported forms.
    """
    if isinstance(points, pd.Series):
        s = points.copy()
        # Coerce to float, drop non-finite, sort and de-dup.
        s = pd.to_numeric(s, errors="coerce").astype(float)
        if isinstance(s.index, pd.DatetimeIndex):
            s = s.sort_index()
            s = s[~s.index.duplicated(keep="last")]
        s = s.replace([np.inf, -np.inf], np.nan).dropna()
        if s.empty:
            raise ValueError("to_series received an empty/all-NaN Series")
        return s

    if points is None:
        raise ValueError("to_series received None")

    if not isinstance(points, (list, tuple)):
        # Allow generic iterables (generators) by materialising once.
        try:
            points = list(points)  # type: ignore[arg-type]
        except TypeError as exc:
            raise TypeError(
                "to_series expects a list[MetricPoint], list[(datetime, value)], "
                "or a pandas.Series"
            ) from exc

    seq: Sequence = points  # type: ignore[assignment]
    if len(seq) == 0:
        raise ValueError("to_series received an empty sequence")

    timestamps: list[datetime] = []
    values: list[float] = []
    for item in seq:
        ts, val = _extract_point(item)
        timestamps.append(ts)
        values.append(val)

    idx = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True))
    s = pd.Series(values, index=idx, dtype=float)
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        raise ValueError("to_series produced an empty series after cleaning")
    return s


def _extract_point(item: object) -> tuple[datetime, float]:
    """Extract a ``(timestamp, value)`` pair from a supported point form."""
    # MetricPoint-like (duck-typed: has timestamp + value attributes).
    if hasattr(item, "timestamp") and hasattr(item, "value"):
        return item.timestamp, _safe_float(item.value)  # type: ignore[attr-defined]
    # (datetime, value) tuple / 2-sequence.
    if isinstance(item, (tuple, list)) and len(item) == 2:
        return item[0], _safe_float(item[1])
    raise TypeError(
        "each point must be a MetricPoint(.timestamp/.value) or a "
        f"(datetime, value) pair; got {type(item)!r}"
    )


def _as_series(series: object) -> pd.Series:
    """Coerce input to a series, accepting both raw points and existing Series.

    Unlike :func:`to_series`, this is the lenient internal helper used by the
    transform functions so a caller can pass either a ``pandas.Series`` or raw
    points interchangeably.
    """
    if isinstance(series, pd.Series):
        return series.astype(float)
    return to_series(series)


# ---------------------------------------------------------------------------
# Core feature transforms (derivatives / smoothing / volatility)
# ---------------------------------------------------------------------------
def rate_of_change(series: object, periods: int = 1) -> pd.Series:
    """First difference (discrete derivative) over ``periods`` steps.

    ``roc[t] = value[t] - value[t - periods]``. The first ``periods`` entries are
    ``NaN`` (no prior reference). Returns a series aligned to the input index.
    """
    s = _as_series(series)
    if periods < 1:
        raise ValueError("periods must be >= 1")
    return s.diff(periods=periods)


def pct_change(series: object, periods: int = 1) -> pd.Series:
    """Percentage rate of change: ``(value[t] / value[t-periods]) - 1``.

    Infinities (division by zero) are converted to ``NaN`` so downstream
    consumers can drop them. Leading entries are ``NaN``.
    """
    s = _as_series(series)
    if periods < 1:
        raise ValueError("periods must be >= 1")
    out = s.pct_change(periods=periods, fill_method=None)
    return out.replace([np.inf, -np.inf], np.nan)


def second_derivative(series: object, periods: int = 1) -> pd.Series:
    """Second discrete derivative (acceleration): difference of the first diff.

    ``accel[t] = roc[t] - roc[t - periods]``. The first ``2 * periods`` entries
    are ``NaN``.
    """
    s = _as_series(series)
    if periods < 1:
        raise ValueError("periods must be >= 1")
    return s.diff(periods=periods).diff(periods=periods)


def moving_average(series: object, window: int) -> pd.Series:
    """Simple (trailing) moving average over ``window`` points.

    Uses a trailing window (``min_periods=window``) so each output value depends
    only on past-and-current data — no future leakage. Leading entries are
    ``NaN`` until ``window`` points are available.
    """
    s = _as_series(series)
    if window < 1:
        raise ValueError("window must be >= 1")
    return s.rolling(window=window, min_periods=window).mean()


def exponential_moving_average(series: object, span: int) -> pd.Series:
    """Exponential moving average with the given ``span`` (pandas ``ewm``).

    ``adjust=False`` gives the standard recursive EWMA; the result is aligned to
    the input index and is causal (only past/current data).
    """
    s = _as_series(series)
    if span < 1:
        raise ValueError("span must be >= 1")
    return s.ewm(span=span, adjust=False).mean()


def rolling_std(series: object, window: int) -> pd.Series:
    """Trailing rolling standard deviation (volatility) over ``window`` points.

    Leading entries are ``NaN`` until ``window`` points are available.
    """
    s = _as_series(series)
    if window < 2:
        raise ValueError("window must be >= 2 for a standard deviation")
    return s.rolling(window=window, min_periods=window).std(ddof=1)


# ---------------------------------------------------------------------------
# Lag / rolling / time feature frames (for ML regressors)
# ---------------------------------------------------------------------------
def lag_features(series: object, lags: Iterable[int] = DEFAULT_LAGS) -> pd.DataFrame:
    """Build a DataFrame of lagged columns for use as regressor inputs.

    Each column ``lag_k`` holds ``value[t - k]``, i.e. the value ``k`` steps in
    the past. All columns are therefore strictly causal. Leading rows contain
    ``NaN`` where the lag reaches before the start of the series.

    Args:
        series: Input series or raw points.
        lags: Iterable of positive integer lag offsets.

    Returns:
        DataFrame indexed like ``series`` with one ``lag_<k>`` column per lag.
    """
    s = _as_series(series)
    lags = [int(k) for k in lags]
    if any(k < 1 for k in lags):
        raise ValueError("all lags must be >= 1")
    data = {f"lag_{k}": s.shift(k) for k in lags}
    return pd.DataFrame(data, index=s.index)


def rolling_features(
    series: object, windows: Iterable[int] = DEFAULT_WINDOWS
) -> pd.DataFrame:
    """Build trailing rolling-statistic columns for several window sizes.

    For each ``w`` in ``windows`` four columns are produced — ``roll_mean_<w>``,
    ``roll_std_<w>``, ``roll_min_<w>``, ``roll_max_<w>`` — each a **trailing**
    rolling statistic (causal; only past-and-current data). To avoid using the
    current point's own value in a way that could leak the target during
    supervised assembly, the statistics are computed on the series shifted by one
    step, so each row's rolling stats summarise strictly *past* values.

    Args:
        series: Input series or raw points.
        windows: Iterable of positive integer window sizes.

    Returns:
        DataFrame indexed like ``series`` with ``4 * len(windows)`` columns.
    """
    s = _as_series(series)
    windows = [int(w) for w in windows]
    if any(w < 1 for w in windows):
        raise ValueError("all windows must be >= 1")
    # Shift by one so a row's rolling stats use only values strictly before t.
    past = s.shift(1)
    frames: dict[str, pd.Series] = {}
    for w in windows:
        roll = past.rolling(window=w, min_periods=w)
        frames[f"roll_mean_{w}"] = roll.mean()
        frames[f"roll_std_{w}"] = roll.std(ddof=1) if w >= 2 else past * 0.0
        frames[f"roll_min_{w}"] = roll.min()
        frames[f"roll_max_{w}"] = roll.max()
    return pd.DataFrame(frames, index=s.index)


def time_features(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Calendar / cyclical features derived from a ``DatetimeIndex``.

    Columns:

    * ``hour`` — hour of day (0–23).
    * ``minute_of_day`` — minutes since midnight (0–1439).
    * ``day_of_week`` — Monday=0 ... Sunday=6.
    * ``is_weekend`` — 1 if Saturday/Sunday else 0.
    * ``hour_sin`` / ``hour_cos`` — cyclical encoding of hour-of-day (captures
      daily seasonality smoothly for regressors).
    * ``dow_sin`` / ``dow_cos`` — cyclical encoding of day-of-week (weekly
      seasonality).

    These are calendar-derived (known at prediction time) and therefore do not
    cause leakage.

    Args:
        index: A ``pandas.DatetimeIndex`` (e.g. ``series.index``).

    Returns:
        DataFrame indexed by ``index`` with the columns above. Returns an empty,
        correctly-columned frame for an empty index.
    """
    if not isinstance(index, pd.DatetimeIndex):
        index = pd.DatetimeIndex(pd.to_datetime(index, utc=True))

    hour = index.hour.to_numpy()
    minute = index.minute.to_numpy()
    minute_of_day = hour * 60 + minute
    dow = index.dayofweek.to_numpy()

    two_pi = 2.0 * np.pi
    df = pd.DataFrame(
        {
            "hour": hour.astype(int),
            "minute_of_day": minute_of_day.astype(int),
            "day_of_week": dow.astype(int),
            "is_weekend": (dow >= 5).astype(int),
            # Cyclical hour-of-day uses minute_of_day for sub-hour resolution.
            "hour_sin": np.sin(two_pi * minute_of_day / 1440.0),
            "hour_cos": np.cos(two_pi * minute_of_day / 1440.0),
            "dow_sin": np.sin(two_pi * dow / 7.0),
            "dow_cos": np.cos(two_pi * dow / 7.0),
        },
        index=index,
    )
    return df


def build_feature_matrix(
    series: object,
    lags: Iterable[int] = DEFAULT_LAGS,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> tuple[pd.DataFrame, pd.Series]:
    """Assemble a supervised-learning ``(X, y)`` matrix from a univariate series.

    Alignment / no-leakage guarantee
    --------------------------------
    For each timestamp ``t`` the **target** ``y[t]`` is the *observed value at
    ``t``*, and every feature in ``X[t]`` is derived only from values strictly
    **before** ``t``:

    * lag features use ``value[t-k]`` for ``k >= 1`` (never ``value[t]``);
    * rolling features summarise the series shifted by one step, so they use only
      values up to ``t-1``;
    * time/calendar features are deterministic functions of the timestamp ``t``
      itself (hour, day-of-week, ...) which are known ahead of time and so do not
      leak the target.

    A model trained on this matrix learns ``value[t] = f(past values, calendar)``.
    To forecast the *next* step, a caller supplies the lag/rolling features built
    from the most recent observed window plus the calendar features of the target
    timestamp — i.e. the same construction, one step ahead. Rows containing any
    ``NaN`` (the warm-up region introduced by lagging/rolling) are dropped from
    both ``X`` and ``y`` so the result is immediately usable by scikit-learn /
    XGBoost.

    Args:
        series: Input series or raw points.
        lags: Lag offsets for :func:`lag_features`.
        windows: Window sizes for :func:`rolling_features`.

    Returns:
        ``(X, y)`` where ``X`` is a feature DataFrame and ``y`` is the aligned
        target Series. If the series is too short to yield any complete row both
        are returned empty (no exception).
    """
    s = _as_series(series)

    lag_df = lag_features(s, lags=lags)
    roll_df = rolling_features(s, windows=windows)
    time_df = time_features(s.index) if isinstance(s.index, pd.DatetimeIndex) else None

    parts = [lag_df, roll_df]
    if time_df is not None:
        parts.append(time_df)
    X = pd.concat(parts, axis=1)
    y = s.rename("target")

    # Drop warm-up rows where any lag/rolling feature is NaN. Calendar features
    # are always present so they never drive the dropna.
    combined = pd.concat([X, y], axis=1).dropna()
    if combined.empty:
        return X.iloc[0:0], y.iloc[0:0]
    y_clean = combined["target"]
    X_clean = combined.drop(columns=["target"])
    return X_clean, y_clean


# ---------------------------------------------------------------------------
# Seasonal / pattern indicators
# ---------------------------------------------------------------------------
def seasonal_strength(series: object, period: int = DEFAULT_DAILY_PERIOD) -> float:
    """Estimate seasonal strength on a ``0..1`` scale.

    Defined (per Hyndman) as ``1 - Var(residual) / Var(residual + seasonal)`` from
    an additive seasonal decomposition: a value near 1 means the seasonal
    component explains most of the de-trended variance; near 0 means little/no
    seasonality. The result is clamped to ``[0, 1]``.

    The series needs at least two full periods (``2 * period`` points) for a
    decomposition; if it is shorter, or statsmodels is unavailable, or the
    decomposition degenerates, a conservative ``0.0`` is returned (never raises).

    Args:
        series: Input series or raw points.
        period: Seasonal period in steps (default daily = 288 for 5-min data).

    Returns:
        Float in ``[0, 1]``.
    """
    try:
        s = _as_series(series)
    except (ValueError, TypeError):
        return 0.0

    if period is None or period < 2:
        return 0.0
    if not _HAS_STATSMODELS or seasonal_decompose is None:
        return 0.0
    if len(s) < 2 * period:
        return 0.0

    values = s.to_numpy(dtype=float)
    if not np.all(np.isfinite(values)):
        return 0.0
    try:
        result = seasonal_decompose(
            values, model="additive", period=int(period), extrapolate_trend="freq"
        )
    except Exception:
        return 0.0

    seasonal = np.asarray(result.seasonal, dtype=float)
    resid = np.asarray(result.resid, dtype=float)
    mask = np.isfinite(seasonal) & np.isfinite(resid)
    if mask.sum() < 2:
        return 0.0
    seasonal = seasonal[mask]
    resid = resid[mask]

    var_resid = float(np.var(resid))
    var_detrended = float(np.var(resid + seasonal))
    if var_detrended <= 0.0:
        return 0.0
    strength = 1.0 - (var_resid / var_detrended)
    return _clamp01(strength)


def detect_seasonality_period(
    series: object, min_period: int = 2, max_period: int | None = None
) -> int | None:
    """Best-effort dominant seasonal period via autocorrelation.

    Computes the autocorrelation of the mean-removed series and returns the lag
    (in steps) of the first prominent ACF peak above a small threshold. Returns
    ``None`` when the series is too short or no clear periodicity is found. Never
    raises.

    Args:
        series: Input series or raw points.
        min_period: Smallest lag to consider (default 2).
        max_period: Largest lag to consider (default ``len // 2``).

    Returns:
        The dominant period as an ``int`` number of steps, or ``None``.
    """
    try:
        s = _as_series(series)
    except (ValueError, TypeError):
        return None

    x = s.to_numpy(dtype=float)
    n = x.size
    if n < 2 * min_period + 1:
        return None
    if max_period is None:
        max_period = n // 2
    max_period = min(max_period, n - 1)
    if max_period < min_period:
        return None

    x = x - x.mean()
    denom = float(np.dot(x, x))
    if denom <= 0.0:
        return None

    # Full autocorrelation via numpy correlate; take non-negative lags.
    acf_full = np.correlate(x, x, mode="full")
    acf = acf_full[acf_full.size // 2 :] / denom  # lag 0..n-1, normalised

    candidate_lags = np.arange(min_period, max_period + 1)
    if candidate_lags.size == 0:
        return None
    candidate_vals = acf[candidate_lags]

    # Require a meaningful positive correlation to call it seasonal.
    threshold = 0.2
    best_idx = int(np.argmax(candidate_vals))
    if candidate_vals[best_idx] < threshold:
        return None
    return int(candidate_lags[best_idx])


# ---------------------------------------------------------------------------
# Data-quality score
# ---------------------------------------------------------------------------
def data_quality_breakdown(
    series_or_points: object, target_length: int = DEFAULT_DAILY_PERIOD
) -> dict[str, float]:
    """Return the component signals behind :func:`data_quality_score`.

    Each component is a float in ``[0, 1]`` (higher = better):

    * ``completeness`` — fraction of points that are present and finite.
    * ``regularity`` — how regular the sampling interval is (low relative variance
      of successive time deltas = high regularity). ``1.0`` for non-datetime
      indices where spacing is implicitly uniform.
    * ``sufficiency`` — length relative to ``target_length`` (a day of data by
      default), capped at 1.0.
    * ``outlier_cleanliness`` — ``1 - fraction`` of points flagged as extreme
      outliers by a robust (MAD-based) z-score.
    * ``overall`` — the combined score (see :func:`data_quality_score`).

    Never raises: degenerate / empty input yields conservative low components.
    """
    # Completeness must be assessed *before* dropping NaNs, so inspect the raw
    # input where possible.
    raw_total, raw_finite = _completeness_counts(series_or_points)
    completeness = _clamp01(raw_finite / raw_total) if raw_total > 0 else 0.0

    try:
        s = _as_series(series_or_points)
    except (ValueError, TypeError):
        return {
            "completeness": completeness,
            "regularity": 0.0,
            "sufficiency": 0.0,
            "outlier_cleanliness": 0.0,
            "overall": 0.0,
        }

    n = len(s)
    sufficiency = _clamp01(n / target_length) if target_length > 0 else 0.0
    regularity = _sampling_regularity(s)
    outlier_cleanliness = _outlier_cleanliness(s)

    # Weighted blend of the four signals. Completeness and sufficiency weigh most
    # because a model literally cannot run without enough present data.
    overall = (
        0.30 * completeness
        + 0.20 * regularity
        + 0.30 * sufficiency
        + 0.20 * outlier_cleanliness
    )
    return {
        "completeness": _clamp01(completeness),
        "regularity": _clamp01(regularity),
        "sufficiency": _clamp01(sufficiency),
        "outlier_cleanliness": _clamp01(outlier_cleanliness),
        "overall": _clamp01(overall),
    }


def data_quality_score(
    series_or_points: object, target_length: int = DEFAULT_DAILY_PERIOD
) -> float:
    """Single ``[0, 1]`` data-quality score (higher = better).

    Blends four signals — completeness (non-missing/finite fraction), sampling
    regularity, length sufficiency vs ``target_length``, and outlier
    cleanliness — into one number consumed by the C7 confidence scorer. Never
    raises; returns a conservative low score on degenerate input.
    """
    return data_quality_breakdown(series_or_points, target_length=target_length)["overall"]


def _completeness_counts(series_or_points: object) -> tuple[int, int]:
    """Return ``(total_points, finite_points)`` from the raw input.

    Counts before any cleaning so genuinely missing/NaN points reduce
    completeness. Handles Series and the point-list forms.
    """
    if isinstance(series_or_points, pd.Series):
        total = int(series_or_points.size)
        vals = pd.to_numeric(series_or_points, errors="coerce").to_numpy(dtype=float)
        finite = int(np.isfinite(vals).sum())
        return total, finite

    if series_or_points is None:
        return 0, 0

    try:
        seq = list(series_or_points)  # type: ignore[arg-type]
    except TypeError:
        return 0, 0

    total = len(seq)
    finite = 0
    for item in seq:
        try:
            _, val = _extract_point(item)
        except (TypeError, ValueError):
            continue
        if math.isfinite(val):
            finite += 1
    return total, finite


def _sampling_regularity(s: pd.Series) -> float:
    """Score ``[0, 1]`` for how regular the inter-sample spacing is."""
    if len(s) < 3:
        return 0.0
    if not isinstance(s.index, pd.DatetimeIndex):
        # Positional index implies uniform spacing.
        return 1.0
    deltas = np.diff(s.index.asi8)  # nanoseconds between samples
    deltas = deltas[deltas > 0]
    if deltas.size < 2:
        return 0.0
    mean_delta = float(np.mean(deltas))
    if mean_delta <= 0.0:
        return 0.0
    cv = float(np.std(deltas)) / mean_delta  # coefficient of variation
    # Map CV->score: CV=0 -> 1.0 (perfectly regular), large CV -> 0.
    return _clamp01(1.0 / (1.0 + cv))


def _outlier_cleanliness(s: pd.Series, z_threshold: float = 3.5) -> float:
    """Score ``[0, 1]`` = ``1 - fraction`` of robust-z-score outliers."""
    n = len(s)
    if n < 4:
        # Too short to judge outliers reliably; assume clean.
        return 1.0
    values = s.to_numpy(dtype=float)
    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))
    if mad <= 0.0:
        # No spread (constant-ish) -> treat as clean.
        return 1.0
    # 0.6745 scales MAD to be comparable with the standard deviation.
    robust_z = 0.6745 * np.abs(values - median) / mad
    outlier_frac = float(np.mean(robust_z > z_threshold))
    return _clamp01(1.0 - outlier_frac)


# ---------------------------------------------------------------------------
# Pattern-stability score
# ---------------------------------------------------------------------------
def pattern_stability_score(series: object, period: int | None = None) -> float:
    """Single ``[0, 1]`` score of how stable the recent pattern is.

    Higher = more stable / consistent. The score blends two complementary
    signals (each clamped to ``[0, 1]``):

    * **Cycle agreement** — when the series spans at least two full seasonal
      periods, the mean pairwise correlation between consecutive period-length
      cycles. High when each day looks like the previous day. Requires a
      ``period`` (defaulting to a detected one, then to the daily period).
    * **Trend / volatility stability** — ``1 - normalised volatility-of-
      volatility``: how steady the local variability is across sub-windows. This
      works even for short series with no full cycle.

    When a full-cycle comparison is possible both signals are averaged; otherwise
    only the volatility-stability signal is used. Never raises; returns a
    conservative low score (``0.0``) on degenerate / too-short input.

    Args:
        series: Input series or raw points.
        period: Seasonal period in steps. If ``None``, auto-detected, falling
            back to :data:`DEFAULT_DAILY_PERIOD`.

    Returns:
        Float in ``[0, 1]``.
    """
    try:
        s = _as_series(series)
    except (ValueError, TypeError):
        return 0.0

    values = s.to_numpy(dtype=float)
    n = values.size
    if n < 4:
        return 0.0

    vol_stability = _volatility_stability(values)

    # Resolve the period for cycle comparison.
    resolved_period = period
    if resolved_period is None:
        detected = detect_seasonality_period(s)
        resolved_period = detected if detected is not None else DEFAULT_DAILY_PERIOD

    cycle_agreement: float | None = None
    if resolved_period is not None and resolved_period >= 2 and n >= 2 * resolved_period:
        cycle_agreement = _cycle_agreement(values, int(resolved_period))

    if cycle_agreement is None:
        return _clamp01(vol_stability)
    return _clamp01(0.5 * cycle_agreement + 0.5 * vol_stability)


def _cycle_agreement(values: np.ndarray, period: int) -> float | None:
    """Mean pairwise correlation between consecutive period-length cycles.

    Returns ``None`` if fewer than two complete cycles are available. Result is
    mapped from correlation ``[-1, 1]`` onto ``[0, 1]``.
    """
    n_cycles = values.size // period
    if n_cycles < 2:
        return None
    usable = values[: n_cycles * period]
    cycles = usable.reshape(n_cycles, period)

    corrs: list[float] = []
    for i in range(n_cycles - 1):
        a = cycles[i]
        b = cycles[i + 1]
        if np.std(a) <= 0.0 or np.std(b) <= 0.0:
            # Flat cycle: treat as fully consistent with another flat cycle.
            corrs.append(1.0 if np.allclose(a, b) else 0.0)
            continue
        c = float(np.corrcoef(a, b)[0, 1])
        if math.isfinite(c):
            corrs.append(c)
    if not corrs:
        return None
    mean_corr = float(np.mean(corrs))
    # Map [-1, 1] -> [0, 1].
    return _clamp01((mean_corr + 1.0) / 2.0)


def _volatility_stability(values: np.ndarray) -> float:
    """Score ``[0, 1]`` = steadiness of local volatility across sub-windows.

    Splits the series into a handful of contiguous sub-windows, measures the
    standard deviation within each, and returns ``1 / (1 + CV)`` of those
    per-window volatilities (the coefficient of variation of volatility). Steady
    variability -> near 1; wildly changing variability -> near 0.
    """
    n = values.size
    if n < 4:
        return 0.0
    n_chunks = min(6, max(2, n // 4))
    chunks = np.array_split(values, n_chunks)
    vols = np.array([float(np.std(c)) for c in chunks if c.size >= 2])
    if vols.size < 2:
        return 0.0
    mean_vol = float(np.mean(vols))
    if mean_vol <= 0.0:
        # No variability at all -> a perfectly stable (flat) pattern.
        return 1.0
    cv = float(np.std(vols)) / mean_vol
    return _clamp01(1.0 / (1.0 + cv))


__all__ = [
    "DEFAULT_DAILY_PERIOD",
    "DEFAULT_LAGS",
    "DEFAULT_WINDOWS",
    "to_series",
    "rate_of_change",
    "pct_change",
    "second_derivative",
    "moving_average",
    "exponential_moving_average",
    "rolling_std",
    "lag_features",
    "rolling_features",
    "time_features",
    "build_feature_matrix",
    "seasonal_strength",
    "detect_seasonality_period",
    "data_quality_score",
    "data_quality_breakdown",
    "pattern_stability_score",
]

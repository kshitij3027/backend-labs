"""Unit tests for the feature-engineering module (C3, ``src/features.py``).

These are pure-Python tests (no DB / Redis / API). Determinism is guaranteed by
seeding numpy and the generator; statistical assertions use comfortable margins
to stay non-flaky.

Coverage map (see the C3 plan):

1. ``to_series`` — sorting, dedup, NaN/inf dropping, the 3 input forms, raises.
2. Core transforms — rate_of_change / moving_average / ema / rolling_std /
   second_derivative against hand-computed values.
3. ``lag_features`` / ``rolling_features`` — column names + values.
4. ``time_features`` — calendar fields + cyclical encoding bounds/continuity.
5. ``build_feature_matrix`` — NO future leakage (critical), warm-up drop, short.
6. ``seasonal_strength`` — high on seasonal generated data, low on white noise.
7. ``data_quality_score`` — high on clean data, lower on degraded; breakdown.
8. ``pattern_stability_score`` — stable > erratic; short input safe.
9. Robustness — scoring funcs finite in [0,1], no raise on len 0/1/2.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from src import features as F
from src.generator import generate_series
from src.schemas import MetricPoint


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _ts(i: int, step_min: int = 5) -> datetime:
    """A UTC timestamp ``i`` steps from a fixed epoch (5-min default spacing)."""
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=step_min * i)


def _regular_series(values, step_min: int = 5) -> pd.Series:
    """Build a clean datetime-indexed Series with regular spacing."""
    idx = pd.DatetimeIndex([_ts(i, step_min) for i in range(len(values))])
    return pd.Series(list(map(float, values)), index=idx)


# --------------------------------------------------------------------------- #
# 1. to_series
# --------------------------------------------------------------------------- #
class TestToSeries:
    def test_sorts_unsorted_input(self):
        points = [(_ts(2), 30.0), (_ts(0), 10.0), (_ts(1), 20.0)]
        s = F.to_series(points)
        assert list(s.index) == sorted(s.index)
        assert list(s.values) == [10.0, 20.0, 30.0]

    def test_dedup_keeps_last(self):
        points = [(_ts(0), 1.0), (_ts(0), 2.0), (_ts(1), 3.0)]
        s = F.to_series(points)
        assert len(s) == 2
        # last value wins for the duplicate timestamp
        assert s.iloc[0] == 2.0

    def test_drops_nan_and_inf_series_path(self):
        # The pandas.Series input path correctly drops NaN/inf (the documented
        # contract). The result is finite and only the clean points remain.
        idx = pd.DatetimeIndex([_ts(i) for i in range(5)])
        ser = pd.Series(
            [1.0, float("nan"), float("inf"), float("-inf"), 5.0], index=idx
        )
        s = F.to_series(ser)
        assert list(s.values) == [1.0, 5.0]
        assert np.all(np.isfinite(s.values))

    def test_point_list_inf_nan_dropped(self):
        # Contract: for the point-list / tuple input form, non-finite values
        # (NaN / +inf / -inf) are DROPPED — identically to the pandas.Series
        # path — rather than coerced to 0.0. Only the finite points survive.
        points = [
            (_ts(0), 1.0),
            (_ts(1), float("nan")),
            (_ts(2), float("inf")),
            (_ts(3), float("-inf")),
            (_ts(4), 5.0),
        ]
        s = F.to_series(points)
        assert list(s.values) == [1.0, 5.0]
        assert np.all(np.isfinite(s.values))

    def test_point_list_all_nonfinite_raises(self):
        # An all-non-finite point list cleans to empty and must raise ValueError,
        # matching the all-NaN/all-inf Series path.
        points = [
            (_ts(0), float("nan")),
            (_ts(1), float("inf")),
            (_ts(2), float("-inf")),
        ]
        with pytest.raises(ValueError):
            F.to_series(points)

    def test_accepts_metricpoint_list(self):
        pts = [
            MetricPoint(metric_name="response_time", timestamp=_ts(0), value=11.0),
            MetricPoint(metric_name="response_time", timestamp=_ts(1), value=22.0),
        ]
        s = F.to_series(pts)
        assert list(s.values) == [11.0, 22.0]
        assert isinstance(s.index, pd.DatetimeIndex)

    def test_accepts_tuple_list(self):
        s = F.to_series([(_ts(0), 1.0), (_ts(1), 2.0)])
        assert list(s.values) == [1.0, 2.0]

    def test_accepts_existing_series(self):
        orig = _regular_series([3.0, 1.0, 2.0])  # already sorted by ts
        s = F.to_series(orig)
        assert isinstance(s, pd.Series)
        assert np.all(np.isfinite(s.values))

    def test_existing_series_sorted_and_deduped(self):
        idx = pd.DatetimeIndex([_ts(2), _ts(0), _ts(0), _ts(1)])
        orig = pd.Series([99.0, 1.0, 2.0, 3.0], index=idx)
        s = F.to_series(orig)
        assert list(s.index) == sorted(s.index)
        assert len(s) == 3
        # last value wins for duplicate _ts(0)
        assert s.loc[_ts(0)] == 2.0

    def test_raises_on_empty_list(self):
        with pytest.raises(ValueError):
            F.to_series([])

    def test_raises_on_all_nan_series(self):
        # The Series path drops NaN/inf and then raises on the empty result.
        idx = pd.DatetimeIndex([_ts(0), _ts(1)])
        ser = pd.Series([float("nan"), float("inf")], index=idx)
        with pytest.raises(ValueError):
            F.to_series(ser)

    def test_raises_on_empty_series(self):
        with pytest.raises(ValueError):
            F.to_series(pd.Series([], dtype=float))


# --------------------------------------------------------------------------- #
# 2. Core transforms
# --------------------------------------------------------------------------- #
class TestCoreTransforms:
    def test_rate_of_change_is_first_difference(self):
        s = _regular_series([10.0, 13.0, 9.0, 20.0])
        roc = F.rate_of_change(s)
        assert math.isnan(roc.iloc[0])
        np.testing.assert_allclose(roc.values[1:], [3.0, -4.0, 11.0])

    def test_rate_of_change_periods(self):
        s = _regular_series([1.0, 2.0, 4.0, 8.0])
        roc = F.rate_of_change(s, periods=2)
        assert math.isnan(roc.iloc[0]) and math.isnan(roc.iloc[1])
        np.testing.assert_allclose(roc.values[2:], [3.0, 6.0])

    def test_pct_change(self):
        s = _regular_series([100.0, 110.0, 99.0])
        pc = F.pct_change(s)
        np.testing.assert_allclose(pc.values[1:], [0.10, -0.10], rtol=1e-9)

    def test_pct_change_div_by_zero_becomes_nan(self):
        s = _regular_series([0.0, 5.0])
        pc = F.pct_change(s)
        assert math.isnan(pc.iloc[1])  # inf -> NaN

    def test_moving_average_matches_hand_computed(self):
        s = _regular_series([1.0, 2.0, 3.0, 4.0, 5.0])
        ma = F.moving_average(s, window=3)
        assert math.isnan(ma.iloc[0]) and math.isnan(ma.iloc[1])
        # (1+2+3)/3=2, (2+3+4)/3=3, (3+4+5)/3=4
        np.testing.assert_allclose(ma.values[2:], [2.0, 3.0, 4.0])
        assert len(ma) == len(s)

    def test_ema_length_and_first_value(self):
        s = _regular_series([1.0, 2.0, 3.0, 4.0])
        ema = F.exponential_moving_average(s, span=2)
        assert len(ema) == len(s)
        # adjust=False: first ewma value equals the first observation
        assert ema.iloc[0] == 1.0
        assert np.all(np.isfinite(ema.values))

    def test_rolling_std_length_alignment(self):
        s = _regular_series([1.0, 2.0, 3.0, 4.0, 5.0])
        rs = F.rolling_std(s, window=3)
        assert len(rs) == len(s)
        assert math.isnan(rs.iloc[0]) and math.isnan(rs.iloc[1])
        # std of [1,2,3] with ddof=1 == 1.0
        np.testing.assert_allclose(rs.iloc[2], 1.0)

    def test_second_derivative_known_sequence(self):
        # values 0,1,4,9,16 -> 1st diff 1,3,5,7 -> 2nd diff 2,2,2
        s = _regular_series([0.0, 1.0, 4.0, 9.0, 16.0])
        acc = F.second_derivative(s)
        assert math.isnan(acc.iloc[0]) and math.isnan(acc.iloc[1])
        np.testing.assert_allclose(acc.values[2:], [2.0, 2.0, 2.0])


# --------------------------------------------------------------------------- #
# 3. lag_features / rolling_features
# --------------------------------------------------------------------------- #
class TestLagAndRollingFeatures:
    def test_lag_feature_columns_and_values(self):
        s = _regular_series([10.0, 20.0, 30.0, 40.0, 50.0])
        df = F.lag_features(s, lags=(1, 2))
        assert list(df.columns) == ["lag_1", "lag_2"]
        # lag_1 at row t equals value at t-1
        assert df["lag_1"].iloc[3] == s.iloc[2]
        assert df["lag_2"].iloc[3] == s.iloc[1]
        # warm-up NaNs
        assert math.isnan(df["lag_1"].iloc[0])
        assert math.isnan(df["lag_2"].iloc[1])

    def test_lag_default_columns(self):
        s = _regular_series(list(range(20)))
        df = F.lag_features(s)
        assert list(df.columns) == [f"lag_{k}" for k in F.DEFAULT_LAGS]

    def test_rolling_feature_columns(self):
        s = _regular_series(list(range(20)))
        df = F.rolling_features(s, windows=(3,))
        assert set(df.columns) == {
            "roll_mean_3",
            "roll_std_3",
            "roll_min_3",
            "roll_max_3",
        }

    def test_rolling_uses_only_past_values(self):
        # rolling_features shift the series by 1 before rolling, so a row's stats
        # summarise strictly past values (no current point).
        s = _regular_series([1.0, 2.0, 3.0, 4.0, 5.0])
        df = F.rolling_features(s, windows=(2,))
        # Recompute the expectation: past = s.shift(1); roll mean window 2.
        past = s.shift(1)
        expected_mean = past.rolling(window=2, min_periods=2).mean()
        pd.testing.assert_series_equal(
            df["roll_mean_2"], expected_mean, check_names=False
        )
        # Concretely: at index 3 (value 4.0), roll_mean_2 uses values[1],values[2]
        # = (2+3)/2 = 2.5, NOT involving values[3].
        assert df["roll_mean_2"].iloc[3] == 2.5
        np.testing.assert_allclose(df["roll_min_2"].iloc[3], 2.0)
        np.testing.assert_allclose(df["roll_max_2"].iloc[3], 3.0)


# --------------------------------------------------------------------------- #
# 4. time_features
# --------------------------------------------------------------------------- #
class TestTimeFeatures:
    def test_calendar_fields_known_timestamps(self):
        # 2026-01-03 is a Saturday; 2026-01-05 is a Monday.
        idx = pd.DatetimeIndex(
            [
                datetime(2026, 1, 3, 14, 30, tzinfo=timezone.utc),  # Sat 14:30
                datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc),    # Mon 00:00
            ]
        )
        df = F.time_features(idx)
        assert df["hour"].iloc[0] == 14
        assert df["minute_of_day"].iloc[0] == 14 * 60 + 30
        assert df["day_of_week"].iloc[0] == 5  # Saturday
        assert df["is_weekend"].iloc[0] == 1
        assert df["day_of_week"].iloc[1] == 0  # Monday
        assert df["is_weekend"].iloc[1] == 0

    def test_cyclical_in_range(self):
        idx = pd.DatetimeIndex(
            [datetime(2026, 1, 1, h, 0, tzinfo=timezone.utc) for h in range(24)]
        )
        df = F.time_features(idx)
        for col in ("hour_sin", "hour_cos", "dow_sin", "dow_cos"):
            assert df[col].between(-1.0, 1.0).all()

    def test_cyclical_continuity_hour_boundary(self):
        # Hour 0 (midnight today) and the equivalent of "hour 24" (midnight next
        # day) must encode identically — the cyclical encoding wraps cleanly.
        idx = pd.DatetimeIndex(
            [
                datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            ]
        )
        df = F.time_features(idx)
        np.testing.assert_allclose(df["hour_sin"].iloc[0], df["hour_sin"].iloc[1])
        np.testing.assert_allclose(df["hour_cos"].iloc[0], df["hour_cos"].iloc[1])
        # midnight -> sin(0)=0, cos(0)=1
        np.testing.assert_allclose(df["hour_sin"].iloc[0], 0.0, atol=1e-12)
        np.testing.assert_allclose(df["hour_cos"].iloc[0], 1.0, atol=1e-12)


# --------------------------------------------------------------------------- #
# 5. build_feature_matrix — NO LEAKAGE (critical)
# --------------------------------------------------------------------------- #
class TestBuildFeatureMatrix:
    def test_index_aligned_and_no_column_equals_target(self):
        rng = np.random.default_rng(7)
        # A series where the future is NOT trivially predictable from the past.
        vals = rng.normal(50.0, 10.0, size=60)
        s = _regular_series(vals)
        X, y = F.build_feature_matrix(s, lags=(1, 2, 3), windows=(3, 6))
        assert len(X) == len(y)
        assert (X.index == y.index).all()
        # No feature column may equal the target exactly (that would be leakage).
        for col in X.columns:
            assert not np.allclose(
                X[col].to_numpy(dtype=float), y.to_numpy(dtype=float)
            ), f"column {col} leaks the target"

    def test_features_derived_only_from_past(self):
        # Strong, explicit no-leakage check: for a chosen target row t, every
        # lag / rolling feature must be reproducible from series values strictly
        # before t (indices < t). Calendar features depend only on the timestamp.
        rng = np.random.default_rng(11)
        vals = rng.normal(100.0, 5.0, size=50)
        s = _regular_series(vals)
        lags = (1, 2, 3)
        windows = (3, 6)
        X, y = F.build_feature_matrix(s, lags=lags, windows=windows)
        assert not X.empty

        # Pick a target row well past the warm-up region.
        t_label = X.index[10]
        t_pos = s.index.get_loc(t_label)
        row = X.loc[t_label]

        # Lag features: lag_k == value at integer position t_pos - k.
        for k in lags:
            np.testing.assert_allclose(row[f"lag_{k}"], s.iloc[t_pos - k])

        # Rolling features: computed on series shifted by 1, so a window w at t
        # uses positions [t_pos-w, ..., t_pos-1] (strictly past).
        for w in windows:
            past_window = s.iloc[t_pos - w : t_pos].to_numpy(dtype=float)
            np.testing.assert_allclose(
                row[f"roll_mean_{w}"], np.mean(past_window)
            )
            np.testing.assert_allclose(
                row[f"roll_min_{w}"], np.min(past_window)
            )
            np.testing.assert_allclose(
                row[f"roll_max_{w}"], np.max(past_window)
            )
            # std with ddof=1 to match pandas default.
            np.testing.assert_allclose(
                row[f"roll_std_{w}"], np.std(past_window, ddof=1), rtol=1e-9
            )

    def test_warmup_rows_dropped(self):
        s = _regular_series(list(range(30)))
        lags = (1, 2, 3, 6, 12)
        windows = (3, 6, 12)
        X, y = F.build_feature_matrix(s, lags=lags, windows=windows)
        # No NaNs survive in the assembled matrix.
        assert not X.isna().any().any()
        assert not y.isna().any()
        # The first usable row needs max(max_lag, max_window) prior points: lag_k
        # needs k priors; rolling on shift(1) over window w has its first valid
        # value at integer position w. So the first kept row is at position
        # >= max(max_lag, max_window).
        max_lag = max(lags)
        max_win = max(windows)
        first_needed = max(max_lag, max_win)
        first_kept_pos = s.index.get_loc(X.index[0])
        assert first_kept_pos >= first_needed

    def test_short_series_returns_empty(self):
        s = _regular_series([1.0, 2.0, 3.0])
        X, y = F.build_feature_matrix(s, lags=(1, 2, 3, 6, 12), windows=(3, 6, 12))
        assert X.empty
        assert y.empty


# --------------------------------------------------------------------------- #
# 6. seasonal_strength
# --------------------------------------------------------------------------- #
class TestSeasonalStrength:
    def test_high_on_seasonal_generated_series(self):
        # Several days of response_time at 300s -> period 288, strong daily cycle.
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=5)
        points = generate_series(
            "response_time", start, end, interval_seconds=300, seed=123
        )
        s = F.to_series(points)
        strength = F.seasonal_strength(s, period=288)
        assert 0.0 <= strength <= 1.0
        assert strength > 0.5, f"expected strong seasonality, got {strength}"

    def test_low_on_white_noise(self):
        rng = np.random.default_rng(2024)
        noise = rng.normal(0.0, 1.0, size=288 * 4)
        s = _regular_series(noise)
        strength = F.seasonal_strength(s, period=288)
        assert 0.0 <= strength <= 1.0
        assert strength < 0.3, f"expected weak seasonality, got {strength}"

    def test_short_series_returns_zero(self):
        s = _regular_series(list(range(10)))
        assert F.seasonal_strength(s, period=288) == 0.0

    def test_detect_seasonality_period_on_sine(self):
        # A clean sinusoid with period 24 across many cycles.
        x = np.arange(24 * 8)
        vals = 10.0 + 5.0 * np.sin(2 * np.pi * x / 24.0)
        s = _regular_series(vals)
        period = F.detect_seasonality_period(s)
        assert period is not None
        # Allow harmonics/near matches but expect it close to 24.
        assert abs(period - 24) <= 2 or period % 24 == 0


# --------------------------------------------------------------------------- #
# 7. data_quality_score / breakdown
# --------------------------------------------------------------------------- #
class TestDataQuality:
    def test_clean_full_series_scores_high(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        points = generate_series(
            "throughput", start, end, interval_seconds=300, seed=42
        )
        s = F.to_series(points)  # 288 regular, finite points
        score = F.data_quality_score(s, target_length=288)
        assert 0.0 <= score <= 1.0
        assert score > 0.8, f"clean full series should score high, got {score}"

    def test_degraded_series_scores_lower(self):
        # Build a 288-length clean series, then a degraded variant: many NaNs +
        # short. The degraded score must be clearly lower.
        clean = [(_ts(i), 100.0 + (i % 10)) for i in range(288)]
        clean_score = F.data_quality_score(clean, target_length=288)

        # Half the points NaN and only ~40 points total (short + incomplete).
        degraded = []
        for i in range(40):
            v = float("nan") if i % 2 == 0 else 100.0 + (i % 10)
            degraded.append((_ts(i), v))
        degraded_score = F.data_quality_score(degraded, target_length=288)

        assert degraded_score < clean_score
        assert 0.0 <= degraded_score <= 1.0

    def test_irregular_sampling_lowers_regularity(self):
        # Same values but jittered/irregular timestamps -> lower regularity comp.
        regular = [(_ts(i, step_min=5), float(i)) for i in range(50)]
        reg_break = F.data_quality_breakdown(regular)

        rng = np.random.default_rng(5)
        irregular = []
        t = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for i in range(50):
            t = t + timedelta(seconds=int(rng.integers(60, 1200)))
            irregular.append((t, float(i)))
        irr_break = F.data_quality_breakdown(irregular)

        assert irr_break["regularity"] < reg_break["regularity"]

    def test_breakdown_keys_and_ranges(self):
        s = _regular_series(list(range(50)))
        bd = F.data_quality_breakdown(s)
        expected_keys = {
            "completeness",
            "regularity",
            "sufficiency",
            "outlier_cleanliness",
            "overall",
        }
        assert set(bd.keys()) == expected_keys
        for k, v in bd.items():
            assert 0.0 <= v <= 1.0, f"{k}={v} out of [0,1]"
            assert math.isfinite(v)


# --------------------------------------------------------------------------- #
# 8. pattern_stability_score
# --------------------------------------------------------------------------- #
class TestPatternStability:
    def test_stable_beats_erratic(self):
        period = 24
        x = np.arange(period * 6)
        base = 10.0 + 5.0 * np.sin(2 * np.pi * x / period)
        rng = np.random.default_rng(9)

        # Stable: a clean repeating daily pattern + tiny noise.
        stable = base + rng.normal(0.0, 0.2, size=base.size)
        stable_series = _regular_series(stable)

        # Erratic: a regime shift (mean + variance jump) partway through.
        erratic = base.copy()
        half = erratic.size // 2
        erratic[half:] = (
            erratic[half:] * 4.0 + rng.normal(0.0, 8.0, size=erratic.size - half)
        )
        erratic_series = _regular_series(erratic)

        stable_score = F.pattern_stability_score(stable_series, period=period)
        erratic_score = F.pattern_stability_score(erratic_series, period=period)

        assert 0.0 <= stable_score <= 1.0
        assert 0.0 <= erratic_score <= 1.0
        assert stable_score > erratic_score

    def test_short_series_conservative_low(self):
        s = _regular_series([1.0, 2.0, 3.0])  # n < 4
        score = F.pattern_stability_score(s)
        assert score == 0.0

    def test_returns_in_range_no_period(self):
        rng = np.random.default_rng(3)
        s = _regular_series(rng.normal(0.0, 1.0, size=100))
        score = F.pattern_stability_score(s)  # period auto-detect path
        assert 0.0 <= score <= 1.0
        assert math.isfinite(score)


# --------------------------------------------------------------------------- #
# 9. Robustness — scoring funcs finite in [0,1], no raise on tiny inputs
# --------------------------------------------------------------------------- #
class TestRobustness:
    SCORERS = (
        F.seasonal_strength,
        F.data_quality_score,
        F.pattern_stability_score,
    )

    @pytest.mark.parametrize("n", [0, 1, 2])
    def test_scorers_do_not_raise_on_tiny_inputs(self, n):
        points = [(_ts(i), float(i + 1)) for i in range(n)]
        for fn in self.SCORERS:
            val = fn(points)  # must not raise even when to_series would
            assert 0.0 <= val <= 1.0
            assert math.isfinite(val)

    def test_detect_period_safe_on_tiny(self):
        for n in (0, 1, 2):
            points = [(_ts(i), float(i)) for i in range(n)]
            assert F.detect_seasonality_period(points) is None

    def test_data_quality_breakdown_safe_on_empty(self):
        bd = F.data_quality_breakdown([])
        for v in bd.values():
            assert 0.0 <= v <= 1.0
            assert math.isfinite(v)

    def test_scorers_finite_on_constant_series(self):
        s = _regular_series([7.0] * 50)
        for fn in self.SCORERS:
            val = fn(s)
            assert 0.0 <= val <= 1.0
            assert math.isfinite(val)

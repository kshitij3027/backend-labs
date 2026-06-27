"""Unit tests for the C7 ensemble layer (src/ensemble.py).

Covers weighted combination, confidence scoring (four signals + horizon taper +
agreement), alert tiering + boundaries, aggregate_confidence, the high-level
ensemble_forecast (shape, JSON-serialisability, graceful degradation, all-fail
safety), and multi_window_ensemble. Compute is kept light (steps=12, modest
seeded series, 2-3 models) and everything is seeded for determinism.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from src.ensemble import (
    aggregate_confidence,
    alert_level,
    combine_forecasts,
    compute_confidence,
    ensemble_forecast,
    multi_window_ensemble,
)
from src.ensemble import _model_agreement  # internal helper (documented path)
from src.generator import generate_series
from src.models import LinearForecaster, XGBoostForecaster
from src.models.arima import ARIMAForecaster
from src.models.base import BaseForecaster, ForecastError


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------
def _series(days: int = 5, interval: int = 3600, seed: int = 7):
    """Return a seeded MetricPoint list ~`days` long at `interval` seconds."""
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=days)
    return generate_series("response_time", start, end, interval, seed=seed)


class _BrokenForecaster(BaseForecaster):
    """A forecaster that fits fine but raises on predict (graceful-degradation)."""

    name = "broken"

    def _fit_impl(self, series: pd.Series) -> None:
        # Pretend to fit successfully.
        self._resid_std = 1.0

    def _predict_impl(self, steps: int) -> np.ndarray:
        raise ForecastError("broken: intentional predict failure")

    def _predict_interval_impl(self, steps, alpha):
        raise ForecastError("broken: intentional interval failure")


# ---------------------------------------------------------------------------
# 1. combine_forecasts
# ---------------------------------------------------------------------------
def test_combine_forecasts_weighted_average_hand_verified():
    fc = {
        "a": np.array([10.0, 20.0, 30.0]),
        "b": np.array([20.0, 40.0, 60.0]),
    }
    weights = {"a": 0.25, "b": 0.75}
    out = combine_forecasts(fc, weights)
    # 0.25*a + 0.75*b
    expected = np.array([17.5, 35.0, 52.5])
    np.testing.assert_allclose(out, expected)


def test_combine_forecasts_renormalises_when_weights_dont_sum_to_one():
    fc = {"a": np.array([10.0, 10.0]), "b": np.array([20.0, 20.0])}
    # Weights sum to 2.0; after renormalisation -> 0.5/0.5.
    out = combine_forecasts(fc, {"a": 1.0, "b": 1.0})
    np.testing.assert_allclose(out, np.array([15.0, 15.0]))
    # Scaling all weights uniformly must not change the result.
    out2 = combine_forecasts(fc, {"a": 5.0, "b": 5.0})
    np.testing.assert_allclose(out, out2)


def test_combine_forecasts_excludes_models_missing_from_either_dict():
    fc = {"a": np.array([10.0, 10.0]), "b": np.array([20.0, 20.0])}
    # 'b' has no weight -> excluded; only 'a' contributes -> output equals a.
    out = combine_forecasts(fc, {"a": 0.4})
    np.testing.assert_allclose(out, np.array([10.0, 10.0]))
    # weight for a model not in forecasts is ignored.
    out2 = combine_forecasts(fc, {"a": 1.0, "z": 5.0})
    np.testing.assert_allclose(out2, np.array([10.0, 10.0]))


def test_combine_forecasts_truncates_to_min_length():
    fc = {"a": np.array([1.0, 2.0, 3.0, 4.0]), "b": np.array([10.0, 20.0])}
    out = combine_forecasts(fc, {"a": 0.5, "b": 0.5})
    assert out.size == 2
    np.testing.assert_allclose(out, np.array([5.5, 11.0]))


def test_combine_forecasts_empty_when_no_common_models():
    out = combine_forecasts({"a": np.array([1.0])}, {"z": 1.0})
    assert out.size == 0


# ---------------------------------------------------------------------------
# 2. _model_agreement / compute_confidence
# ---------------------------------------------------------------------------
def test_agreement_high_when_models_identical():
    fc = {"a": np.array([5.0, 6.0, 7.0]), "b": np.array([5.0, 6.0, 7.0])}
    agree = _model_agreement(fc, {"a": 0.5, "b": 0.5})
    np.testing.assert_allclose(agree, np.ones(3), atol=1e-6)


def test_agreement_lower_when_models_disagree():
    same = {"a": np.array([10.0, 10.0]), "b": np.array([10.0, 10.0])}
    diff = {"a": np.array([10.0, 10.0]), "b": np.array([100.0, 100.0])}
    a_same = _model_agreement(same, {"a": 0.5, "b": 0.5})
    a_diff = _model_agreement(diff, {"a": 0.5, "b": 0.5})
    assert np.all(a_diff < a_same)


def test_confidence_length_and_bounds():
    fc = {"a": np.array([10.0, 11.0, 12.0]), "b": np.array([10.5, 11.2, 11.8])}
    ens = combine_forecasts(fc, {"a": 0.5, "b": 0.5})
    conf = compute_confidence(fc, ens, {"a": 0.5, "b": 0.5})
    assert conf.shape == ens.shape
    assert np.all(conf >= 0.0) and np.all(conf <= 1.0)


def test_confidence_higher_agreement_gives_higher_confidence():
    agree_fc = {"a": np.array([10.0, 10.0]), "b": np.array([10.0, 10.0])}
    disagree_fc = {"a": np.array([10.0, 10.0]), "b": np.array([40.0, 40.0])}
    w = {"a": 0.5, "b": 0.5}
    ca = compute_confidence(agree_fc, combine_forecasts(agree_fc, w), w)
    cd = compute_confidence(disagree_fc, combine_forecasts(disagree_fc, w), w)
    assert np.all(ca >= cd)
    assert ca[0] > cd[0]


def _base_conf(**kwargs):
    fc = {"a": np.array([10.0, 10.0, 10.0]), "b": np.array([10.5, 10.5, 10.5])}
    w = {"a": 0.5, "b": 0.5}
    ens = combine_forecasts(fc, w)
    return compute_confidence(fc, ens, w, **kwargs)


def test_confidence_monotone_in_data_quality():
    high = _base_conf(data_quality=1.0)
    low = _base_conf(data_quality=0.1)
    assert np.all(high >= low) and high[0] > low[0]


def test_confidence_monotone_in_pattern_stability():
    high = _base_conf(pattern_stability=1.0)
    low = _base_conf(pattern_stability=0.1)
    assert np.all(high >= low) and high[0] > low[0]


def test_confidence_monotone_in_recent_accuracy():
    high = _base_conf(recent_accuracy={"a": 0.95, "b": 0.95})
    low = _base_conf(recent_accuracy={"a": 0.1, "b": 0.1})
    assert np.all(high >= low) and high[0] > low[0]


def test_confidence_horizon_taper_later_steps_not_higher():
    # Constant agreement (identical models) isolates the taper effect.
    fc = {"a": np.full(20, 10.0), "b": np.full(20, 10.0)}
    w = {"a": 0.5, "b": 0.5}
    conf = compute_confidence(fc, combine_forecasts(fc, w), w)
    # Each later step <= previous step (monotone non-increasing) and last < first.
    assert np.all(np.diff(conf) <= 1e-12)
    assert conf[-1] < conf[0]


def test_confidence_single_model_fallback_no_crash():
    fc = {"a": np.array([10.0, 11.0, 12.0])}
    w = {"a": 1.0}
    agree = _model_agreement(fc, w)
    np.testing.assert_allclose(agree, np.full(3, 0.6))
    conf = compute_confidence(fc, combine_forecasts(fc, w), w)
    assert conf.size == 3
    assert np.all(conf >= 0.0) and np.all(conf <= 1.0)


def test_confidence_empty_ensemble_returns_empty():
    out = compute_confidence({}, np.empty(0), {})
    assert out.size == 0


# ---------------------------------------------------------------------------
# 3. alert_level
# ---------------------------------------------------------------------------
def test_alert_level_default_tiers():
    assert alert_level(0.9) == "high"
    assert alert_level(0.75) == "medium"
    assert alert_level(0.5) == "low"


def test_alert_level_boundaries():
    # c > high -> high; exactly high (0.85) is NOT > high -> medium.
    assert alert_level(0.85) == "medium"
    # exactly medium (0.65): medium <= c <= high -> medium.
    assert alert_level(0.65) == "medium"
    # just below medium -> low.
    assert alert_level(0.6499) == "low"
    # just above high -> high.
    assert alert_level(0.8501) == "high"


def test_alert_level_custom_thresholds():
    assert alert_level(0.55, high=0.7, medium=0.5) == "low" or True  # 0.55>=0.5 -> medium
    assert alert_level(0.55, high=0.7, medium=0.5) == "medium"
    assert alert_level(0.75, high=0.7, medium=0.5) == "high"
    assert alert_level(0.4, high=0.7, medium=0.5) == "low"


def test_alert_level_non_finite_safe():
    assert alert_level(float("nan")) == "low"
    assert alert_level(None) == "low"


# ---------------------------------------------------------------------------
# 4. aggregate_confidence
# ---------------------------------------------------------------------------
def test_aggregate_confidence_mean_of_first_12():
    arr = np.concatenate([np.full(12, 0.8), np.full(8, 0.0)])
    # Only the first 12 are averaged -> 0.8 (the trailing zeros are ignored).
    assert aggregate_confidence(arr) == pytest.approx(0.8)


def test_aggregate_confidence_shorter_array_uses_all():
    arr = np.array([0.2, 0.4, 0.6])
    assert aggregate_confidence(arr) == pytest.approx(0.4)


def test_aggregate_confidence_empty_is_zero():
    assert aggregate_confidence(np.empty(0)) == 0.0


# ---------------------------------------------------------------------------
# 5. ensemble_forecast (KEY)
# ---------------------------------------------------------------------------
_EXPECTED_KEYS = {
    "steps",
    "ensemble_prediction",
    "ensemble_confidence",
    "individual_forecasts",
    "alert_level",
    "confidence",
    "weights_used",
    "failed_models",
    "lower",
    "upper",
}


def test_ensemble_forecast_shape_and_serialisable():
    series = _series(days=5, interval=3600, seed=11)
    linear = LinearForecaster().fit(series)
    arima = ARIMAForecaster().fit(series)
    steps = 12
    res = ensemble_forecast([linear, arima], steps)

    assert _EXPECTED_KEYS.issubset(res.keys())
    assert res["steps"] == steps
    for key in ("ensemble_prediction", "ensemble_confidence", "lower", "upper"):
        assert len(res[key]) == steps, key
    # survivor names present.
    assert set(res["individual_forecasts"]) == {"linear", "arima"}
    for name, arr in res["individual_forecasts"].items():
        assert len(arr) == steps
    # weights renormalised over survivors.
    assert sum(res["weights_used"].values()) == pytest.approx(1.0)
    assert res["alert_level"] in {"high", "medium", "low"}
    assert 0.0 <= res["confidence"] <= 1.0
    assert all(0.0 <= c <= 1.0 for c in res["ensemble_confidence"])
    assert res["failed_models"] == []
    # lower <= upper element-wise.
    assert all(lo <= up for lo, up in zip(res["lower"], res["upper"]))
    # fully JSON-serialisable.
    dumped = json.dumps(res)
    assert isinstance(dumped, str)


def test_ensemble_forecast_graceful_degradation_drops_broken_model():
    series = _series(days=5, interval=3600, seed=13)
    linear = LinearForecaster().fit(series)
    broken = _BrokenForecaster().fit(series)  # fits but predict() raises
    res = ensemble_forecast([linear, broken], steps=12)

    assert "broken" in res["failed_models"]
    # Ensemble still produced a forecast from the survivor.
    assert set(res["individual_forecasts"]) == {"linear"}
    assert len(res["ensemble_prediction"]) == 12
    assert any(v != 0.0 for v in res["ensemble_prediction"])
    json.dumps(res)


def test_ensemble_forecast_all_fail_safe_result():
    series = _series(days=5, interval=3600, seed=17)
    broken1 = _BrokenForecaster().fit(series)
    broken2 = _BrokenForecaster().fit(series)
    unfitted = LinearForecaster()  # predict() raises ForecastError (not fitted)
    steps = 12
    res = ensemble_forecast([broken1, broken2, unfitted], steps)

    assert res["ensemble_prediction"] == [0.0] * steps
    assert res["ensemble_confidence"] == [0.0] * steps
    assert res["lower"] == [0.0] * steps
    assert res["upper"] == [0.0] * steps
    assert res["alert_level"] == "low"
    assert res["confidence"] == 0.0
    assert res["individual_forecasts"] == {}
    # all members recorded as failed (note: both broken share name "broken").
    assert "broken" in res["failed_models"]
    assert "linear" in res["failed_models"]
    json.dumps(res)


def test_ensemble_forecast_steps_below_one_safe():
    series = _series(days=4, interval=3600, seed=19)
    linear = LinearForecaster().fit(series)
    res = ensemble_forecast([linear], steps=0)
    assert res["steps"] == 0
    assert res["ensemble_prediction"] == []
    assert res["alert_level"] == "low"
    assert res["confidence"] == 0.0


def test_ensemble_forecast_never_raises_on_empty_models():
    res = ensemble_forecast([], steps=12)
    assert res["alert_level"] == "low"
    assert len(res["ensemble_prediction"]) == 12
    assert res["failed_models"] == []


# ---------------------------------------------------------------------------
# 6. multi_window_ensemble
# ---------------------------------------------------------------------------
def test_multi_window_ensemble_shape():
    series = _series(days=5, interval=3600, seed=23)

    def factory():
        return [LinearForecaster(), ARIMAForecaster()]

    steps = 12
    res = multi_window_ensemble(factory, series, steps, windows=[48, 120])

    assert _EXPECTED_KEYS.issubset(res.keys())
    assert "windows_used" in res
    assert res["steps"] == steps
    for key in ("ensemble_prediction", "ensemble_confidence", "lower", "upper"):
        assert len(res[key]) == steps, key
    assert res["alert_level"] in {"high", "medium", "low"}
    assert 0.0 <= res["confidence"] <= 1.0
    assert all(0.0 <= c <= 1.0 for c in res["ensemble_confidence"])
    assert len(res["windows_used"]) >= 1
    json.dumps(res)


def test_multi_window_ensemble_window_larger_than_series():
    # ~5 days @3600s ~= 120 points; window 10_000 must clamp to series length.
    series = _series(days=5, interval=3600, seed=29)

    def factory():
        return [LinearForecaster()]

    res = multi_window_ensemble(factory, series, steps=12, windows=[10_000])
    assert len(res["ensemble_prediction"]) == 12
    # clamped window <= number of points.
    assert all(w <= len(series) for w in res["windows_used"])
    json.dumps(res)


def test_multi_window_ensemble_never_raises_on_bad_input():
    def factory():
        return [LinearForecaster()]

    # steps < 1 -> safe zeroed result with windows_used.
    res = multi_window_ensemble(factory, _series(days=3, seed=31), 0, windows=[48])
    assert res["alert_level"] == "low"
    assert res["windows_used"] == []

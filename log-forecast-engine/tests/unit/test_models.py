"""Unit tests for C4 forecasting models: metrics, base interface, ARIMA, ExpSmoothing.

Pure unit tests (no DB/API). Series are produced via the seeded synthetic
generator and kept short so statsmodels fits fast in-container.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from src.generator import generate_series
from src.models.arima import ARIMAForecaster
from src.models.base import BaseForecaster, ForecastError
from src.models.exp_smoothing import ExpSmoothingForecaster
from src.models.metrics import (
    accuracy_score_ts,
    compute_metrics,
    mae,
    mape,
    rmse,
    smape,
)

# Model classes under interface-conformance test.
MODEL_CLASSES = [ARIMAForecaster, ExpSmoothingForecaster]
EXPECTED_NAMES = {ARIMAForecaster: "arima", ExpSmoothingForecaster: "exp_smoothing"}
METRIC_KEYS = {"mape", "smape", "rmse", "mae", "accuracy"}


def _series(metric: str = "throughput", days: int = 3, interval: int = 3600, seed: int = 7):
    """Short, deterministic series (default 3 days @ 1h = 72 points)."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=days)
    return generate_series(metric, start, end, interval, seed=seed)


# ---------------------------------------------------------------------------
# 1. metrics module
# ---------------------------------------------------------------------------
def test_mae_rmse_exact() -> None:
    y_true = [1.0, 2.0, 3.0, 4.0]
    y_pred = [1.0, 2.0, 4.0, 6.0]  # errors: 0, 0, 1, 2
    assert mae(y_true, y_pred) == pytest.approx(0.75)
    # rmse = sqrt((0+0+1+4)/4) = sqrt(1.25)
    assert rmse(y_true, y_pred) == pytest.approx(math.sqrt(1.25))


def test_mape_is_fraction() -> None:
    # true=100, pred=110 -> 10% error == fraction 0.1
    assert mape([100.0], [110.0]) == pytest.approx(0.10)
    # true=[100, 200], pred=[110, 180] -> |10/100|, |20/200| = 0.1, 0.1 -> 0.1
    assert mape([100.0, 200.0], [110.0, 180.0]) == pytest.approx(0.10)


def test_smape_is_fraction_in_unit_range() -> None:
    # |a-b| / ((|a|+|b|)/2); true=100,pred=110 -> 10/105
    assert smape([100.0], [110.0]) == pytest.approx(10.0 / 105.0)
    # bounded in [0, 1] for non-negative data
    val = smape([1.0, 2.0, 3.0], [3.0, 2.0, 1.0])
    assert 0.0 <= val <= 1.0


def test_accuracy_is_one_minus_smape_in_unit_range() -> None:
    y_true = [100.0, 200.0, 300.0]
    y_pred = [110.0, 180.0, 330.0]
    sm = smape(y_true, y_pred)
    acc = accuracy_score_ts(y_true, y_pred)
    assert 0.0 <= acc <= 1.0
    assert acc == pytest.approx(min(1.0, max(0.0, 1.0 - sm)))


def test_accuracy_perfect_forecast_is_one() -> None:
    assert accuracy_score_ts([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_compute_metrics_keys_exact() -> None:
    out = compute_metrics([1.0, 2.0, 3.0], [1.1, 2.1, 2.9])
    assert set(out.keys()) == METRIC_KEYS
    assert all(isinstance(v, float) for v in out.values())


def test_metrics_degenerate_empty_no_raise() -> None:
    for fn in (mae, rmse, mape, smape):
        assert math.isinf(fn([], []))
    assert accuracy_score_ts([], []) == 0.0
    out = compute_metrics([], [])
    assert math.isinf(out["mape"]) and out["accuracy"] == 0.0


def test_metrics_all_zero_true_no_raise() -> None:
    # all-equal-zero true: error metrics finite (eps-guarded), no exception.
    out = compute_metrics([0.0, 0.0, 0.0], [0.0, 0.0, 0.0])
    assert set(out.keys()) == METRIC_KEYS
    # perfect match -> errors ~0, accuracy 1
    assert out["mae"] == pytest.approx(0.0)
    assert out["rmse"] == pytest.approx(0.0)
    assert out["accuracy"] == pytest.approx(1.0)


def test_metrics_drop_nonfinite_pairs() -> None:
    # NaN/inf pairs dropped; remaining (2,2),(4,4) -> perfect
    y_true = [1.0, float("nan"), 2.0, float("inf"), 4.0]
    y_pred = [float("inf"), 5.0, 2.0, 9.0, 4.0]
    # surviving pairs: index2 (2,2), index4 (4,4) -> mae 0
    assert mae(y_true, y_pred) == pytest.approx(0.0)


def test_metrics_never_raise_on_garbage() -> None:
    # length mismatch + non-finite should degrade, not raise
    out = compute_metrics([1.0, 2.0], [1.0])
    assert set(out.keys()) == METRIC_KEYS


# ---------------------------------------------------------------------------
# 2. Interface conformance (parametrized over both models)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_name_attribute(cls) -> None:
    assert cls.name == EXPECTED_NAMES[cls]
    assert cls().name == EXPECTED_NAMES[cls]


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_predict_before_fit_raises(cls) -> None:
    model = cls()
    assert model.is_fitted is False
    with pytest.raises(ForecastError):
        model.predict(5)


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_fit_returns_self_and_sets_fitted(cls) -> None:
    model = cls()
    returned = model.fit(_series())
    assert returned is model
    assert model.is_fitted is True


@pytest.mark.parametrize("cls", MODEL_CLASSES)
@pytest.mark.parametrize("steps", [1, 12])
def test_predict_length_and_finite(cls, steps) -> None:
    model = cls().fit(_series())
    out = model.predict(steps)
    assert isinstance(out, np.ndarray)
    assert out.shape == (steps,)
    assert np.all(np.isfinite(out))


@pytest.mark.parametrize("cls", MODEL_CLASSES)
@pytest.mark.parametrize("steps", [1, 12])
def test_predict_interval_contract(cls, steps) -> None:
    model = cls().fit(_series())
    lower, upper = model.predict_interval(steps)
    assert lower.shape == (steps,)
    assert upper.shape == (steps,)
    assert np.all(np.isfinite(lower)) and np.all(np.isfinite(upper))
    # Contract: lower <= upper element-wise.
    assert np.all(lower <= upper)


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_returns_bundle_in_range(cls) -> None:
    model = cls()
    out = model.validate(_series(), horizon=12)
    assert set(out.keys()) == METRIC_KEYS
    assert 0.0 <= out["accuracy"] <= 1.0


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_save_load_roundtrip_identical(cls, tmp_path) -> None:
    model = cls().fit(_series())
    original = model.predict(12)
    path = str(tmp_path / f"{cls.name}.joblib")
    model.save(path)
    loaded = BaseForecaster.load(path)
    assert isinstance(loaded, cls)
    assert loaded.is_fitted is True
    assert np.allclose(loaded.predict(12), original)


# ---------------------------------------------------------------------------
# 3. Accuracy sanity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_accuracy_sanity_real_forecast(cls) -> None:
    # Clean trending/seasonal generated series -> real (non-garbage) forecast.
    model = cls()
    out = model.validate(_series(metric="throughput", days=4, interval=3600), horizon=12)
    assert out["accuracy"] > 0.3, f"{cls.name} accuracy too low: {out}"


# ---------------------------------------------------------------------------
# 4. Graceful failure
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_fit_empty_raises_forecast_error(cls) -> None:
    with pytest.raises(ForecastError):
        cls().fit([])


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_fit_too_short_raises_forecast_error(cls) -> None:
    # Single point -> ForecastError (not a raw statsmodels/pandas error).
    one_point = _series()[:1]
    with pytest.raises(ForecastError):
        cls().fit(one_point)


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_too_short_returns_degenerate_no_raise(cls) -> None:
    model = cls()
    out = model.validate(_series()[:1], horizon=12)  # must not raise
    assert set(out.keys()) == METRIC_KEYS
    assert math.isinf(out["mape"])
    assert out["accuracy"] == 0.0


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_empty_returns_degenerate_no_raise(cls) -> None:
    out = cls().validate([], horizon=12)
    assert out["accuracy"] == 0.0
    assert math.isinf(out["rmse"])

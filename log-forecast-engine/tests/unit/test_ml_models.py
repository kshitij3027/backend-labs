"""Unit tests for C5 ML forecasters: LinearForecaster + XGBoostForecaster.

Pure unit tests (no DB/API). Both models implement the SAME BaseForecaster
contract verified for the statistical models in C4 (``test_models.py``); these
tests mirror that style, parametrized over the two ML members.

Series come from the seeded synthetic generator and are kept modest in length
(long enough for the max lag of 12 and windows up to 12) and the XGBoost params
are left at their deliberately-modest defaults so fits stay fast in-container.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from src.generator import generate_series
from src.models import LinearForecaster, XGBoostForecaster
from src.models.base import BaseForecaster, ForecastError

# Also exercise the documented direct-module import paths.
from src.models.linear import LinearForecaster as LinearForecasterDirect  # noqa: F401
from src.models.xgboost_model import (  # noqa: F401
    XGBoostForecaster as XGBoostForecasterDirect,
)

MODEL_CLASSES = [LinearForecaster, XGBoostForecaster]
EXPECTED_NAMES = {LinearForecaster: "linear", XGBoostForecaster: "xgboost"}
METRIC_KEYS = {"mape", "smape", "rmse", "mae", "accuracy"}


def _series(metric: str = "throughput", days: int = 5, interval: int = 3600, seed: int = 7):
    """Deterministic series long enough for lags<=12 / windows<=12.

    Default: 5 days @ 1h = 120 points, leaving plenty of complete feature rows
    after the lag/rolling warm-up.
    """
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=days)
    return generate_series(metric, start, end, interval, seed=seed)


def _values(points) -> np.ndarray:
    return np.array([p.value for p in points], dtype=float)


# ---------------------------------------------------------------------------
# 1. Interface conformance
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
@pytest.mark.parametrize("steps", [1, 6, 12])
def test_predict_length_and_finite(cls, steps) -> None:
    model = cls().fit(_series())
    out = model.predict(steps)
    assert isinstance(out, np.ndarray)
    assert out.shape == (steps,)
    assert np.all(np.isfinite(out))


# ---------------------------------------------------------------------------
# 2. Recursive multi-step sanity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_recursive_multistep_plausible_range(cls) -> None:
    points = _series()
    train = _values(points)
    lo, hi = float(train.min()), float(train.max())
    span = hi - lo if hi > lo else max(abs(hi), 1.0)

    model = cls().fit(points)
    out = model.predict(12)
    assert out.shape == (12,)
    assert np.all(np.isfinite(out))
    # Plausible band: within a few spans of the observed training range. This
    # catches a runaway recursion (inf/nan/absurd drift) without being brittle.
    assert np.all(out >= lo - 3.0 * span)
    assert np.all(out <= hi + 3.0 * span)
    # Recursion should not collapse to a single repeated constant for a
    # seasonal/trending series.
    assert np.unique(np.round(out, 6)).size > 1


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_predict_one_then_twelve_both_finite(cls) -> None:
    # steps=1 and steps=12 are independent recursive rollouts; the first element
    # need not match exactly, but both must be finite (no strict equality).
    model = cls().fit(_series())
    one = model.predict(1)
    twelve = model.predict(12)
    assert one.shape == (1,) and twelve.shape == (12,)
    assert np.isfinite(one[0]) and np.all(np.isfinite(twelve))


# ---------------------------------------------------------------------------
# 3. No-leakage smoke
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_predict_needs_no_future_data(cls) -> None:
    # The contract: predict(steps) takes only the horizon, no future actuals.
    # Asserting a correct-length, all-finite recursive forecast is the closest
    # observable proxy for "no future data was required".
    model = cls().fit(_series())
    out = model.predict(12)
    assert out.shape == (12,)
    assert np.all(np.isfinite(out))


# ---------------------------------------------------------------------------
# 4. Prediction intervals
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
@pytest.mark.parametrize("steps", [1, 6, 12])
def test_predict_interval_contract(cls, steps) -> None:
    model = cls().fit(_series())
    lower, upper = model.predict_interval(steps)
    assert lower.shape == (steps,)
    assert upper.shape == (steps,)
    assert np.all(np.isfinite(lower)) and np.all(np.isfinite(upper))
    assert np.all(lower <= upper)


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_predict_interval_widens_with_horizon(cls) -> None:
    model = cls().fit(_series())
    lower, upper = model.predict_interval(12)
    width = upper - lower
    # Gaussian band scales ~sqrt(h), so the step-12 band is at least as wide as
    # the step-1 band (allow >= to avoid float-edge flakiness).
    assert width[-1] >= width[0]


# ---------------------------------------------------------------------------
# 5. validate
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_returns_bundle_in_range(cls) -> None:
    out = cls().validate(_series(), horizon=12)
    assert set(out.keys()) == METRIC_KEYS
    assert 0.0 <= out["accuracy"] <= 1.0
    assert all(isinstance(v, float) for v in out.values())


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_accuracy_sanity_on_clean_series(cls) -> None:
    # Clean trending/seasonal series -> a real (non-garbage) forecast.
    out = cls().validate(_series(metric="throughput", days=5, interval=3600), horizon=12)
    assert out["accuracy"] > 0.3, f"{cls.name} accuracy too low: {out}"


# ---------------------------------------------------------------------------
# 6. save / load roundtrip
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_save_load_roundtrip_identical(cls, tmp_path) -> None:
    model = cls().fit(_series())
    original = model.predict(8)
    path = str(tmp_path / f"{cls.name}.joblib")
    model.save(path)
    loaded = BaseForecaster.load(path)
    assert isinstance(loaded, cls)
    assert loaded.is_fitted is True
    assert np.allclose(loaded.predict(8), original)


# ---------------------------------------------------------------------------
# 7. _clone_kwargs round-trips through validate
# ---------------------------------------------------------------------------
def test_linear_clone_kwargs_via_validate() -> None:
    # validate() internally rebuilds the model via _clone_kwargs(); a non-default
    # alpha must round-trip without error and yield a valid metric bundle.
    model = LinearForecaster(alpha=2.0)
    assert model._clone_kwargs()["alpha"] == 2.0
    out = model.validate(_series(), horizon=12)
    assert set(out.keys()) == METRIC_KEYS
    assert 0.0 <= out["accuracy"] <= 1.0


def test_xgboost_clone_kwargs_via_validate() -> None:
    model = XGBoostForecaster(n_estimators=50)
    assert model._clone_kwargs()["n_estimators"] == 50
    out = model.validate(_series(), horizon=12)
    assert set(out.keys()) == METRIC_KEYS
    assert 0.0 <= out["accuracy"] <= 1.0


# ---------------------------------------------------------------------------
# 8. Graceful failure
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_fit_empty_raises_forecast_error(cls) -> None:
    with pytest.raises(ForecastError):
        cls().fit([])


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_fit_too_short_raises_forecast_error(cls) -> None:
    # 3 points: cannot build any complete feature row (max lag 12, windows 12)
    # -> ForecastError, not a raw sklearn/xgboost/pandas error.
    too_short = _series()[:3]
    with pytest.raises(ForecastError):
        cls().fit(too_short)


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_too_short_returns_degenerate_no_raise(cls) -> None:
    out = cls().validate(_series()[:3], horizon=12)  # must not raise
    assert set(out.keys()) == METRIC_KEYS
    assert math.isinf(out["mape"])
    assert out["accuracy"] == 0.0


@pytest.mark.parametrize("cls", MODEL_CLASSES)
def test_validate_empty_returns_degenerate_no_raise(cls) -> None:
    out = cls().validate([], horizon=12)
    assert out["accuracy"] == 0.0
    assert math.isinf(out["rmse"])

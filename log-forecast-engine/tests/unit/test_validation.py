"""Unit tests for C6: validation / deploy-gate module (src.validation).

Pure logic tests (no DB/API). Series come from the seeded synthetic generator
and are kept short (~4 days @ 1h = 96 points) with a modest horizon so the
statistical fits run fast in-container.

Covers:
  * backtest_model — key shape, accuracy range, threshold gating, never-raises.
  * walk_forward_validate — fold keys, bare metric keys, short-series fallback.
  * evaluate_models — the deploy gate (deployed/rejected), graceful degradation
    with a deliberately broken model, empty-list edge case.
  * accuracy_to_weights — the weighting formula, sum==1.0, rejected weight 0,
    no-deployed and all-zero fallbacks, both input shapes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from src.generator import generate_series
from src.models.arima import ARIMAForecaster
from src.models.base import BaseForecaster, ForecastError
from src.models.exp_smoothing import ExpSmoothingForecaster
from src.models.linear import LinearForecaster
from src.models.xgboost_model import XGBoostForecaster
from src.validation import (
    accuracy_to_weights,
    backtest_model,
    evaluate_models,
    walk_forward_validate,
)

HORIZON = 6
_ERROR_KEYS = ("mape", "smape", "rmse", "mae")
_BACKTEST_KEYS = {"name", "mape", "smape", "rmse", "mae", "accuracy", "passed"}


def _series(metric: str = "throughput", days: int = 4, interval: int = 3600, seed: int = 7):
    """Deterministic ~4-day hourly series (96 points)."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=days)
    return generate_series(metric, start, end, interval, seed=seed)


def _all_models() -> list[BaseForecaster]:
    return [
        ARIMAForecaster(),
        ExpSmoothingForecaster(),
        LinearForecaster(),
        XGBoostForecaster(),
    ]


class BrokenForecaster(BaseForecaster):
    """A forecaster whose fit always blows up — used to test graceful degradation."""

    name = "broken"

    def _fit_impl(self, series):  # noqa: ANN001
        raise ForecastError("broken: intentional fit failure for testing")

    def _predict_impl(self, steps):  # noqa: ANN001
        return np.zeros(steps, dtype=float)

    def _predict_interval_impl(self, steps, alpha):  # noqa: ANN001
        z = np.zeros(steps, dtype=float)
        return z, z


# ---------------------------------------------------------------------------
# 1. backtest_model
# ---------------------------------------------------------------------------
def test_backtest_model_key_shape_and_range() -> None:
    res = backtest_model(ARIMAForecaster(), _series(), HORIZON)
    assert _BACKTEST_KEYS.issubset(res.keys())
    assert res["name"] == "arima"
    assert 0.0 <= res["accuracy"] <= 1.0
    assert isinstance(res["passed"], bool)
    # Default deploy threshold is 0.6; passed must be consistent with accuracy.
    assert res["passed"] == (res["accuracy"] >= 0.6)


def test_backtest_model_threshold_floor_passes() -> None:
    res = backtest_model(LinearForecaster(), _series(), HORIZON, threshold=0.0)
    assert res["passed"] is True
    assert res["accuracy"] >= 0.0


def test_backtest_model_threshold_above_one_rejects() -> None:
    res = backtest_model(LinearForecaster(), _series(), HORIZON, threshold=1.01)
    assert res["passed"] is False


def test_backtest_model_passed_matches_explicit_threshold() -> None:
    res = backtest_model(ExpSmoothingForecaster(), _series(), HORIZON, threshold=0.5)
    assert res["passed"] == (res["accuracy"] >= 0.5)


def test_backtest_model_too_short_degrades_without_raising() -> None:
    # A series too short to validate -> degenerate, rejected, never raises.
    short = _series()[:1]
    res = backtest_model(ARIMAForecaster(), short, HORIZON)
    assert res["accuracy"] == 0.0
    assert res["name"] == "arima"
    assert res["passed"] is False
    # A degenerate (failed) validation must surface the promised "error" string,
    # even though BaseForecaster.validate swallows the failure internally and
    # returns a non-finite-error bundle rather than raising.
    assert "error" in res
    assert isinstance(res["error"], str) and res["error"]


# ---------------------------------------------------------------------------
# 2. walk_forward_validate
# ---------------------------------------------------------------------------
def test_walk_forward_returns_fold_and_bare_keys() -> None:
    res = walk_forward_validate(LinearForecaster(), _series(), HORIZON, n_splits=3)
    for key in ("name", "accuracy", "accuracy_mean", "accuracy_std",
                "fold_accuracies", "n_folds"):
        assert key in res
    # bare + *_mean metric keys both present (interchangeable with a holdout result).
    for k in _ERROR_KEYS:
        assert k in res
        assert f"{k}_mean" in res
    assert 0.0 <= res["accuracy"] <= 1.0
    assert res["accuracy"] == pytest.approx(res["accuracy_mean"])
    assert isinstance(res["fold_accuracies"], list)
    assert res["accuracy_std"] >= 0.0


def test_walk_forward_short_series_falls_back_no_raise() -> None:
    # Too short for expanding folds -> single-holdout fallback, walk-forward shape.
    short = _series()[:5]
    res = walk_forward_validate(ExpSmoothingForecaster(), short, HORIZON)
    assert "accuracy_mean" in res
    assert "fold_accuracies" in res
    assert 0.0 <= res["accuracy"] <= 1.0


def test_walk_forward_degenerate_series_no_raise() -> None:
    res = walk_forward_validate(ARIMAForecaster(), [], HORIZON)
    assert res["accuracy"] == 0.0
    assert res["passed"] is False


# ---------------------------------------------------------------------------
# 3. evaluate_models — the deploy gate (KEY)
# ---------------------------------------------------------------------------
def test_evaluate_models_all_deploy_at_zero_threshold() -> None:
    out = evaluate_models(_all_models(), _series(), HORIZON, threshold=0.0)
    assert out["any_deployed"] is True
    assert out["rejected"] == []
    expected = {"arima", "exp_smoothing", "linear", "xgboost"}
    assert set(out["deployed"]) == expected
    assert set(out["results"].keys()) == expected
    assert out["threshold"] == 0.0
    assert out["method"] == "holdout"


def test_evaluate_models_high_threshold_rejects_all() -> None:
    out = evaluate_models(_all_models(), _series(), HORIZON, threshold=0.99)
    assert out["deployed"] == []
    assert out["any_deployed"] is False
    assert set(out["rejected"]) == {"arima", "exp_smoothing", "linear", "xgboost"}


def test_evaluate_models_graceful_degradation_no_raise() -> None:
    # KEY graceful-degradation guarantee: a model that blows up in fit must not
    # crash the whole evaluation; the call returns and still evaluates the others.
    models = [LinearForecaster(), BrokenForecaster(), ARIMAForecaster()]
    out = evaluate_models(models, _series(), HORIZON, threshold=0.0)
    assert set(out["results"].keys()) == {"linear", "broken", "arima"}
    broken_res = out["results"]["broken"]
    assert broken_res["accuracy"] == 0.0  # degenerate, as expected
    # The healthy models are still evaluated and deployed at threshold 0.0.
    assert "linear" in out["deployed"]
    assert "arima" in out["deployed"]


def test_evaluate_models_broken_model_rejected_under_real_threshold() -> None:
    # Under the DEFAULT deploy threshold (0.6) a broken model is correctly
    # rejected: its degenerate accuracy 0.0 < 0.6.
    out = evaluate_models([BrokenForecaster()], _series(), HORIZON)
    assert "broken" in out["rejected"]
    assert "broken" not in out["deployed"]
    assert out["results"]["broken"]["passed"] is False


def test_evaluate_models_broken_model_rejected_at_zero_threshold() -> None:
    # A completely broken model must NEVER deploy, even at threshold=0.0. A failed
    # (degenerate) validation is detected via its non-finite error metrics and
    # forced to passed=False with an "error" string, instead of slipping through
    # the `accuracy >= threshold` check (0.0 >= 0.0). Healthy models still deploy.
    models = [LinearForecaster(), BrokenForecaster(), ARIMAForecaster()]
    out = evaluate_models(models, _series(), HORIZON, threshold=0.0)
    assert "broken" in out["rejected"]
    assert "broken" not in out["deployed"]
    broken_res = out["results"]["broken"]
    assert broken_res["passed"] is False
    assert "error" in broken_res
    assert isinstance(broken_res["error"], str) and broken_res["error"]
    # The real models are unaffected and still deploy at threshold 0.0.
    assert "linear" in out["deployed"]
    assert "arima" in out["deployed"]


def test_evaluate_models_empty_list() -> None:
    out = evaluate_models([], _series(), HORIZON)
    assert out["results"] == {}
    assert out["deployed"] == []
    assert out["rejected"] == []
    assert out["any_deployed"] is False


def test_evaluate_models_walk_forward_method() -> None:
    out = evaluate_models(
        _all_models(), _series(), HORIZON, threshold=0.0, method="walk_forward"
    )
    assert out["method"] == "walk_forward"
    assert out["any_deployed"] is True


# ---------------------------------------------------------------------------
# 4. accuracy_to_weights
# ---------------------------------------------------------------------------
def test_accuracy_to_weights_formula_and_sum() -> None:
    full = {
        "results": {
            "arima": {"accuracy": 0.8, "passed": True},
            "linear": {"accuracy": 0.4, "passed": True},
        },
        "deployed": ["arima", "linear"],
    }
    base = {"arima": 0.5, "linear": 0.5}
    weights = accuracy_to_weights(full, base_weights=base)
    # raw: arima = 0.5*0.8 = 0.4 ; linear = 0.5*0.4 = 0.2 ; total = 0.6
    assert weights["arima"] == pytest.approx(0.4 / 0.6)
    assert weights["linear"] == pytest.approx(0.2 / 0.6)
    assert np.isclose(sum(weights.values()), 1.0)


def test_accuracy_to_weights_rejected_get_zero() -> None:
    full = {
        "results": {
            "arima": {"accuracy": 0.8, "passed": True},
            "linear": {"accuracy": 0.9, "passed": False},  # rejected
        },
        "deployed": ["arima"],
    }
    base = {"arima": 0.5, "linear": 0.5}
    weights = accuracy_to_weights(full, base_weights=base)
    # Only the deployed model appears; rejected is absent (weight 0).
    assert set(weights.keys()) == {"arima"}
    assert weights.get("linear", 0.0) == 0.0
    assert weights["arima"] == pytest.approx(1.0)
    assert np.isclose(sum(weights.values()), 1.0)


def test_accuracy_to_weights_no_deployed_returns_empty() -> None:
    full = {"results": {"arima": {"accuracy": 0.8, "passed": False}}, "deployed": []}
    assert accuracy_to_weights(full) == {}


def test_accuracy_to_weights_all_zero_accuracy_uniform() -> None:
    full = {
        "results": {
            "arima": {"accuracy": 0.0, "passed": True},
            "linear": {"accuracy": 0.0, "passed": True},
        },
        "deployed": ["arima", "linear"],
    }
    base = {"arima": 0.5, "linear": 0.5}
    weights = accuracy_to_weights(full, base_weights=base)
    # All raw products zero -> uniform split.
    assert weights["arima"] == pytest.approx(0.5)
    assert weights["linear"] == pytest.approx(0.5)
    assert np.isclose(sum(weights.values()), 1.0)


def test_accuracy_to_weights_accepts_bare_map() -> None:
    bare = {
        "arima": {"accuracy": 0.8, "passed": True},
        "linear": {"accuracy": 0.4, "passed": True},
        "xgboost": {"accuracy": 0.9, "passed": False},  # rejected -> excluded
    }
    base = {"arima": 0.5, "linear": 0.5, "xgboost": 0.5}
    weights = accuracy_to_weights(bare, base_weights=base)
    assert set(weights.keys()) == {"arima", "linear"}
    assert weights["arima"] == pytest.approx(0.4 / 0.6)
    assert weights["linear"] == pytest.approx(0.2 / 0.6)
    assert np.isclose(sum(weights.values()), 1.0)


def test_accuracy_to_weights_default_base_weights_no_raise() -> None:
    # Real evaluate_models output + default (settings) base weights -> sane weights.
    out = evaluate_models(_all_models(), _series(), HORIZON, threshold=0.0)
    weights = accuracy_to_weights(out)
    assert set(weights.keys()) == set(out["deployed"])
    assert np.isclose(sum(weights.values()), 1.0)
    assert all(w >= 0.0 for w in weights.values())

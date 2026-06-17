"""Unit tests for :mod:`src.forecast` — Holt's linear trend + confidence.

The module is pure math, so these tests exercise it directly with crafted
series: monotonic ramps (trend sign + projected value), constant series (flat +
high confidence), noisy series (lower confidence), and degenerate inputs
(empty / single element) which must never raise.
"""

import math

import pytest

from src.forecast import holt_forecast, confidence, build_forecast


# Canonical keys returned by build_forecast (mirrors the system-wide payload).
CANONICAL_KEYS = {
    "metric",
    "current",
    "predicted",
    "horizon_minutes",
    "trend",
    "confidence",
    "level",
    "slope",
}


# --------------------------------------------------------------------------- #
# holt_forecast — trend direction
# --------------------------------------------------------------------------- #
def test_rising_series_has_positive_slope_and_higher_prediction():
    """A strictly increasing ramp yields slope>0, trend 'rising', predicted>current."""
    result = holt_forecast([10, 20, 30, 40, 50], horizon_steps=3)

    assert result["slope"] > 0.0
    assert result["trend"] == "rising"
    assert result["predicted"] > result["current"]
    assert result["current"] == 50.0
    assert math.isfinite(result["predicted"])


def test_falling_series_has_negative_slope_and_lower_prediction():
    """A strictly decreasing ramp yields slope<0, trend 'falling', predicted<current."""
    result = holt_forecast([50, 40, 30, 20, 10], horizon_steps=3)

    assert result["slope"] < 0.0
    assert result["trend"] == "falling"
    assert result["predicted"] < result["current"]
    assert result["current"] == 10.0
    assert math.isfinite(result["predicted"])


def test_flat_series_has_near_zero_slope_and_flat_trend():
    """A constant series yields ~zero slope and trend 'flat'."""
    result = holt_forecast([50, 50, 50, 50, 50], horizon_steps=3)

    assert abs(result["slope"]) < 1e-3
    assert result["trend"] == "flat"
    # Level should settle on the constant value; prediction tracks it.
    assert result["level"] == pytest.approx(50.0, abs=1e-6)
    assert result["predicted"] == pytest.approx(50.0, abs=1e-3)


def test_predicted_never_negative_for_steep_fall_projected_far():
    """Projecting a steep downtrend far out must floor predicted at 0, not go negative."""
    result = holt_forecast([100, 80, 60, 40, 20], horizon_steps=100)
    assert result["predicted"] >= 0.0
    assert math.isfinite(result["predicted"])


def test_level_and_slope_are_floats():
    """Returned numeric fields are plain Python floats (JSON-friendly)."""
    result = holt_forecast([1, 2, 3], horizon_steps=1)
    for key in ("current", "predicted", "level", "slope"):
        assert isinstance(result[key], float)
    assert isinstance(result["trend"], str)


# --------------------------------------------------------------------------- #
# holt_forecast — degenerate inputs (must not raise)
# --------------------------------------------------------------------------- #
def test_empty_series_returns_neutral_zero():
    """An empty series returns all-zero neutral values with trend 'flat'."""
    result = holt_forecast([], horizon_steps=5)
    assert result["current"] == 0.0
    assert result["predicted"] == 0.0
    assert result["level"] == 0.0
    assert result["slope"] == 0.0
    assert result["trend"] == "flat"
    assert math.isfinite(result["predicted"])


def test_single_element_series_is_flat_and_tracks_value():
    """A single-element series has slope 0, level/predicted equal to the value."""
    result = holt_forecast([42], horizon_steps=5)
    assert result["current"] == 42.0
    assert result["predicted"] == 42.0
    assert result["level"] == 42.0
    assert result["slope"] == 0.0
    assert result["trend"] == "flat"
    assert math.isfinite(result["predicted"])


def test_two_element_series_does_not_raise():
    """The minimal trend-bearing series (two points) is handled cleanly."""
    result = holt_forecast([10, 20], horizon_steps=2)
    assert math.isfinite(result["predicted"])
    assert result["slope"] > 0.0
    assert result["trend"] == "rising"


# --------------------------------------------------------------------------- #
# confidence
# --------------------------------------------------------------------------- #
def test_confidence_high_for_flat_series():
    """A perfectly flat series is maximally stable -> high confidence.

    With no residuals the spec blends ``0.6 * stability + 0.4 * 0.5``; a flat
    series has ``stability == 1.0`` so the score reaches the formula's ceiling of
    ``0.8`` (it climbs toward 1.0 once small residuals are supplied).
    """
    score = confidence([50, 50, 50, 50, 50])
    assert score >= 0.8
    assert 0.0 <= score <= 1.0
    # Supplying tiny recent residuals pushes confidence to near-1.0.
    assert confidence([50, 50, 50, 50, 50], recent_residuals=[0.0, 0.0]) > 0.95


def test_noisy_series_has_lower_confidence_than_flat():
    """A noisy series scores strictly lower confidence than a flat one."""
    flat_score = confidence([50, 50, 50, 50, 50])
    noisy_score = confidence([50, 10, 60, 5, 55, 8])
    assert noisy_score < flat_score


def test_confidence_within_bounds_for_various_inputs():
    """Confidence stays within [0, 1] across a spread of inputs."""
    samples = [
        [50, 50, 50, 50, 50],
        [10, 20, 30, 40, 50],
        [50, 10, 60, 5, 55, 8],
        [0, 0, 0, 0],
        [0, 100, 0, 100],
        [-5, 5, -5, 5],
        [1000000, 1000001, 999999, 1000000],
    ]
    for series in samples:
        score = confidence(series)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0, f"out of bounds for {series}: {score}"


def test_confidence_zero_for_empty_and_single():
    """Insufficient data (len < 2) returns 0.0 confidence."""
    assert confidence([]) == 0.0
    assert confidence([42]) == 0.0


def test_confidence_uses_residuals_when_provided():
    """Small recent residuals raise confidence vs. large ones on the same series."""
    series = [50, 52, 49, 51, 50]
    small_resid = confidence(series, recent_residuals=[0.5, 0.3, 0.4])
    large_resid = confidence(series, recent_residuals=[40.0, 35.0, 50.0])
    assert small_resid > large_resid
    assert 0.0 <= small_resid <= 1.0
    assert 0.0 <= large_resid <= 1.0


def test_confidence_ignores_single_residual():
    """A single residual (len < 2) is ignored, falling back to the neutral prior."""
    series = [50, 52, 49, 51, 50]
    with_one = confidence(series, recent_residuals=[1.0])
    without = confidence(series, recent_residuals=None)
    assert with_one == pytest.approx(without)


# --------------------------------------------------------------------------- #
# build_forecast — canonical assembler
# --------------------------------------------------------------------------- #
def test_build_forecast_returns_all_canonical_keys_with_types():
    """build_forecast returns exactly the canonical keys with correct types."""
    result = build_forecast(
        [10, 20, 30, 40, 50],
        horizon_steps=2,
        metric="effective_utilization",
        horizon_minutes=10,
    )

    assert set(result.keys()) == CANONICAL_KEYS

    assert isinstance(result["metric"], str)
    assert result["metric"] == "effective_utilization"
    assert isinstance(result["current"], float)
    assert isinstance(result["predicted"], float)
    assert isinstance(result["horizon_minutes"], int)
    assert result["horizon_minutes"] == 10
    assert result["trend"] in ("rising", "falling", "flat")
    assert isinstance(result["confidence"], float)
    assert 0.0 <= result["confidence"] <= 1.0
    assert isinstance(result["level"], float)
    assert isinstance(result["slope"], float)


def test_build_forecast_horizon_minutes_passed_through():
    """The horizon_minutes argument is surfaced verbatim (as int)."""
    result = build_forecast(
        [5, 6, 7],
        horizon_steps=1,
        metric="cpu",
        horizon_minutes=15,
    )
    assert result["horizon_minutes"] == 15
    assert isinstance(result["horizon_minutes"], int)


def test_build_forecast_predicted_never_negative_far_horizon():
    """Even a steep downtrend projected far out keeps predicted >= 0."""
    result = build_forecast(
        [100, 80, 60, 40, 20],
        horizon_steps=500,
        metric="effective_utilization",
        horizon_minutes=10,
    )
    assert result["predicted"] >= 0.0
    assert math.isfinite(result["predicted"])


def test_build_forecast_matches_underlying_functions():
    """Assembled fields agree with the standalone holt_forecast / confidence calls."""
    series = [12, 18, 25, 31, 40]
    h = holt_forecast(series, horizon_steps=3, alpha=0.25, beta=0.10)
    conf = confidence(series)
    result = build_forecast(
        series,
        horizon_steps=3,
        metric="effective_utilization",
        horizon_minutes=10,
    )
    assert result["current"] == pytest.approx(h["current"])
    assert result["predicted"] == pytest.approx(h["predicted"])
    assert result["level"] == pytest.approx(h["level"])
    assert result["slope"] == pytest.approx(h["slope"])
    assert result["trend"] == h["trend"]
    assert result["confidence"] == pytest.approx(conf)


def test_build_forecast_empty_series_is_graceful():
    """An empty series produces a well-formed canonical payload, no exception."""
    result = build_forecast(
        [],
        horizon_steps=3,
        metric="effective_utilization",
        horizon_minutes=10,
    )
    assert set(result.keys()) == CANONICAL_KEYS
    assert result["current"] == 0.0
    assert result["predicted"] == 0.0
    assert result["trend"] == "flat"
    assert result["confidence"] == 0.0

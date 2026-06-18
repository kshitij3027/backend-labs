"""Unit tests for :mod:`src.patterns` — anomaly detection and pattern learning.

Both classes are pure (no I/O, no other ``src`` imports), so these tests drive
their public methods directly with crafted series / observations and assert on the
returned values. Timestamps are derived from a fixed base and offset by whole hours
so the *local* hour-of-day buckets differ deterministically regardless of the host
timezone (we assert on the relationship between two distinct hours, never on a
specific clock hour).
"""

import time

import pytest

from src.patterns import AnomalyDetector, PatternLearner


# A fixed, far-from-DST-edge base instant. We only ever offset it by whole hours,
# so the exact wall-clock hour it lands on is irrelevant to the assertions.
BASE_TS = 1_700_000_000.0  # 2023-11-14T...Z
ONE_HOUR = 3600.0


# --------------------------------------------------------------------------- #
# AnomalyDetector
# --------------------------------------------------------------------------- #
def test_stable_series_is_not_anomalous():
    """A perfectly flat series has zero spread → never anomalous, zscore 0."""
    det = AnomalyDetector()
    result = det.detect([50.0] * 20)

    assert result["active"] is False
    assert result["zscore"] == 0.0


def test_final_spike_is_anomalous_with_large_zscore():
    """A long calm series ending in a huge spike → active, large positive zscore."""
    det = AnomalyDetector()
    series = [50.0] * 19 + [500.0]

    result = det.detect(series)

    assert result["active"] is True
    # The spike sits many sigma above the (mostly-50) mean.
    assert result["zscore"] > 3.0


def test_downward_spike_has_negative_zscore():
    """A sudden dip is anomalous but reports a NEGATIVE zscore (not an up-spike)."""
    det = AnomalyDetector()
    series = [500.0] * 19 + [50.0]

    result = det.detect(series)

    assert result["active"] is True
    assert result["zscore"] < 0.0


def test_too_short_series_is_not_anomalous():
    """Fewer than ``min_samples`` points → not enough evidence, neutral result."""
    det = AnomalyDetector(min_samples=10)
    # Only 5 points, with an obvious outlier that must still be ignored.
    result = det.detect([50.0, 50.0, 50.0, 50.0, 9999.0])

    assert result["active"] is False
    assert result["zscore"] == 0.0


def test_empty_series_is_not_anomalous():
    """An empty series is handled defensively (no crash) → neutral result."""
    det = AnomalyDetector()
    assert det.detect([]) == {"active": False, "zscore": 0.0}


def test_z_threshold_is_configurable():
    """A moderate outlier flips active depending on the configured z_threshold."""
    series = [50.0] * 19 + [80.0]  # a modest bump

    # The same series can be sub-threshold at 3-sigma but anomalous at 1-sigma.
    strict = AnomalyDetector(z_threshold=3.0).detect(series)
    lenient = AnomalyDetector(z_threshold=1.0).detect(series)

    assert lenient["active"] is True
    assert strict["zscore"] == lenient["zscore"]  # same z, different verdict
    assert lenient["zscore"] > 0.0


# --------------------------------------------------------------------------- #
# PatternLearner
# --------------------------------------------------------------------------- #
def test_seasonality_factor_high_hour_above_one_low_hour_below_one():
    """Observing a hot hour and a cool hour yields factors >1 and <1 respectively."""
    learner = PatternLearner()

    high_ts = BASE_TS
    low_ts = BASE_TS + ONE_HOUR
    # Guard the construction: the two timestamps must fall in different local hours.
    assert time.localtime(high_ts).tm_hour != time.localtime(low_ts).tm_hour

    # Many observations: the "high" hour runs much hotter than the "low" hour.
    for i in range(50):
        learner.observe(high_ts + i, 90.0)
        learner.observe(low_ts + i, 10.0)

    high_factor = learner.seasonality_factor(high_ts)
    low_factor = learner.seasonality_factor(low_ts)

    assert high_factor > 1.0
    assert low_factor < 1.0
    # Both stay inside the documented clamp band.
    assert 0.25 <= high_factor <= 4.0
    assert 0.25 <= low_factor <= 4.0


def test_seasonality_factor_is_one_without_data():
    """With no observations the factor is the neutral 1.0 (insufficient data)."""
    learner = PatternLearner()
    assert learner.seasonality_factor(BASE_TS) == 1.0


def test_seasonality_factor_one_for_unobserved_hour():
    """An hour with no observations of its own returns 1.0 even if others exist."""
    learner = PatternLearner()
    observed_ts = BASE_TS
    other_ts = BASE_TS + ONE_HOUR
    assert time.localtime(observed_ts).tm_hour != time.localtime(other_ts).tm_hour

    for i in range(20):
        learner.observe(observed_ts + i, 75.0)

    # The other hour has an empty bucket → neutral factor.
    assert learner.seasonality_factor(other_ts) == 1.0


def test_seasonality_factor_clamped_to_sane_range():
    """An extreme hot hour vs near-zero baseline is clamped to the [0.25, 4.0] band."""
    learner = PatternLearner()
    # One very hot hour (1000) plus four cold hours (0.0). Across the five equally-
    # sampled buckets the overall mean is 1000/5 = 200, so the raw hot ratio is
    # 1000/200 = 5.0 (> the 4.0 ceiling) and the raw cold ratio is 0.0 (< the 0.25
    # floor) — forcing BOTH clamps to engage.
    hot_ts = BASE_TS
    cold_hours = [BASE_TS + ONE_HOUR * (k + 1) for k in range(4)]
    # All five hours must fall in distinct local-hour buckets.
    all_hours = [time.localtime(hot_ts).tm_hour] + [
        time.localtime(c).tm_hour for c in cold_hours
    ]
    assert len(set(all_hours)) == len(all_hours)

    for i in range(50):
        learner.observe(hot_ts + i, 1000.0)
        for cold_ts in cold_hours:
            learner.observe(cold_ts + i, 0.0)

    # Raw hot ratio (≈ 1000 / 200 = 5.0) exceeds 4.0 → clamps DOWN to the ceiling;
    # raw cold ratio (≈ 0.0) is below 0.25 → clamps UP to the floor.
    assert learner.seasonality_factor(hot_ts) == 4.0
    assert learner.seasonality_factor(cold_hours[0]) == 0.25


def test_hourly_profile_returns_per_hour_means():
    """hourly_profile reports the mean per observed hour and omits unobserved ones."""
    learner = PatternLearner()
    hour_a_ts = BASE_TS
    hour_b_ts = BASE_TS + ONE_HOUR
    ha = time.localtime(hour_a_ts).tm_hour
    hb = time.localtime(hour_b_ts).tm_hour
    assert ha != hb

    # Hour A: mean of [20, 40] = 30. Hour B: constant 80.
    learner.observe(hour_a_ts, 20.0)
    learner.observe(hour_a_ts + 1, 40.0)
    for i in range(5):
        learner.observe(hour_b_ts + i, 80.0)

    profile = learner.hourly_profile()

    assert profile[ha] == pytest.approx(30.0)
    assert profile[hb] == pytest.approx(80.0)
    # Only the two observed hours appear.
    assert set(profile.keys()) == {ha, hb}


def test_observe_ignores_non_finite_values():
    """A NaN/inf observation is dropped and does not corrupt the learned mean."""
    learner = PatternLearner()
    ts = BASE_TS
    hour = time.localtime(ts).tm_hour

    learner.observe(ts, 50.0)
    learner.observe(ts + 1, float("nan"))
    learner.observe(ts + 2, float("inf"))
    learner.observe(ts + 3, 50.0)

    # Only the two finite 50.0 values count → clean mean of 50.0.
    assert learner.hourly_profile()[hour] == pytest.approx(50.0)

"""Unit tests for the ConfidenceCalibrator (C9, feature area C).

Pin the calibration contract:

* on a synthetic **miscalibrated** dataset (distinct raw levels whose empirical
  positive-rate differs from the raw score, but is monotonic in it), fitting **isotonic**
  regression **improves the Brier score** (calibrated < raw) and the fitted transform is
  **monotonic non-decreasing** in the raw score;
* with fewer than ``calibration_min_samples`` samples, or a single outcome class, the
  calibrator stays **unfitted** and ``transform`` is the **identity** — no crash — while
  ``stats`` still reports a safe, correctly-shaped payload;
* all transformed outputs land in ``[0, 1]`` (inputs are clamped too);
* the 10-bin reliability diagram always has the right shape;
* ``record_outcome`` learns from the **raw** (pre-calibration) score and labels the true
  root cause positive.
"""

import pytest

from src.analysis.calibration import ConfidenceCalibrator
from src.config import Settings
from src.models import LogLevel, RootCause

#: Distinct raw levels with a KNOWN empirical positive rate that (a) differs from the raw
#: score — so the raw scores are miscalibrated — but (b) is monotonic increasing in raw, so
#: isotonic can fit it without pooling and drives the Brier score down.
_MISCALIBRATED_GROUPS: list[tuple[float, float]] = [
    (0.10, 0.00),
    (0.30, 0.10),
    (0.50, 0.20),
    (0.70, 0.60),
    (0.90, 1.00),
]


def _settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def _feed_miscalibrated(calibrator: ConfidenceCalibrator, per_group: int = 20) -> None:
    """Record ``per_group`` samples at each miscalibrated raw level (both classes present)."""
    for raw_level, positive_rate in _MISCALIBRATED_GROUPS:
        positives = round(per_group * positive_rate)
        for i in range(per_group):
            calibrator.record(raw_level, i < positives)


def _rc(event_id: str, confidence: float, raw_confidence: float | None) -> RootCause:
    return RootCause(
        event_id=event_id,
        service="api-gateway",
        level=LogLevel.CRITICAL,
        message="boom",
        confidence=confidence,
        raw_confidence=raw_confidence,
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _assert_bins_shape(bins) -> None:
    """The reliability diagram is always the full, stable 10-element ladder."""
    assert isinstance(bins, list)
    assert len(bins) == 10
    for b in bins:
        assert set(b) == {
            "bin_lower",
            "bin_upper",
            "mean_predicted",
            "observed_freq",
            "count",
        }
        assert 0.0 <= b["bin_lower"] < b["bin_upper"] <= 1.0
        assert isinstance(b["count"], int) and b["count"] >= 0
        if b["count"] == 0:
            assert b["mean_predicted"] is None
            assert b["observed_freq"] is None
        else:
            assert 0.0 <= b["mean_predicted"] <= 1.0
            assert 0.0 <= b["observed_freq"] <= 1.0


def _is_monotonic_non_decreasing(calibrator: ConfidenceCalibrator, steps: int = 50) -> bool:
    ys = [calibrator.transform(i / steps) for i in range(steps + 1)]
    assert all(0.0 <= y <= 1.0 for y in ys)
    return all(b >= a - 1e-9 for a, b in zip(ys, ys[1:]))


# --- Isotonic: improves Brier + monotonic ----------------------------------------


def test_isotonic_improves_brier_and_is_monotonic():
    cal = ConfidenceCalibrator(
        _settings(calibration_method="isotonic", calibration_min_samples=10)
    )
    _feed_miscalibrated(cal)

    assert cal.fit() is True
    assert cal.fitted is True

    stats = cal.stats()
    assert stats["method"] == "isotonic"
    assert stats["fitted"] is True
    assert isinstance(stats["brier_raw"], float)
    assert isinstance(stats["brier_calibrated"], float)
    # Calibration reduces the Brier score on the miscalibrated data.
    assert stats["brier_calibrated"] < stats["brier_raw"]
    # The isotonic transform is monotonic non-decreasing in the raw score.
    assert _is_monotonic_non_decreasing(cal)
    _assert_bins_shape(stats["reliability_bins"])


def test_platt_is_monotonic_and_bounded():
    cal = ConfidenceCalibrator(
        _settings(calibration_method="platt", calibration_min_samples=10)
    )
    _feed_miscalibrated(cal)

    assert cal.fit() is True
    stats = cal.stats()
    assert stats["method"] == "platt"
    assert stats["fitted"] is True
    # Logistic (Platt) calibration is also monotonic non-decreasing and bounded.
    assert _is_monotonic_non_decreasing(cal)
    assert isinstance(stats["brier_raw"], float)
    assert isinstance(stats["brier_calibrated"], float)


def test_unknown_method_falls_back_to_isotonic():
    cal = ConfidenceCalibrator(
        _settings(calibration_method="bogus", calibration_min_samples=10)
    )
    _feed_miscalibrated(cal)

    assert cal.fit() is True
    # A malformed method degrades to isotonic rather than raising.
    assert cal.stats()["method"] == "isotonic"
    assert _is_monotonic_non_decreasing(cal)


# --- Identity fallback (sparse / single-class) -----------------------------------


def test_insufficient_samples_stays_identity():
    cal = ConfidenceCalibrator(_settings(calibration_min_samples=10))
    # Both classes present, but fewer than the minimum -> cannot fit.
    cal.record(0.9, True)
    cal.record(0.1, False)

    assert cal.fit() is False
    assert cal.fitted is False
    # transform is the identity while unfitted.
    for x in (0.0, 0.2, 0.5, 0.8, 1.0):
        assert cal.transform(x) == pytest.approx(x)

    stats = cal.stats()
    assert stats["fitted"] is False
    assert stats["n_samples"] == 2
    assert stats["brier_calibrated"] is None  # unfitted
    assert isinstance(stats["brier_raw"], float)  # both classes -> computable
    _assert_bins_shape(stats["reliability_bins"])


def test_single_class_stays_identity():
    cal = ConfidenceCalibrator(_settings(calibration_min_samples=3))
    # Plenty of samples, but only one outcome class -> a monotonic map can't be learned.
    for _ in range(10):
        cal.record(0.7, True)

    assert cal.fit() is False
    assert cal.fitted is False
    assert cal.transform(0.42) == pytest.approx(0.42)

    stats = cal.stats()
    assert stats["fitted"] is False
    # Single class -> Brier is not computable (guarded) -> None, no crash.
    assert stats["brier_raw"] is None
    assert stats["brier_calibrated"] is None
    _assert_bins_shape(stats["reliability_bins"])


def test_empty_calibrator_stats_are_safe():
    cal = ConfidenceCalibrator(_settings())
    stats = cal.stats()

    assert stats["n_samples"] == 0
    assert stats["fitted"] is False
    assert stats["brier_raw"] is None
    assert stats["brier_calibrated"] is None
    assert stats["reliability_bins"] == []

    # transform is the identity and clamps out-of-range inputs into [0, 1].
    assert cal.transform(0.5) == pytest.approx(0.5)
    assert cal.transform(1.5) == pytest.approx(1.0)
    assert cal.transform(-0.2) == pytest.approx(0.0)


# --- Recording outcomes ----------------------------------------------------------


def test_record_outcome_labels_true_root_positive():
    cal = ConfidenceCalibrator(_settings())
    root_causes = [
        _rc("r", confidence=0.9, raw_confidence=0.9),
        _rc("a", confidence=0.5, raw_confidence=0.5),
        _rc("b", confidence=0.3, raw_confidence=0.3),
    ]
    cal.record_outcome(root_causes, "r")

    # One sample per candidate; only the true root is the positive.
    assert cal._raw == [0.9, 0.5, 0.3]
    assert cal._labels == [1, 0, 0]


def test_record_outcome_learns_from_raw_not_calibrated_confidence():
    cal = ConfidenceCalibrator(_settings())
    # A report whose displayed confidence was already calibrated away from the raw score.
    cal.record_outcome([_rc("r", confidence=0.2, raw_confidence=0.8)], "r")
    # The calibrator must learn from the PRE-calibration raw value (0.8), not 0.2.
    assert cal._raw == [0.8]
    assert cal._labels == [1]


def test_record_outcome_falls_back_to_confidence_when_raw_absent():
    cal = ConfidenceCalibrator(_settings())
    # A pre-C9 report carries no raw_confidence (None) -> fall back to confidence.
    cal.record_outcome([_rc("r", confidence=0.6, raw_confidence=None)], "other")
    assert cal._raw == [0.6]
    assert cal._labels == [0]


def test_fit_then_transform_all_outputs_in_unit_interval():
    cal = ConfidenceCalibrator(_settings(calibration_min_samples=10))
    _feed_miscalibrated(cal)
    cal.fit()

    for i in range(21):
        y = cal.transform(i / 20)
        assert 0.0 <= y <= 1.0

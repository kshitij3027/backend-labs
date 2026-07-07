"""Unit tests for the PatternLearner (baselines, boost curve, anomaly/new flags).

assess() judges a correlation against the baseline BEFORE that observation
(record() folds it in afterwards): boost = min(0.15, 0.03*ln(1+count)), so a
first sighting earns exactly 0.0, recurrence grows the boost log-like, and it
caps at 0.15. An established pattern (count >= 5) is anomalous when its new
strength deviates more than 2 sigma from the learned mean; is_new marks the first
sighting of an already-strong (>= 0.8) relationship. Everything runs in memory:
store=None (or a broken store) means zero-boost degradation, never an exception.
"""

import math

import pytest

from src.config import Settings
from src.models import Correlation, CorrelationType, EventRef, SourceType
from src.patterns import BOOST_CAP, PatternLearner, pattern_endpoints, pattern_key

EPOCH = 1000.0


def ref(source: SourceType, ts: float) -> EventRef:
    return EventRef(
        id=f"{source.value}-{ts}",
        source=source,
        service=source.value,
        message=f"{source.value} event",
        timestamp=ts,
    )


def fab_corr(
    ctype: CorrelationType = CorrelationType.TEMPORAL,
    strength: float = 0.9,
    source_a: SourceType = SourceType.WEB,
    source_b: SourceType = SourceType.DATABASE,
    details: dict | None = None,
) -> Correlation:
    return Correlation(
        id="corr-fab",
        detected_at=EPOCH,
        correlation_type=ctype,
        event_a=ref(source_a, EPOCH - 2.0),
        event_b=ref(source_b, EPOCH),
        strength=strength,
        confidence=0.5,
        details=details or {},
    )


def make_learner() -> PatternLearner:
    return PatternLearner(Settings(_env_file=None), store=None)


def seed(learner: PatternLearner, corr: Correlation, times: int) -> None:
    """Record ``corr`` ``times`` times (one batch — count semantics identical)."""
    learner.record([corr] * times, now=EPOCH)


# --- Boost curve -------------------------------------------------------------------
def test_first_sighting_gets_zero_boost():
    assessment = make_learner().assess(fab_corr(strength=0.5), now=EPOCH)
    assert assessment.count == 0
    assert assessment.boost == 0.0
    assert assessment.avg_strength == 0.0
    assert not assessment.is_anomalous


def test_boost_grows_log_like_with_recurrence():
    learner = make_learner()
    corr = fab_corr(strength=0.5)
    seed(learner, corr, 3)
    boost_3 = learner.assess(corr, now=EPOCH).boost
    seed(learner, corr, 7)  # 10 total observations now
    boost_10 = learner.assess(corr, now=EPOCH).boost

    assert 0.0 < boost_3 < boost_10 < BOOST_CAP
    assert boost_3 == pytest.approx(0.03 * math.log(4))  # ln(1 + 3)
    assert boost_10 == pytest.approx(0.03 * math.log(11))  # ln(1 + 10)


def test_boost_caps_at_point_15_for_huge_counts():
    learner = make_learner()
    corr = fab_corr(strength=0.5)
    seed(learner, corr, 10_000)  # uncapped formula would give ~0.276
    assessment = learner.assess(corr, now=EPOCH)
    assert assessment.count == 10_000
    assert assessment.boost == BOOST_CAP


def test_assess_is_against_prior_count_baseline():
    learner = make_learner()
    corr = fab_corr(strength=0.8)
    assert learner.assess(corr, now=EPOCH).count == 0  # nothing recorded yet

    learner.record([corr], now=EPOCH)

    assessment = learner.assess(corr, now=EPOCH + 2.0)
    assert assessment.count == 1
    assert assessment.avg_strength == pytest.approx(0.8)
    assert assessment.boost == pytest.approx(0.03 * math.log(2))


# --- Anomaly detection --------------------------------------------------------------
def test_anomalous_when_beyond_two_sigma_of_flat_baseline():
    learner = make_learner()
    seed(learner, fab_corr(strength=0.5), 6)  # sigma ~ 0 baseline

    assessment = learner.assess(fab_corr(strength=0.95), now=EPOCH)

    assert assessment.count == 6
    assert assessment.avg_strength == pytest.approx(0.5)
    assert assessment.is_anomalous
    assert not assessment.is_new  # anomalous, but the pattern itself is well known


def test_not_anomalous_within_two_sigma():
    learner = make_learner()
    for strength in (0.4, 0.5, 0.6, 0.4, 0.5, 0.6):  # sigma ~ 0.082
        learner.record([fab_corr(strength=strength)], now=EPOCH)

    assessment = learner.assess(fab_corr(strength=0.55), now=EPOCH)
    assert assessment.count == 6
    assert not assessment.is_anomalous  # |0.55 - 0.5| = 0.05 < 2 sigma ~ 0.163


def test_not_anomalous_below_min_count():
    learner = make_learner()
    seed(learner, fab_corr(strength=0.5), 4)  # < 5 observations: too new to judge
    assert not learner.assess(fab_corr(strength=0.95), now=EPOCH).is_anomalous


def test_matching_strength_not_anomalous_on_flat_baseline():
    learner = make_learner()
    seed(learner, fab_corr(strength=0.5), 6)
    assert not learner.assess(fab_corr(strength=0.5), now=EPOCH).is_anomalous


# --- New-pattern flag ---------------------------------------------------------------
def test_is_new_flags_unseen_strong_pattern_only():
    learner = make_learner()
    assert learner.assess(fab_corr(strength=0.9), now=EPOCH).is_new
    assert not learner.assess(fab_corr(strength=0.7), now=EPOCH).is_new  # too weak


def test_is_new_false_once_seen():
    learner = make_learner()
    learner.record([fab_corr(strength=0.9)], now=EPOCH)
    assert not learner.assess(fab_corr(strength=0.9), now=EPOCH).is_new


# --- Degradation --------------------------------------------------------------------
def test_store_none_degrades_to_local_learning():
    learner = PatternLearner(Settings(_env_file=None), store=None)
    corr = fab_corr(strength=0.9)
    assert learner.assess(corr, now=EPOCH).boost == 0.0
    learner.record([corr], now=EPOCH)  # must not raise without a store
    assert learner.assess(corr, now=EPOCH).count == 1


class _BrokenStore:
    """Duck-typed store whose every pattern op raises (worst-case backend)."""

    def load_patterns(self):
        raise ConnectionError("redis down")

    def record_patterns(self, updates):
        raise ConnectionError("redis down")


def test_broken_store_degrades_to_zero_boost_without_raising():
    learner = PatternLearner(Settings(_env_file=None), store=_BrokenStore())
    corr = fab_corr(strength=0.9)

    assessment = learner.assess(corr, now=EPOCH)  # hydration failure -> empty dict
    assert assessment.count == 0
    assert assessment.boost == 0.0

    learner.record([corr], now=EPOCH)  # mirror failure swallowed
    assert learner.assess(corr, now=EPOCH).count == 1  # local dict still learned


# --- Endpoint normalization ---------------------------------------------------------
def test_event_pair_endpoints_are_sorted_source_values():
    corr = fab_corr(source_a=SourceType.WEB, source_b=SourceType.DATABASE)
    assert pattern_endpoints(corr) == ("database", "web")
    assert pattern_key(corr) == ("temporal", "database", "web")


def test_metric_endpoints_are_sorted_metric_names():
    corr = fab_corr(
        ctype=CorrelationType.METRIC,
        details={"metric_a": "web.error_rate", "metric_b": "db.pool_utilization"},
    )
    assert pattern_endpoints(corr) == ("db.pool_utilization", "web.error_rate")
    assert pattern_key(corr) == (
        "metric_based",
        "db.pool_utilization",
        "web.error_rate",
    )


def test_metric_without_series_names_falls_back_to_sources():
    corr = fab_corr(ctype=CorrelationType.METRIC, details={})
    assert pattern_endpoints(corr) == ("database", "web")


def test_direction_does_not_split_a_pattern():
    learner = make_learner()
    learner.record(
        [fab_corr(source_a=SourceType.WEB, source_b=SourceType.DATABASE)], now=EPOCH
    )
    flipped = fab_corr(source_a=SourceType.DATABASE, source_b=SourceType.WEB)
    assert learner.assess(flipped, now=EPOCH).count == 1

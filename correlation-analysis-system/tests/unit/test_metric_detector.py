"""Unit tests for the MetricDetector (spec area: metric correlation + FDR).

A dict-backed FakeAggregator stands in for the per-second rings so every
scenario is hand-scripted: a planted correlated target pair must survive the
cycle's Benjamini-Hochberg pass with the exact confidence formula
``(1 - p_adj) * min(1, n / 30)``; all-noise, short, and zero-variance rings
must emit nothing; TLCC must report the planted 3 s lag; Jaccard must score
overlapping error bursts; and event refs must anchor to the latest window
event of each series' source (or synthesize ``metric:{series}`` refs).
"""

import numpy as np
import pytest

from src.aggregation import SERIES
from src.config import Settings
from src.engine.base import DetectionContext
from src.engine.metric import (
    JACCARD_PAIRS,
    MI_PAIRS,
    PARAMETRIC_PAIRS,
    TLCC_PAIRS,
    MetricDetector,
)
from src.models import CorrelationType, LogEvent, SourceType

NOW = 1_700_000_000.0

TARGET = {"web.error_rate", "db.pool_utilization"}


class FakeAggregator:
    """Dict-backed stand-in mirroring MetricAggregator's read API."""

    def __init__(self, data=None, presence=None):
        self.data = {
            name: np.asarray(values, dtype=float)
            for name, values in (data or {}).items()
        }
        self.presence = {
            key: np.asarray(values, dtype=float)
            for key, values in (presence or {}).items()
        }

    def series(self, name, n=60):
        return self.data[name][-n:]

    def aligned(self, names, n=60):
        return {name: self.series(name, n) for name in names}

    def error_presence(self, source, n=60):
        arr = self.presence.get(str(source))
        if arr is None:
            return np.zeros(n)
        return arr[-n:]


def mk_event(source: SourceType, ts: float, **kw) -> LogEvent:
    """A hand-built LogEvent with sensible defaults for non-essential fields."""
    return LogEvent(
        id=kw.pop("id", f"{source.value}-{ts}"),
        timestamp=ts,
        source=source,
        service=kw.pop("service", source.value),
        level=kw.pop("level", "INFO"),
        message=kw.pop("message", f"{source.value} event at {ts}"),
        **kw,
    )


def make_detector() -> MetricDetector:
    return MetricDetector(Settings(_env_file=None))


def make_ctx(aggregator, window_events=(), now=NOW) -> DetectionContext:
    return DetectionContext(
        now=now, new_events=[], window_events=list(window_events), aggregator=aggregator
    )


def constant_series(value: float = 0.5) -> dict:
    """Every registered series pinned flat — zero variance, nothing testable."""
    return {name: np.full(60, value) for name in SERIES}


def correlated_target_over_constants(seed: int = 9) -> dict:
    """The web<->db target pair strongly correlated; everything else flat."""
    rng = np.random.default_rng(seed)
    data = constant_series()
    base = rng.normal(0.0, 1.0, 60)
    data["web.error_rate"] = base
    data["db.pool_utilization"] = 0.9 * base + rng.normal(0.0, 0.2, 60)
    return data


# --- Registry sanity ---------------------------------------------------------------
def test_pair_registries_lead_with_targets_and_use_known_series():
    targets = (
        ("web.error_rate", "db.pool_utilization"),
        ("payment.latency_ms_avg", "user.abandonment_count"),
        ("inventory.timeout_count", "checkout.failure_count"),
    )
    assert PARAMETRIC_PAIRS[:3] == targets
    assert TLCC_PAIRS == targets
    assert MI_PAIRS == targets
    known = set(SERIES)
    for pair in PARAMETRIC_PAIRS:
        assert set(pair) <= known  # a typo here would be silently guarded away
    for source_a, source_b in JACCARD_PAIRS:
        assert SourceType(source_a) is not SourceType(source_b)


def test_no_aggregator_yields_nothing():
    assert make_detector().detect(make_ctx(None)) == []


# --- (a) correlated target pair over a realistic BH pool ---------------------------
def test_correlated_target_pair_survives_fdr_with_exact_confidence():
    rng = np.random.default_rng(7)
    # Every registered series carries independent noise so the BH pass runs
    # over a realistic ~19-candidate pool; only the target pair co-moves.
    data = {name: rng.normal(0.0, 1.0, 60) for name in SERIES}
    base = rng.normal(0.0, 1.0, 60)
    data["web.error_rate"] = base
    data["db.pool_utilization"] = 0.9 * base + rng.normal(0.0, 0.2, 60)

    found = make_detector().detect(make_ctx(FakeAggregator(data)))

    hits = [
        corr
        for corr in found
        if {corr.details["metric_a"], corr.details["metric_b"]} == TARGET
        and "p_adj" in corr.details
    ]
    assert hits
    corr = hits[0]
    assert corr.correlation_type is CorrelationType.METRIC
    assert corr.strength >= 0.4
    assert corr.details["p_adj"] < 0.05
    assert corr.details["n"] == 60
    assert corr.details["window_seconds"] == 60
    expected = (1.0 - corr.details["p_adj"]) * min(1.0, corr.details["n"] / 30.0)
    assert corr.confidence == pytest.approx(expected, abs=1e-6)


# --- (b) all-noise cycle stays silent ------------------------------------------------
def test_independent_noise_emits_nothing():
    # Independent shuffles of a uniform grid: pure noise with flat marginals
    # (which keeps the MI denominator maximal). The FDR pass plus the 0.4/0.35
    # strength floors must filter every spurious candidate.
    rng = np.random.default_rng(42)
    grid = np.linspace(0.0, 1.0, 60)
    data = {name: rng.permutation(grid) for name in SERIES}
    assert make_detector().detect(make_ctx(FakeAggregator(data))) == []


# --- (c)/(d) short and zero-variance rings ------------------------------------------
def test_fewer_than_min_samples_emits_nothing():
    rng = np.random.default_rng(15)
    data = {name: rng.normal(0.0, 1.0, 8) for name in SERIES}  # n=8 < 10
    assert make_detector().detect(make_ctx(FakeAggregator(data))) == []


def test_zero_variance_series_emit_nothing():
    assert make_detector().detect(make_ctx(FakeAggregator(constant_series()))) == []


# --- (e) dedupe ----------------------------------------------------------------------
def test_second_cycle_on_same_data_is_deduped():
    detector = make_detector()
    aggregator = FakeAggregator(correlated_target_over_constants())
    assert detector.detect(make_ctx(aggregator, now=NOW))
    # Next cycle, 2 s later: every pair+method key is still within its TTL.
    assert detector.detect(make_ctx(aggregator, now=NOW + 2.0)) == []


# --- (f) TLCC lead-lag ---------------------------------------------------------------
def test_planted_lead_lag_reports_lag_seconds():
    rng = np.random.default_rng(3)
    data = constant_series()
    base = rng.normal(0.0, 1.0, 60)
    data["web.error_rate"] = base
    # db series = web series delayed 3 s (plus tiny noise): at lag 0 the pair
    # is white-noise-uncorrelated, so lagged_xcorr is the ONLY strong signal.
    data["db.pool_utilization"] = (
        np.concatenate([rng.normal(0.0, 1.0, 3), base[:-3]])
        + rng.normal(0.0, 0.05, 60)
    )

    found = make_detector().detect(make_ctx(FakeAggregator(data)))

    tlcc = [corr for corr in found if corr.details["method"] == "lagged_xcorr"]
    assert len(tlcc) == 1
    corr = tlcc[0]
    assert {corr.details["metric_a"], corr.details["metric_b"]} == TARGET
    assert corr.details["lag_seconds"] == 3
    assert corr.strength > 0.9
    assert corr.details["p_adj"] < 0.05


# --- (g) Jaccard on error-presence ---------------------------------------------------
def test_jaccard_scores_overlapping_error_bursts():
    db = np.zeros(60)
    db[20:32] = 1.0  # database errors during seconds 20..31
    web = np.zeros(60)
    web[22:34] = 1.0  # web errors during seconds 22..33
    aggregator = FakeAggregator(
        constant_series(), presence={"database": db, "web": web}
    )

    found = make_detector().detect(make_ctx(aggregator))

    jac = [corr for corr in found if corr.details["method"] == "jaccard"]
    assert len(jac) == 1
    corr = jac[0]
    # intersection 22..31 = 10 seconds, union 20..33 = 14 seconds.
    assert {corr.details["metric_a"], corr.details["metric_b"]} == {
        "error_presence.database",
        "error_presence.web",
    }
    assert corr.strength == pytest.approx(10 / 14, abs=1e-9)
    assert 0.5 <= corr.strength <= 1.0
    assert corr.details["j"] == pytest.approx(round(10 / 14, 4), abs=1e-9)
    assert corr.details["n"] == 14
    assert corr.confidence == pytest.approx(14 / 15, abs=1e-9)
    assert {corr.event_a.source, corr.event_b.source} == {
        SourceType.DATABASE,
        SourceType.WEB,
    }


# --- (h) event refs ------------------------------------------------------------------
def test_event_refs_use_latest_window_event_per_source():
    window = [
        mk_event(SourceType.WEB, NOW - 30.0, id="web-old"),
        mk_event(SourceType.DATABASE, NOW - 2.0, id="db-1"),
        mk_event(SourceType.WEB, NOW - 1.0, id="web-new"),
    ]
    aggregator = FakeAggregator(correlated_target_over_constants())

    found = make_detector().detect(make_ctx(aggregator, window_events=window))

    assert found
    corr = found[0]  # first emission is the target pair (metric_a = web series)
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_a.id == "web-new"  # the LATEST web event, not the old one
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.event_b.id == "db-1"
    assert corr.event_a.timestamp <= NOW
    assert corr.event_b.timestamp <= NOW


def test_event_refs_synthesized_when_window_is_empty():
    aggregator = FakeAggregator(correlated_target_over_constants())

    found = make_detector().detect(make_ctx(aggregator, window_events=[]))

    assert found
    corr = found[0]
    assert corr.event_a.id == "metric:web.error_rate"
    assert corr.event_b.id == "metric:db.pool_utilization"
    assert corr.event_a.id.startswith("metric:")
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.event_a.service == "nginx"
    assert corr.event_b.service == "postgresql"
    assert corr.event_a.timestamp == NOW  # stamped at the cycle clock — never future
    assert corr.event_b.timestamp == NOW

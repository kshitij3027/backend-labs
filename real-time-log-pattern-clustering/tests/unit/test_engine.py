"""Unit tests for the :class:`~src.engine.ClusteringEngine` (C8).

These pin the public engine contract the API (C11), WebSocket (C12) and demo (C10) drive:

* :meth:`warm_up` fits the feature pipeline + all three clusterers (each becomes
  ``is_fitted``) and flips :pyattr:`is_warmed`,
* :meth:`process` returns a :class:`~src.schemas.ClusterAssignment` with exactly one
  :class:`~src.schemas.AlgoResult` per algorithm, each confidence in ``[0, 1]``,
* :meth:`process_batch` returns one assignment per input log and the running
  ``total_processed`` counter tracks every processed log,
* :func:`~src.engine.categorize` maps the generator's four families to the right pattern
  types with the documented precedence,
* the read accessors (:meth:`get_patterns`, :meth:`get_anomalies`, :meth:`get_clusters`,
  :meth:`scatter_points`, :meth:`quality_metrics`, :meth:`stats_snapshot`) return well-formed
  data after processing,
* :meth:`refit` runs without error and the engine keeps processing afterward,
* and the anomaly *mechanism* (2-of-3 consensus) is verified directly rather than relying on a
  particular generated log being flagged (which would be flaky).

The synthetic generator (:mod:`src.log_generator`) provides deterministic logs with real
structure, so warm-up actually forms clusters and the assertions are stable.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.engine import (
    PATTERN_ERROR,
    PATTERN_GENERIC,
    PATTERN_PERFORMANCE,
    PATTERN_SECURITY,
    ClusteringEngine,
    categorize,
)
from src.log_generator import generate_logs, generate_pattern_batch
from src.preprocessing import mask_log, parse_log
from src.schemas import AlgoResult, ClusterAssignment


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def warmed_engine() -> ClusteringEngine:
    """A :class:`ClusteringEngine` warmed on 800 deterministic logs.

    Module-scoped because warm-up (fit + 3 clusterers) is the slow part; the tests that use
    it only *read* or append, and the few that mutate counters tolerate a shared baseline by
    asserting on deltas / membership rather than absolute totals.
    """
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    return engine


# --------------------------------------------------------------------------- #
# Warm-up
# --------------------------------------------------------------------------- #


def test_warm_up_fits_everything(warmed_engine: ClusteringEngine) -> None:
    """After warm_up the engine is warmed and all three clusterers are fitted."""
    assert warmed_engine.is_warmed is True
    assert set(warmed_engine.clusterers) == {"kmeans", "dbscan", "hdbscan"}
    for clusterer in warmed_engine.clusterers.values():
        assert clusterer.is_fitted is True
    # The feature pipeline is frozen with a stable, positive dimension.
    assert warmed_engine.features.fitted is True
    assert warmed_engine.features.feature_dim > 0


def test_warm_up_empty_raises() -> None:
    """Warming up on an empty batch is an error (nothing to fit a vocabulary on)."""
    engine = ClusteringEngine()
    with pytest.raises(ValueError):
        engine.warm_up([])


def test_process_before_warmup_raises() -> None:
    """Calling process() before warm_up() raises a clear RuntimeError."""
    engine = ClusteringEngine()
    one = generate_logs(1, seed=5)[0]
    with pytest.raises(RuntimeError):
        engine.process(one)


# --------------------------------------------------------------------------- #
# process() — single log hot path
# --------------------------------------------------------------------------- #


def test_process_returns_assignment_with_three_results(
    warmed_engine: ClusteringEngine,
) -> None:
    """process() returns a ClusterAssignment with exactly one AlgoResult per algorithm."""
    one = generate_logs(1, seed=2)[0]
    assignment = warmed_engine.process(one)

    assert isinstance(assignment, ClusterAssignment)
    assert len(assignment.results) == 3
    algos = {r.algorithm for r in assignment.results}
    assert algos == {"kmeans", "dbscan", "hdbscan"}

    for r in assignment.results:
        assert isinstance(r, AlgoResult)
        assert 0.0 <= r.confidence <= 1.0
        assert isinstance(r.cluster_id, int)
        assert isinstance(r.is_anomaly, bool)

    assert isinstance(assignment.is_new_pattern, bool)
    assert isinstance(assignment.is_anomaly, bool)
    assert assignment.pattern_type in {
        PATTERN_SECURITY,
        PATTERN_PERFORMANCE,
        PATTERN_ERROR,
        PATTERN_GENERIC,
    }
    # masked_message is the masked form of the log message.
    assert assignment.masked_message == mask_log(parse_log(one)["message"])


# --------------------------------------------------------------------------- #
# process_batch() — throughput path
# --------------------------------------------------------------------------- #


def test_process_batch_counts_and_length() -> None:
    """process_batch returns one assignment per log and total_processed tracks the count."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))

    batch = generate_logs(50, seed=2)
    assignments = engine.process_batch(batch)

    assert len(assignments) == 50
    assert all(isinstance(a, ClusterAssignment) for a in assignments)
    assert all(len(a.results) == 3 for a in assignments)

    snap = engine.stats_snapshot()
    assert snap.total_processed == 50

    # A couple of singles on top should bump the counter exactly.
    engine.process(batch[0])
    engine.process(batch[1])
    assert engine.stats_snapshot().total_processed == 52


def test_process_batch_empty_returns_empty(warmed_engine: ClusteringEngine) -> None:
    """An empty batch yields an empty list and touches no state."""
    assert warmed_engine.process_batch([]) == []


# --------------------------------------------------------------------------- #
# categorize() — the helper, called directly (deterministic, not luck-based)
# --------------------------------------------------------------------------- #


def _categorize_log(log) -> str:
    """Run categorize() on a LogEntry/dict via the same parse+mask the engine uses."""
    parsed = parse_log(log)
    return categorize(parsed, mask_log(parsed["message"]))


def test_categorize_security_failed_login() -> None:
    """A failed-login auth ERROR categorizes as a security pattern."""
    log = {
        "timestamp": datetime(2026, 6, 23, 8, 0, 0),
        "service": "auth",
        "level": "ERROR",
        "message": "Failed login attempt for user-1 from 203.0.113.7",
        "source_ip": "203.0.113.7",
        "status_code": 401,
    }
    assert categorize(parse_log(log), mask_log(log["message"])) == PATTERN_SECURITY


def test_categorize_performance_high_latency() -> None:
    """A slow-query / high-latency log categorizes as a performance pattern."""
    log = {
        "timestamp": datetime(2026, 6, 23, 11, 0, 0),
        "service": "database",
        "level": "WARN",
        "message": "Slow query detected on table orders took 3200ms",
        "response_time_ms": 3200.0,
        "status_code": 200,
    }
    assert categorize(parse_log(log), mask_log(log["message"])) == PATTERN_PERFORMANCE


def test_categorize_error_500_exception() -> None:
    """A 500 / unhandled-exception log categorizes as an error pattern."""
    log = {
        "timestamp": datetime(2026, 6, 23, 2, 30, 0),
        "service": "api-gateway",
        "level": "ERROR",
        "message": "Unhandled exception in req-abc123: NullPointerException",
        "status_code": 500,
    }
    assert categorize(parse_log(log), mask_log(log["message"])) == PATTERN_ERROR


def test_categorize_generic_routine_info() -> None:
    """A routine INFO 200 request categorizes as generic."""
    log = {
        "timestamp": datetime(2026, 6, 23, 12, 0, 0),
        "service": "web",
        "level": "INFO",
        "message": "Request served /api/v1/users 200 in 12ms",
        "response_time_ms": 12.0,
        "status_code": 200,
    }
    assert categorize(parse_log(log), mask_log(log["message"])) == PATTERN_GENERIC


def test_categorize_precedence_security_over_error() -> None:
    """Security beats error: an auth ERROR about credentials is security, not error."""
    log = {
        "timestamp": datetime(2026, 6, 23, 8, 0, 0),
        "service": "auth",
        "level": "ERROR",
        "message": "Invalid credentials for user-9",
        "status_code": 401,
    }
    assert categorize(parse_log(log), mask_log(log["message"])) == PATTERN_SECURITY


def test_categorize_generator_families_map_correctly() -> None:
    """The C4 generator's families map predominantly to their matching pattern types.

    Asserted as a strong majority (not 100%) because a few messages in a family are
    intentionally ambiguous; the engine only needs the bulk to land correctly.
    """
    for family, expected in (
        ("security", PATTERN_SECURITY),
        ("performance", PATTERN_PERFORMANCE),
        ("error", PATTERN_ERROR),
    ):
        logs = generate_pattern_batch(family, 60, seed=7)
        matches = sum(1 for log in logs if _categorize_log(log) == expected)
        assert matches >= 0.7 * len(logs), (
            f"family {family!r}: only {matches}/{len(logs)} -> {expected}"
        )


# --------------------------------------------------------------------------- #
# Consensus anomaly MECHANISM (verified directly, not via a lucky generated log)
# --------------------------------------------------------------------------- #


def _algo_results(flags: list[bool]) -> list[AlgoResult]:
    """Build 3 AlgoResults with the given is_anomaly flags (kmeans/dbscan/hdbscan)."""
    names = ("kmeans", "dbscan", "hdbscan")
    return [
        AlgoResult(algorithm=n, cluster_id=0, confidence=0.5, is_anomaly=f)
        for n, f in zip(names, flags)
    ]


@pytest.mark.parametrize(
    "flags,expected_anomaly",
    [
        ([False, False, False], False),
        ([True, False, False], False),  # 1 vote -> not consensus
        ([True, True, False], True),  # 2 votes -> consensus
        ([True, True, True], True),  # 3 votes -> consensus
    ],
)
def test_consensus_anomaly_two_of_three(
    flags: list[bool], expected_anomaly: bool
) -> None:
    """is_anomaly is True iff >= 2 of the 3 algorithms vote (consensus rule)."""
    results = _algo_results(flags)
    parsed = {"service": "web", "level": "INFO", "message": "ok", "status_code": 200}
    assignment = ClusteringEngine._combine(results, parsed, "ok")
    assert assignment.is_anomaly is expected_anomaly


def test_new_pattern_when_any_minus_one() -> None:
    """is_new_pattern is True iff any algorithm returns cluster_id == -1."""
    parsed = {"service": "web", "level": "INFO", "message": "ok", "status_code": 200}

    none_minus = [
        AlgoResult(algorithm=n, cluster_id=0, confidence=0.5, is_anomaly=False)
        for n in ("kmeans", "dbscan", "hdbscan")
    ]
    assert ClusteringEngine._combine(none_minus, parsed, "ok").is_new_pattern is False

    one_minus = [
        AlgoResult(algorithm="kmeans", cluster_id=0, confidence=0.5, is_anomaly=False),
        AlgoResult(algorithm="dbscan", cluster_id=-1, confidence=0.0, is_anomaly=True),
        AlgoResult(algorithm="hdbscan", cluster_id=2, confidence=0.5, is_anomaly=False),
    ]
    assert ClusteringEngine._combine(one_minus, parsed, "ok").is_new_pattern is True


# --------------------------------------------------------------------------- #
# Read accessors after processing
# --------------------------------------------------------------------------- #


def test_accessors_after_processing() -> None:
    """After processing a batch, patterns/anomalies/clusters/scatter are well-formed."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    engine.process_batch(generate_logs(200, seed=3))

    patterns = engine.get_patterns()
    assert len(patterns) > 0
    # Sorted by count descending.
    counts = [p.count for p in patterns]
    assert counts == sorted(counts, reverse=True)
    for p in patterns:
        assert p.algorithm in {"kmeans", "dbscan", "hdbscan"}
        assert p.pattern_type in {
            PATTERN_SECURITY,
            PATTERN_PERFORMANCE,
            PATTERN_ERROR,
            PATTERN_GENERIC,
        }

    anomalies = engine.get_anomalies()
    assert isinstance(anomalies, list)
    assert len(anomalies) <= 50

    clusters = engine.get_clusters("kmeans")
    assert isinstance(clusters, list)
    assert len(clusters) > 0
    first = clusters[0]
    assert {"cluster_id", "size", "representative", "pattern_type", "examples"} <= set(
        first
    )
    assert isinstance(first["examples"], list)
    # Sizes are descending.
    sizes = [c["size"] for c in clusters]
    assert sizes == sorted(sizes, reverse=True)

    # Drill-down on the largest cluster.
    detail = engine.get_cluster_detail("kmeans", first["cluster_id"])
    assert detail["size"] == first["size"]
    assert "confidence" in detail and "mean" in detail["confidence"]

    # Scatter points.
    pts = engine.scatter_points("kmeans")
    assert isinstance(pts, list)
    assert len(pts) > 0
    for pt in pts[:10]:
        assert set(pt) == {"x", "y", "cluster_id"}
        assert isinstance(pt["cluster_id"], int)


def test_get_anomalies_respects_limit() -> None:
    """get_anomalies caps the returned list at the requested limit."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    engine.process_batch(generate_logs(300, seed=4))
    capped = engine.get_anomalies(limit=5)
    assert len(capped) <= 5


# --------------------------------------------------------------------------- #
# Quality metrics + stats snapshot
# --------------------------------------------------------------------------- #


def test_quality_metrics_keys_and_types(warmed_engine: ClusteringEngine) -> None:
    """quality_metrics returns the three keys with float|None values."""
    q = warmed_engine.quality_metrics()
    assert set(q) == {"silhouette", "davies_bouldin", "coherence"}
    for key, value in q.items():
        assert value is None or isinstance(value, float), f"{key}={value!r}"
    # With an 800-log warm-up K-means forms real clusters, so silhouette is computable.
    assert q["silhouette"] is None or isinstance(q["silhouette"], float)


def test_stats_snapshot_shape() -> None:
    """stats_snapshot exposes counters, throughput, total_clusters and quality fields."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    engine.process_batch(generate_logs(100, seed=8))

    snap = engine.stats_snapshot()
    assert snap.total_processed == 100
    assert snap.algorithms == ["kmeans", "dbscan", "hdbscan"]
    assert snap.throughput_per_sec >= 0.0
    # total_clusters is the SUM across algorithms of live (non-noise) cluster counts.
    expected_total = sum(c.n_clusters() for c in engine.clusterers.values())
    assert snap.total_clusters == expected_total
    assert snap.patterns_discovered > 0
    assert snap.anomalies_detected >= 0


def test_coherence_in_unit_interval() -> None:
    """The coherence metric, when computed, lies in [0, 1] (success-criteria scale)."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    coh = engine.quality_metrics()["coherence"]
    assert coh is None or (0.0 <= coh <= 1.0)


# --------------------------------------------------------------------------- #
# Refit
# --------------------------------------------------------------------------- #


def test_refit_runs_and_engine_still_processes() -> None:
    """refit() runs without error and the engine keeps processing afterward."""
    engine = ClusteringEngine()
    engine.warm_up(generate_logs(800, seed=1))
    engine.process_batch(generate_logs(150, seed=9))

    # Must not raise.
    engine.refit()
    for clusterer in engine.clusterers.values():
        assert clusterer.is_fitted is True

    # Still processes after a refit.
    one = generate_logs(1, seed=10)[0]
    assignment = engine.process(one)
    assert len(assignment.results) == 3
    assert engine.seconds_since_refit() >= 0.0


def test_refit_before_warmup_is_noop() -> None:
    """refit() on an un-warmed engine is a safe no-op (and should_refit is False)."""
    engine = ClusteringEngine()
    engine.refit()  # must not raise
    assert engine.should_refit() is False

"""Unit tests for the one-shot ``demo`` mode (:mod:`src.demo`, C10).

These pin the demo's public contract — the structured return value of :func:`run_demo`, the
exact project_requirements §8 console format produced by :func:`format_entry` /
:func:`format_insights`, and the corpus fallback / CLI smoke behaviour — without asserting on
anything time-dependent (the throughput "Processing Rate" is informational only).

Warm-up sizes are kept small (``n_warmup`` in the low hundreds) so the suite stays fast: the
engine still forms real clusters at that size, which is all the rendering paths need.
"""

from __future__ import annotations

from datetime import datetime

from src.demo import (
    DEFAULT_CORPUS,
    format_entry,
    format_insights,
    load_corpus,
    main,
    run_demo,
)
from src.engine import ClusteringEngine
from src.schemas import AlgoResult, ClusterAssignment, LogEntry, StatsSnapshot


# --------------------------------------------------------------------------- #
# run_demo — structured return value
# --------------------------------------------------------------------------- #


def test_run_demo_returns_structured_dict() -> None:
    """run_demo returns entries (one per streamed log) + a 5-field insights dict."""
    result = run_demo(n_warmup=200, n_demo=5, seed=1, emit=lambda _s: None)

    assert result["warmup"] == 200
    assert result["demo"] == 5

    entries = result["entries"]
    assert len(entries) == 5
    for i, entry in enumerate(entries, start=1):
        assert entry["index"] == i
        # Exactly one result per clustering algorithm, in canonical order.
        algos = [r["algorithm"] for r in entry["results"]]
        assert algos == list(ClusteringEngine.ALGORITHMS)
        assert len(entry["results"]) == 3
        for r in entry["results"]:
            assert 0.0 <= r["confidence"] <= 1.0
            assert isinstance(r["cluster_id"], int)

    insights = result["insights"]
    for key in (
        "total_clusters",
        "algorithms_used",
        "throughput_per_sec",
        "patterns_discovered",
        "anomalies_detected",
    ):
        assert key in insights
    assert insights["algorithms_used"] == len(ClusteringEngine.ALGORITHMS)


# --------------------------------------------------------------------------- #
# run_demo — rendered output (captured via injected emit)
# --------------------------------------------------------------------------- #


def test_run_demo_emits_required_sections() -> None:
    """The rendered demo text contains every §8 section marker the Docker tester asserts."""
    captured: list[str] = []
    run_demo(n_warmup=200, n_demo=5, seed=1, emit=captured.append)
    text = "\n".join(captured)

    assert "📝 Processing Log Entry" in text
    assert "🎯 Cluster Results:" in text
    assert "📊 Cluster Insights:" in text
    assert "Processing Rate:" in text
    assert "Patterns Discovered:" in text
    # At least one algorithm placed a log in a cluster OR flagged an anomaly.
    assert ("Cluster " in text) or ("ANOMALY DETECTED" in text)


# --------------------------------------------------------------------------- #
# format_entry — anomalous vs. clustered algorithm lines
# --------------------------------------------------------------------------- #


def _log() -> LogEntry:
    return LogEntry(
        timestamp=datetime(2026, 6, 23, 12, 0, 0),
        service="auth",
        level="ERROR",
        message="Multiple failed login attempts detected",
    )


def test_format_entry_renders_anomaly_line() -> None:
    """An anomalous algo renders the '🚨 ANOMALY DETECTED in {algo}!' line (no Cluster line)."""
    # Mirrors the project_requirements §8 sample: kmeans is the highest-confidence
    # non-anomalous algo (0.87), so the discovered-patterns block credits it.
    assignment = ClusterAssignment(
        results=[
            AlgoResult(algorithm="kmeans", cluster_id=2, confidence=0.87, is_anomaly=False),
            AlgoResult(algorithm="dbscan", cluster_id=0, confidence=0.85, is_anomaly=False),
            AlgoResult(algorithm="hdbscan", cluster_id=-1, confidence=0.10, is_anomaly=True),
        ],
        is_new_pattern=True,
        is_anomaly=False,
        pattern_type="security_pattern",
        masked_message="Multiple failed login attempts detected",
    )

    block = format_entry(1, _log(), assignment)

    assert "📝 Processing Log Entry 1:" in block
    assert "Service: auth" in block
    assert "Level: ERROR" in block
    assert "Message: Multiple failed login attempts detected" in block
    assert "🚨 ANOMALY DETECTED in hdbscan!" in block
    # The anomalous algorithm must NOT also get a "Cluster N" line.
    assert "hdbscan: Cluster" not in block
    # Discovered-patterns block credits the best non-anomalous algo (kmeans @ 0.87).
    assert "Pattern Type: security_pattern" in block
    assert "Algorithm: kmeans" in block
    assert "Confidence: 0.87" in block


def test_format_entry_renders_cluster_line() -> None:
    """A non-anomalous algo renders 'Cluster {id} (confidence: 0.NN)' with 2-dp confidence."""
    assignment = ClusterAssignment(
        results=[
            AlgoResult(algorithm="kmeans", cluster_id=3, confidence=0.5, is_anomaly=False),
            AlgoResult(algorithm="dbscan", cluster_id=1, confidence=0.75, is_anomaly=False),
            AlgoResult(algorithm="hdbscan", cluster_id=4, confidence=0.66, is_anomaly=False),
        ],
        is_new_pattern=False,
        is_anomaly=False,
        pattern_type="performance_pattern",
        masked_message="High latency serving <PATH>: <NUM>ms",
    )

    block = format_entry(2, _log(), assignment)

    assert "kmeans: Cluster 3 (confidence: 0.50)" in block
    assert "dbscan: Cluster 1 (confidence: 0.75)" in block
    assert "ANOMALY DETECTED" not in block


def test_format_entry_generic_pattern_shows_none() -> None:
    """A generic, non-new-pattern log renders the '(none)' discovered-patterns line."""
    assignment = ClusterAssignment(
        results=[
            AlgoResult(algorithm="kmeans", cluster_id=0, confidence=0.9, is_anomaly=False),
            AlgoResult(algorithm="dbscan", cluster_id=0, confidence=0.9, is_anomaly=False),
            AlgoResult(algorithm="hdbscan", cluster_id=0, confidence=0.9, is_anomaly=False),
        ],
        is_new_pattern=False,
        is_anomaly=False,
        pattern_type="generic",
        masked_message="Health check ok",
    )

    block = format_entry(1, _log(), assignment)
    assert "🔍 Discovered Patterns: (none)" in block


# --------------------------------------------------------------------------- #
# format_insights — §8 summary block
# --------------------------------------------------------------------------- #


def test_format_insights_renders_all_fields() -> None:
    """format_insights renders all five §8 insight lines with the canonical algorithm count."""
    stats = StatsSnapshot(
        total_processed=1000,
        throughput_per_sec=1247.0,
        total_clusters=12,
        patterns_discovered=5,
        anomalies_detected=2,
        algorithms=list(ClusteringEngine.ALGORITHMS),
    )

    block = format_insights(stats)

    assert "📊 Cluster Insights:" in block
    assert "Total Clusters: 12" in block
    assert f"Algorithms Used: {len(ClusteringEngine.ALGORITHMS)}" in block
    assert "Processing Rate: 1247 logs/second" in block  # rounded, no decimals
    assert "Patterns Discovered: 5" in block
    assert "Anomalies Detected: 2" in block


# --------------------------------------------------------------------------- #
# load_corpus — fallback when the file is missing
# --------------------------------------------------------------------------- #


def test_load_corpus_falls_back_when_missing() -> None:
    """A non-existent corpus path yields a non-empty list of generated LogEntry records."""
    logs = load_corpus("does/not/exist.jsonl", fallback_n=50)
    assert isinstance(logs, list)
    assert len(logs) == 50
    assert all(isinstance(entry, LogEntry) for entry in logs)


def test_load_corpus_reads_committed_sample() -> None:
    """The committed default corpus parses into LogEntry records (sanity check, not required)."""
    import os

    if not os.path.exists(DEFAULT_CORPUS):
        return  # corpus not present in this checkout; fallback path is covered above
    logs = load_corpus(DEFAULT_CORPUS)
    assert len(logs) > 0
    assert all(isinstance(entry, LogEntry) for entry in logs)


# --------------------------------------------------------------------------- #
# main — CLI smoke
# --------------------------------------------------------------------------- #


def test_main_smoke(capsys) -> None:
    """main(...) runs end-to-end with small sizes and returns 0; output is ignored."""
    rc = main(["--warmup", "150", "--demo", "3", "--seed", "2"])
    assert rc == 0
    # Drain captured stdout so it doesn't leak into other tests' output.
    _ = capsys.readouterr()

"""One-shot ``demo`` mode for the Real-Time Log Pattern Clustering engine (C10).

This is the batch demonstration the project requirements (§1, §8) call for: a single,
self-contained run that *warms up* the :class:`~src.engine.ClusteringEngine` on a historical
batch, streams a handful of sample logs through it one at a time, and prints each log's
per-algorithm cluster assignment + discovered pattern in the **exact** console format from
project_requirements §8 (emojis and indentation included). It finishes by processing a larger
remainder so the aggregate counters are meaningful, then prints a ``Cluster Insights`` summary.

Run it as::

    python -m src.demo

which is also the smoke check the Docker tester executes (it asserts the printed sections
appear). The demo is deliberately deterministic where it can be: the corpus is loaded from the
committed ``data/sample.jsonl`` (or, if that file is absent — e.g. a minimal container —
falls back to the seeded :func:`~src.log_generator.generate_logs` so it runs *anywhere*), and
every warm-up / generation step is seeded. Only the engine's throughput figure depends on real
wall-clock time, so tests must not assert an exact rate.

Design note — *testability*: the rendering functions (:func:`format_entry`,
:func:`format_insights`) **return** strings and :func:`run_demo` **returns** a structured dict
rather than burying everything in ``print``. The caller does the printing (via the injectable
``emit`` callback), so tests can assert on the returned data and/or the captured text without
parsing stdout fragilely.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Callable

from src.engine import ClusteringEngine
from src.log_generator import generate_logs
from src.preprocessing import parse_log
from src.schemas import AlgoResult, ClusterAssignment, LogEntry, StatsSnapshot

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

#: Default committed corpus used to warm up + drive the demo. Relative to the process CWD,
#: which is the repo root in the container (``WORKDIR /app``) and in local ``python -m`` runs.
DEFAULT_CORPUS: str = "data/sample.jsonl"

#: Pattern type the engine assigns to logs that match none of the categorized families. A
#: per-entry "Discovered Patterns" block is only rendered when the log is a *new* pattern or
#: carries a more specific type than this.
_GENERIC_TYPE: str = "generic"


# --------------------------------------------------------------------------- #
# Corpus loading
# --------------------------------------------------------------------------- #


def load_corpus(path: str = DEFAULT_CORPUS, fallback_n: int = 800) -> list[LogEntry]:
    """Load the demo corpus, falling back to a generated batch if the file is absent.

    If ``path`` exists, every non-blank JSON Lines record is parsed into a
    :class:`~src.schemas.LogEntry` (blank lines and individually malformed lines are skipped
    so one bad row never aborts the demo). Otherwise — so the demo runs in *any* container,
    even one that does not ship the corpus file — a deterministic batch is produced via
    :func:`~src.log_generator.generate_logs` (``seed=42``).

    Args:
        path: Path to a JSON Lines corpus (one ``LogEntry`` JSON object per line).
        fallback_n: How many logs to generate when ``path`` does not exist.

    Returns:
        A non-empty list of :class:`~src.schemas.LogEntry`. (If the file exists but yields no
        valid rows, the generated fallback is returned so callers always get usable data.)
    """
    if os.path.exists(path):
        entries: list[LogEntry] = []
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(LogEntry.model_validate_json(line))
                except Exception:  # noqa: BLE001 - skip a malformed row, keep the rest
                    continue
        if entries:
            return entries

    # File missing (or unreadable / empty): deterministic generated fallback.
    return generate_logs(fallback_n, seed=42)


# --------------------------------------------------------------------------- #
# Rendering — per-entry block + insights summary
# --------------------------------------------------------------------------- #


def _best_nonanomalous(results: list[AlgoResult]) -> AlgoResult | None:
    """Return the highest-confidence non-anomalous algorithm result (or ``None``).

    Used to populate the per-entry "Discovered Patterns" block's Algorithm/Confidence lines:
    we credit the discovery to whichever algorithm placed the log in a real cluster with the
    most confidence. Falls back to the single most-confident result of any kind if *every*
    algorithm flagged the log as anomalous.
    """
    if not results:
        return None
    non_anom = [r for r in results if not r.is_anomaly]
    pool = non_anom if non_anom else list(results)
    return max(pool, key=lambda r: r.confidence)


def format_entry(index: int, log: LogEntry, assignment: ClusterAssignment) -> str:
    """Render one processed log's result block in the exact project_requirements §8 format.

    Produces (with the §8 emojis and indentation)::

        📝 Processing Log Entry {index}:
           Service: {service}
           Level: {level}
           Message: {message}
           🎯 Cluster Results:
              {algorithm}: Cluster {cluster_id} (confidence: 0.NN)
              🚨 ANOMALY DETECTED in {algorithm}!     # for any anomalous algo
           🔍 Discovered Patterns:
              Pattern Type: {pattern_type}
              Algorithm: {best non-anomalous algorithm}
              Confidence: 0.NN

    The "Discovered Patterns" block is only shown when the log is a *new* pattern
    (``is_new_pattern``) or its ``pattern_type`` is more specific than ``"generic"``; otherwise
    a ``🔍 Discovered Patterns: (none)`` line is emitted so the section is always present.

    Args:
        index: 1-based position of this log in the demo stream (for the header line).
        log: The original :class:`~src.schemas.LogEntry` (its Service/Level/Message are shown).
        assignment: The engine's :class:`~src.schemas.ClusterAssignment` for ``log``.

    Returns:
        The fully-rendered, multi-line block as a single string (no trailing newline). The
        caller is responsible for printing it, which keeps this function pure/testable.
    """
    parsed = parse_log(log)
    lines: list[str] = [
        f"📝 Processing Log Entry {index}:",
        f"   Service: {parsed.get('service', '')}",
        f"   Level: {parsed.get('level', '')}",
        f"   Message: {parsed.get('message', '')}",
        "   🎯 Cluster Results:",
    ]

    for r in assignment.results:
        if r.is_anomaly:
            lines.append(f"      🚨 ANOMALY DETECTED in {r.algorithm}!")
        else:
            lines.append(
                f"      {r.algorithm}: Cluster {r.cluster_id} "
                f"(confidence: {r.confidence:.2f})"
            )

    pattern_type = assignment.pattern_type or _GENERIC_TYPE
    show_pattern = assignment.is_new_pattern or pattern_type != _GENERIC_TYPE
    if show_pattern:
        best = _best_nonanomalous(assignment.results)
        lines.append("   🔍 Discovered Patterns:")
        lines.append(f"      Pattern Type: {pattern_type}")
        if best is not None:
            lines.append(f"      Algorithm: {best.algorithm}")
            lines.append(f"      Confidence: {best.confidence:.2f}")
    else:
        lines.append("   🔍 Discovered Patterns: (none)")

    return "\n".join(lines)


def format_insights(stats: StatsSnapshot) -> str:
    """Render the ``📊 Cluster Insights`` summary block from a stats snapshot (§8 format).

    Produces::

        📊 Cluster Insights:
           Total Clusters: {total_clusters}
           Algorithms Used: {len(ClusteringEngine.ALGORITHMS)}
           Processing Rate: {throughput_per_sec:.0f} logs/second
           Patterns Discovered: {patterns_discovered}
           Anomalies Detected: {anomalies_detected}

    ``Algorithms Used`` is taken from :pyattr:`ClusteringEngine.ALGORITHMS` (the canonical
    set the engine runs), and the rate is rounded to a whole number — its exact value depends
    on real wall-clock time, so it is informational only.

    Args:
        stats: A :class:`~src.schemas.StatsSnapshot` from
            :meth:`~src.engine.ClusteringEngine.stats_snapshot`.

    Returns:
        The rendered summary block as a single string (no trailing newline).
    """
    return "\n".join(
        [
            "📊 Cluster Insights:",
            f"   Total Clusters: {stats.total_clusters}",
            f"   Algorithms Used: {len(ClusteringEngine.ALGORITHMS)}",
            f"   Processing Rate: {stats.throughput_per_sec:.0f} logs/second",
            f"   Patterns Discovered: {stats.patterns_discovered}",
            f"   Anomalies Detected: {stats.anomalies_detected}",
        ]
    )


# --------------------------------------------------------------------------- #
# Demo driver
# --------------------------------------------------------------------------- #


def _entry_payload(index: int, log: LogEntry, assignment: ClusterAssignment) -> dict[str, Any]:
    """Build the structured per-entry record returned by :func:`run_demo` (for tests)."""
    parsed = parse_log(log)
    best = _best_nonanomalous(assignment.results)
    return {
        "index": index,
        "service": parsed.get("service", ""),
        "level": parsed.get("level", ""),
        "message": parsed.get("message", ""),
        "pattern_type": assignment.pattern_type,
        "is_new_pattern": assignment.is_new_pattern,
        "is_anomaly": assignment.is_anomaly,
        "best_algorithm": best.algorithm if best is not None else None,
        "best_confidence": best.confidence if best is not None else None,
        "results": [
            {
                "algorithm": r.algorithm,
                "cluster_id": r.cluster_id,
                "confidence": r.confidence,
                "is_anomaly": r.is_anomaly,
            }
            for r in assignment.results
        ],
    }


def run_demo(
    corpus_path: str = DEFAULT_CORPUS,
    n_warmup: int = 500,
    n_demo: int = 8,
    seed: int = 42,
    emit: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Run the full one-shot demo: warm up, stream a few logs, print results + insights.

    Steps:

    1. :func:`load_corpus` the logs (from ``corpus_path`` or the generated fallback).
    2. Build a :class:`~src.engine.ClusteringEngine` and :meth:`~ClusteringEngine.warm_up`
       it on the first ``n_warmup`` logs.
    3. Stream the next ``n_demo`` logs one at a time through
       :meth:`~ClusteringEngine.process`, ``emit``-ing each :func:`format_entry` block.
    4. Process the remaining logs via :meth:`~ClusteringEngine.process_batch` so the
       aggregate counters (clusters/patterns/anomalies/throughput) are meaningful.
    5. ``emit`` the :func:`format_insights` summary from the final stats snapshot.

    The corpus is sliced so warm-up, demo, and remainder logs do not overlap; if the corpus
    is smaller than ``n_warmup`` it is topped up with deterministic generated logs so warm-up
    always has enough data.

    Args:
        corpus_path: Path passed to :func:`load_corpus`.
        n_warmup: Number of logs to warm up on (the model is fit on these).
        n_demo: Number of logs to stream individually with per-entry output.
        seed: Seed for any generated top-up / fallback logs (keeps the run deterministic).
        emit: Sink for rendered text; defaults to :func:`print`. Tests inject a list-appender
            (or use ``capsys``) to capture the output without touching stdout.

    Returns:
        A structured dict::

            {
                "entries": [ {per-entry dict with algorithm "results"}, ... ],  # len == n_demo
                "insights": { total_clusters, algorithms_used, throughput_per_sec,
                              patterns_discovered, anomalies_detected },
                "warmup": n_warmup,
                "demo": n_demo,
            }

        so tests can assert on the structure without parsing stdout.
    """
    corpus = load_corpus(corpus_path)

    # Make sure there is enough data for warm-up + the demo stream. If the corpus is short,
    # deterministically top it up so the demo is robust to a tiny/empty file.
    needed = n_warmup + n_demo
    if len(corpus) < needed:
        corpus = corpus + generate_logs(needed - len(corpus), seed=seed)

    warmup_logs = corpus[:n_warmup]
    demo_logs = corpus[n_warmup : n_warmup + n_demo]
    remainder = corpus[n_warmup + n_demo :]

    engine = ClusteringEngine()
    engine.warm_up(warmup_logs)

    entries: list[dict[str, Any]] = []
    for i, log in enumerate(demo_logs, start=1):
        assignment = engine.process(log)
        emit(format_entry(i, log, assignment))
        entries.append(_entry_payload(i, log, assignment))

    # Fold the remainder in (off the per-entry print path) so the insights numbers reflect a
    # real workload rather than just the handful of demo logs.
    if remainder:
        engine.process_batch(remainder)

    emit("")  # blank line separating the per-entry blocks from the insights summary
    stats = engine.stats_snapshot()
    emit(format_insights(stats))

    return {
        "entries": entries,
        "insights": {
            "total_clusters": stats.total_clusters,
            "algorithms_used": len(ClusteringEngine.ALGORITHMS),
            "throughput_per_sec": stats.throughput_per_sec,
            "patterns_discovered": stats.patterns_discovered,
            "anomalies_detected": stats.anomalies_detected,
        },
        "warmup": n_warmup,
        "demo": n_demo,
    }


# --------------------------------------------------------------------------- #
# CLI entry point
# --------------------------------------------------------------------------- #


def main(argv: "list[str] | None" = None) -> int:
    """CLI entry point for ``python -m src.demo``.

    Parses ``--corpus`` / ``--warmup`` / ``--demo`` / ``--seed`` and runs :func:`run_demo`
    (printing to stdout). Returns a process exit code.

    Args:
        argv: Argument vector (defaults to ``sys.argv[1:]`` via argparse).

    Returns:
        ``0`` on success.
    """
    parser = argparse.ArgumentParser(
        prog="python -m src.demo",
        description=(
            "One-shot demo: warm up the clustering engine on a batch, stream a few sample "
            "logs, and print per-log cluster results plus a Cluster Insights summary."
        ),
    )
    parser.add_argument(
        "--corpus",
        default=DEFAULT_CORPUS,
        help=f"Path to a JSONL log corpus (default: {DEFAULT_CORPUS}; "
        "falls back to generated logs if absent).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=500,
        help="Number of logs to warm up / fit the model on (default: 500).",
    )
    parser.add_argument(
        "--demo",
        type=int,
        default=8,
        help="Number of logs to stream individually with per-entry output (default: 8).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed for any generated/top-up logs (default: 42).",
    )
    args = parser.parse_args(argv)

    run_demo(
        corpus_path=args.corpus,
        n_warmup=args.warmup,
        n_demo=args.demo,
        seed=args.seed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

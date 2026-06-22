"""Unit tests for the live-metrics aggregator (Commit 10).

Exercises :class:`src.metrics.MetricsAggregator` in isolation — no FastAPI app, no
HTTP, no model. These assert the aggregator's contract directly: the documented
:meth:`~src.metrics.MetricsAggregator.snapshot` shape, how :meth:`record` folds a
classification result into the running tallies (counts, distributions, running
average confidence, the bounded recent-predictions ring buffer), the
service-distribution-only-when-present rule, live throughput, the status mirror,
JSON-serializability, and thread-safety of concurrent ``record`` calls.

The snapshot keys are pinned against ``SNAPSHOT_KEYS`` so a drift in the public
shape (a renamed/removed/added field) fails loudly here rather than silently in the
dashboard or ``GET /metrics``.
"""

from __future__ import annotations

import json
import threading

from src.metrics import RECENT_MAXLEN, MetricsAggregator

# The exact public snapshot contract documented on MetricsAggregator.snapshot().
SNAPSHOT_KEYS = {
    "total_classified",
    "severity_distribution",
    "category_distribution",
    "service_distribution",
    "avg_confidence",
    "throughput_per_sec",
    "recent_predictions",
    "model_status",
    "current_version",
    "uptime_sec",
}

# The keys each recent-prediction ring-buffer entry is contracted to carry.
RECENT_ENTRY_KEYS = {"raw_log", "severity", "category", "confidence", "ts"}


def test_fresh_snapshot_has_all_keys_and_empty_defaults():
    """A brand-new aggregator's snapshot has every key with empty/zero defaults."""
    snap = MetricsAggregator().snapshot()

    assert set(snap) == SNAPSHOT_KEYS, f"unexpected snapshot keys: {sorted(snap)}"
    assert snap["total_classified"] == 0
    # Distributions are empty *dicts* (not None) before anything is recorded.
    assert snap["severity_distribution"] == {}
    assert snap["category_distribution"] == {}
    assert snap["service_distribution"] == {}
    assert snap["avg_confidence"] == 0.0
    assert snap["recent_predictions"] == []
    # Default status is "ready" with no version (the aggregator's __init__ default).
    assert snap["model_status"] == "ready"
    assert snap["current_version"] is None
    # uptime is a non-negative number from the start.
    assert isinstance(snap["uptime_sec"], float)
    assert snap["uptime_sec"] >= 0.0


def test_record_single_updates_counts_and_avg():
    """One record bumps total, populates both distributions, sets avg = its confidence."""
    agg = MetricsAggregator()
    agg.record({"severity": "ERROR", "category": "SYSTEM", "confidence": 0.9})

    snap = agg.snapshot()
    assert snap["total_classified"] == 1
    assert snap["severity_distribution"] == {"ERROR": 1}
    assert snap["category_distribution"] == {"SYSTEM": 1}
    assert snap["avg_confidence"] == 0.9


def test_record_multiple_running_avg_and_counts():
    """Multiple records accumulate counts and compute a correct running average."""
    agg = MetricsAggregator()
    agg.record({"severity": "ERROR", "category": "SYSTEM", "confidence": 0.8})
    agg.record({"severity": "ERROR", "category": "NETWORK", "confidence": 1.0})

    snap = agg.snapshot()
    assert snap["total_classified"] == 2
    # Same severity twice -> count 2; two distinct categories -> 1 each.
    assert snap["severity_distribution"] == {"ERROR": 2}
    assert snap["category_distribution"] == {"SYSTEM": 1, "NETWORK": 1}
    # Running average of 0.8 and 1.0 is exactly 0.9.
    assert snap["avg_confidence"] == 0.9


def test_service_distribution_only_when_service_present():
    """``service_distribution`` counts only results carrying a non-empty ``service``."""
    agg = MetricsAggregator()
    # One result with an explicit service, one without it at all.
    agg.record({"severity": "INFO", "category": "WEB", "confidence": 0.7, "service": "web"})
    agg.record({"severity": "INFO", "category": "WEB", "confidence": 0.7})

    snap = agg.snapshot()
    # Only the service-bearing record contributes to the service distribution.
    assert snap["service_distribution"] == {"web": 1}
    # Both still counted in the overall total.
    assert snap["total_classified"] == 2


def test_service_distribution_ignores_empty_service():
    """A falsy ``service`` (empty string / None) is treated as absent."""
    agg = MetricsAggregator()
    agg.record({"severity": "INFO", "category": "WEB", "confidence": 0.5, "service": ""})
    agg.record({"severity": "INFO", "category": "WEB", "confidence": 0.5, "service": None})

    assert agg.snapshot()["service_distribution"] == {}


def test_recent_predictions_capped_and_newest_last():
    """The recent ring buffer caps at RECENT_MAXLEN, keeps newest last, full entries."""
    agg = MetricsAggregator()
    total = 60  # > RECENT_MAXLEN (50)
    for i in range(total):
        agg.record(
            {"severity": "INFO", "category": "SYSTEM", "confidence": 0.5},
            raw_log=f"log line number {i}",
        )

    snap = agg.snapshot()
    # Total counts ALL records, even though the ring buffer is bounded.
    assert snap["total_classified"] == total

    recent = snap["recent_predictions"]
    assert len(recent) == RECENT_MAXLEN == 50
    # Newest last: the final entry is the most recently recorded line (#59).
    assert recent[-1]["raw_log"] == f"log line number {total - 1}"
    # Oldest retained is #10 (60 records, keep the last 50 -> 10..59).
    assert recent[0]["raw_log"] == f"log line number {total - RECENT_MAXLEN}"
    # Every entry carries the documented keys.
    for entry in recent:
        assert set(entry) == RECENT_ENTRY_KEYS, f"unexpected entry keys: {sorted(entry)}"


def test_throughput_zero_when_fresh_and_positive_after_record():
    """Throughput is ~0 on a fresh aggregator and > 0 right after recording."""
    agg = MetricsAggregator()
    # Nothing recorded yet -> no records in the window -> zero rate.
    assert agg.throughput_per_sec() == 0.0

    for _ in range(5):
        agg.record({"severity": "INFO", "category": "SYSTEM", "confidence": 0.5})
    # Just-recorded events fall inside the rate window, so the rate is positive.
    assert agg.throughput_per_sec() > 0.0


def test_set_status_updates_model_status_and_version():
    """``set_status`` reflects model status, and a later call updates status + version."""
    agg = MetricsAggregator()

    agg.set_status("training")
    snap = agg.snapshot()
    assert snap["model_status"] == "training"
    # Version untouched (we passed no version) -> still the default None.
    assert snap["current_version"] is None

    agg.set_status("ready", "v3")
    snap = agg.snapshot()
    assert snap["model_status"] == "ready"
    assert snap["current_version"] == "v3"


def test_set_status_partial_leaves_other_field_unchanged():
    """Passing only a version keeps the existing status (each arg is independent)."""
    agg = MetricsAggregator()
    agg.set_status("training", "v1")

    # Update only the version; status must remain "training".
    agg.set_status(current_version="v2")
    snap = agg.snapshot()
    assert snap["model_status"] == "training"
    assert snap["current_version"] == "v2"


def test_snapshot_is_json_serializable():
    """A populated snapshot round-trips through json.dumps without error."""
    agg = MetricsAggregator()
    agg.record(
        {"severity": "ERROR", "category": "SYSTEM", "confidence": 0.9, "service": "db"},
        raw_log="Database connection failed",
    )
    agg.set_status("ready", "v1")

    serialized = json.dumps(agg.snapshot())
    # Sanity round-trip: it parses back to an equivalent dict with the same keys.
    reloaded = json.loads(serialized)
    assert set(reloaded) == SNAPSHOT_KEYS
    assert reloaded["total_classified"] == 1


def test_concurrent_records_are_thread_safe():
    """Recording from several threads yields a total equal to the number of records."""
    agg = MetricsAggregator()
    threads_count = 8
    per_thread = 250
    expected = threads_count * per_thread

    def worker() -> None:
        for _ in range(per_thread):
            agg.record({"severity": "INFO", "category": "SYSTEM", "confidence": 0.5})

    threads = [threading.Thread(target=worker) for _ in range(threads_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = agg.snapshot()
    # No lost updates: every record across every thread is counted exactly once.
    assert snap["total_classified"] == expected
    # The single severity/category sums to the same total (no torn counter updates).
    assert snap["severity_distribution"] == {"INFO": expected}
    assert snap["category_distribution"] == {"SYSTEM": expected}

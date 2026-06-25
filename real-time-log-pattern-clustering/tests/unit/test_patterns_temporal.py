"""Unit tests for batch temporal pattern mining (C18, Feature Area A).

These exercise :func:`src.patterns.temporal.mine_temporal_patterns` and
:func:`src.patterns.temporal.hour_histogram` against the *seeded* synthetic corpus, which
plants several recurring temporal patterns (a nightly 02:00 error spike, weekday service
bursts, business-hours performance degradation, ...). The miner must rediscover >= 5 distinct
patterns and, in particular, the 02:00 nightly error window — which we cross-check against the
hour histogram so the assertion is grounded in the data, not just the label.
"""

from __future__ import annotations

from src.log_generator import generate_logs
from src.patterns.temporal import hour_histogram, mine_temporal_patterns

# The documented keys every mined pattern must carry.
_REQUIRED_KEYS = {"pattern_id", "kind", "description", "window", "metric", "count", "services"}


def test_mines_at_least_five_patterns_with_required_keys() -> None:
    """On a representative corpus the miner finds >= 5 patterns, each fully shaped."""
    logs = generate_logs(2500, seed=3)
    patterns = mine_temporal_patterns(logs)

    assert len(patterns) >= 5
    for p in patterns:
        assert _REQUIRED_KEYS.issubset(p.keys())
        assert isinstance(p["services"], list)
        # metric is the severity / sort key — always a real number.
        assert isinstance(p["metric"], (int, float))


def test_detects_nightly_0200_error_spike() -> None:
    """At least one pattern is an 02:00 hourly_error_spike, corroborated by the histogram."""
    logs = generate_logs(2500, seed=3)
    patterns = mine_temporal_patterns(logs)

    spikes = [p for p in patterns if p["kind"] == "hourly_error_spike"]
    assert spikes, "expected at least one hourly_error_spike pattern"

    # At least one spike must reference the planted 02:00 nightly window.
    nightly = [
        p
        for p in spikes
        if "02:00" in p["window"] or "02:00" in p["description"]
    ]
    assert nightly, f"no 02:00 spike found among {[p['window'] for p in spikes]}"

    # Ground the claim in the data: the 02:00 hour's error rate must run materially above the
    # corpus-wide baseline error rate.
    hist = hour_histogram(logs)
    overall_errors = sum(hist[h]["error_count"] for h in range(24))
    overall_count = sum(hist[h]["count"] for h in range(24))
    base_rate = overall_errors / overall_count
    assert hist[2]["error_rate"] > base_rate * 1.4
    assert hist[2]["count"] > 0


def test_hour_histogram_covers_all_24_hours() -> None:
    """hour_histogram returns every hour 0..23 with the documented sub-keys."""
    logs = generate_logs(2500, seed=3)
    hist = hour_histogram(logs)

    assert set(hist.keys()) == set(range(24))
    for h in range(24):
        cell = hist[h]
        assert {"count", "error_count", "error_rate"} <= cell.keys()
        assert cell["count"] >= cell["error_count"] >= 0
        if cell["count"]:
            assert cell["error_rate"] == cell["error_count"] / cell["count"]


def test_distinct_kinds_present() -> None:
    """The corpus is rich enough to surface more than one *kind* of temporal pattern."""
    logs = generate_logs(2500, seed=3)
    patterns = mine_temporal_patterns(logs)
    kinds = {p["kind"] for p in patterns}
    # At minimum the error spike + one volume/burst/perf kind should appear.
    assert "hourly_error_spike" in kinds
    assert len(kinds) >= 2


def test_empty_and_tiny_inputs_do_not_crash() -> None:
    """Empty / tiny inputs return a list (possibly short) without raising."""
    assert mine_temporal_patterns([]) == []
    # A couple of logs is far too little to form a pattern, but must not blow up.
    tiny = generate_logs(2, seed=3)
    result = mine_temporal_patterns(tiny)
    assert isinstance(result, list)


def test_respects_max_patterns_cap() -> None:
    """The result never exceeds the requested max_patterns."""
    logs = generate_logs(2500, seed=3)
    capped = mine_temporal_patterns(logs, max_patterns=3)
    assert len(capped) <= 3

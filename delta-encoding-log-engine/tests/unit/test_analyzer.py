"""Unit tests for the read-only adaptive recommender (Commit 14).

Covers :class:`app.analyzer.PatternAnalyzer` / :class:`app.analyzer.CompressionMode`:
the per-step field-churn computation (mirroring the codec's add/remove/change
semantics), the **monotonic non-increasing** keyframe-interval recommendation
(the key contract: low churn ⇒ larger interval, high churn ⇒ smaller), the mode
recommendation, the JSON-native snapshot shape, the bounded sliding window, reset,
and light concurrency.

The analyzer is purely advisory — it observes the *shape* of the data and reports a
recommendation; it never touches the encoder. These tests verify the recommendation
math and the snapshot contract only; the "it doesn't alter compression" guarantee is
exercised end-to-end in the integration suite.
"""
from __future__ import annotations

import threading

import pytest

from app.analyzer import CompressionMode, PatternAnalyzer
from app.generator import generate_logs


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def _churn_window(analyzer: PatternAnalyzer, churn: float, n: int = 50) -> None:
    """Feed ``n`` synthetic adjacent pairs whose per-step churn is exactly ``churn``.

    Each pair shares a fixed key-set of 4 keys; ``round(churn * 4)`` of them change
    value between prev and cur, the rest are identical. Because every key is present
    in both entries, the union is 4 and the churn fraction is ``changed / 4`` —
    landing on 0.0, 0.25, 0.5, 0.75, 1.0 for the boundary levels we test.
    """
    n_keys = 4
    changed = round(churn * n_keys)
    for _ in range(n):
        prev = {"a": 1, "b": 2, "c": 3, "d": 4}
        cur = dict(prev)
        for i in range(changed):
            key = "abcd"[i]
            cur[key] = prev[key] + 100  # force a value change on this key
        analyzer.observe([prev, cur])


# --------------------------------------------------------------------------- #
# Step-churn computation.
# --------------------------------------------------------------------------- #
def test_churn_half_when_one_of_two_keys_changes():
    """observe([{a:1,b:2},{a:1,b:3}]) -> 1 of 2 keys changed -> churn ~ 0.5."""
    a = PatternAnalyzer()
    a.observe([{"a": 1, "b": 2}, {"a": 1, "b": 3}])
    assert a.observed_churn() == pytest.approx(0.5)


def test_churn_zero_for_identical_pair():
    """An all-identical pair has no changed keys -> churn 0.0."""
    a = PatternAnalyzer()
    a.observe([{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}])
    assert a.observed_churn() == pytest.approx(0.0)


def test_churn_counts_added_keys():
    """{a:1} -> {a:1,b:2}: 1 churned (b added) of 2 union -> 0.5."""
    a = PatternAnalyzer()
    a.observe([{"a": 1}, {"a": 1, "b": 2}])
    assert a.observed_churn() == pytest.approx(0.5)


def test_churn_counts_removed_keys():
    """{a:1} -> {}: 1 churned (a removed) of 1 union -> 1.0."""
    a = PatternAnalyzer()
    a.observe([{"a": 1}, {}])
    assert a.observed_churn() == pytest.approx(1.0)


def test_churn_mixed_add_remove_change():
    """{a:1,b:2,c:3} -> {a:9,b:2,d:4}: a changed + c removed + d added = 3 of union 4 -> 0.75."""
    a = PatternAnalyzer()
    a.observe([{"a": 1, "b": 2, "c": 3}, {"a": 9, "b": 2, "d": 4}])
    # union = {a,b,c,d} = 4 ; churned = a (changed) + c (removed) + d (added) = 3.
    assert a.observed_churn() == pytest.approx(0.75)


def test_empty_or_single_entry_batch_adds_no_samples():
    """A batch with <2 entries has no adjacent pairs -> no samples recorded."""
    a = PatternAnalyzer()
    a.observe([])
    a.observe([{"a": 1}])
    assert a.snapshot()["samples"] == 0
    assert a.observed_churn() == pytest.approx(0.0)


def test_two_empty_entries_contribute_zero_churn():
    """Two empty dicts: no fields at all -> a zero-churn sample (1 sample, churn 0.0)."""
    a = PatternAnalyzer()
    a.observe([{}, {}])
    assert a.snapshot()["samples"] == 1
    assert a.observed_churn() == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Monotonic keyframe interval — THE key contract.
# --------------------------------------------------------------------------- #
def test_low_churn_recommends_larger_interval_than_high_churn():
    """Low-churn window -> LARGER recommended interval than a high-churn window."""
    low = PatternAnalyzer(min_interval=10, max_interval=500)
    high = PatternAnalyzer(min_interval=10, max_interval=500)
    _churn_window(low, churn=0.0)   # entries barely move
    _churn_window(high, churn=1.0)  # most keys change every step
    assert low.recommended_keyframe_interval() > high.recommended_keyframe_interval()


def test_boundary_intervals_hit_max_and_min():
    """All-0.0 churn -> interval == max_interval (500); all-1.0 churn -> min_interval (10)."""
    zero = PatternAnalyzer(min_interval=10, max_interval=500)
    one = PatternAnalyzer(min_interval=10, max_interval=500)
    _churn_window(zero, churn=0.0)
    _churn_window(one, churn=1.0)
    assert zero.recommended_keyframe_interval() == 500
    assert one.recommended_keyframe_interval() == 10


def test_interval_is_non_increasing_across_churn_levels():
    """Across churn 0.0, 0.25, 0.5, 0.75, 1.0 the recommended interval is non-increasing."""
    levels = [0.0, 0.25, 0.5, 0.75, 1.0]
    intervals = []
    for churn in levels:
        a = PatternAnalyzer(min_interval=10, max_interval=500)
        _churn_window(a, churn=churn)
        intervals.append(a.recommended_keyframe_interval())

    # Each higher churn level gives an interval <= the previous (strictly non-increasing).
    for prev, cur in zip(intervals, intervals[1:]):
        assert cur <= prev, f"non-monotonic: {dict(zip(levels, intervals))}"

    # Endpoints pin to the bounds, so the sequence genuinely descends overall.
    assert intervals[0] == 500
    assert intervals[-1] == 10
    assert intervals[0] > intervals[-1]


# --------------------------------------------------------------------------- #
# Generator-driven: real synthetic batches at churn 0.0 vs 1.0.
# --------------------------------------------------------------------------- #
def test_generator_low_churn_recommends_larger_interval():
    """generate_logs(churn=0.0) -> strictly larger interval than churn=1.0 (same seed)."""
    low = PatternAnalyzer(window=400, min_interval=10, max_interval=500)
    high = PatternAnalyzer(window=400, min_interval=10, max_interval=500)

    low.observe(generate_logs(200, seed=1, churn=0.0, schema_width=10))
    high.observe(generate_logs(200, seed=1, churn=1.0, schema_width=10))

    low_iv = low.recommended_keyframe_interval()
    high_iv = high.recommended_keyframe_interval()

    print(
        f"\n[generator] low-churn(0.0): observed_churn={low.observed_churn()} "
        f"interval={low_iv}  |  high-churn(1.0): observed_churn={high.observed_churn()} "
        f"interval={high_iv}"
    )

    assert low_iv > high_iv


# --------------------------------------------------------------------------- #
# Mode recommendation.
# --------------------------------------------------------------------------- #
def test_mode_max_for_low_churn():
    """All-low-churn window (< 0.2) -> CompressionMode.MAX."""
    a = PatternAnalyzer()
    _churn_window(a, churn=0.0)
    assert a.recommended_mode() == CompressionMode.MAX


def test_mode_fast_for_high_churn():
    """All-high-churn window (> 0.6) -> CompressionMode.FAST."""
    a = PatternAnalyzer()
    _churn_window(a, churn=1.0)
    assert a.recommended_mode() == CompressionMode.FAST


def test_mode_balanced_for_mid_churn():
    """Mid churn (~0.4) -> CompressionMode.BALANCED."""
    a = PatternAnalyzer()
    # 0.4 lands between the 0.2 / 0.6 thresholds -> BALANCED.
    for _ in range(50):
        # 2 of 5 keys change -> 0.4 exactly.
        prev = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
        cur = dict(prev, a=11, b=22)
        a.observe([prev, cur])
    assert a.observed_churn() == pytest.approx(0.4)
    assert a.recommended_mode() == CompressionMode.BALANCED


def test_mode_defaults_to_configured_when_no_observations():
    """With no observations the configured mode is returned verbatim."""
    a = PatternAnalyzer(mode=CompressionMode.FAST)
    assert a.recommended_mode() == CompressionMode.FAST


# --------------------------------------------------------------------------- #
# Snapshot shape + JSON-native values.
# --------------------------------------------------------------------------- #
def test_snapshot_keys_and_json_native_types():
    """snapshot() carries all advisory keys; modes are strings, everything JSON-native."""
    a = PatternAnalyzer(window=200, current_interval=100, mode=CompressionMode.BALANCED)
    _churn_window(a, churn=0.5, n=30)
    snap = a.snapshot()

    expected_keys = {
        "observed_churn",
        "recommended_keyframe_interval",
        "current_keyframe_interval",
        "recommended_mode",
        "mode",
        "window",
        "samples",
    }
    assert set(snap) == expected_keys

    # Modes serialize as plain strings (str enum .value), not enum objects.
    assert isinstance(snap["recommended_mode"], str)
    assert isinstance(snap["mode"], str)
    assert snap["mode"] == "balanced"
    assert snap["recommended_mode"] in {"fast", "balanced", "max"}

    # Numeric fields are native ints / floats.
    assert isinstance(snap["observed_churn"], float)
    assert isinstance(snap["recommended_keyframe_interval"], int)
    assert isinstance(snap["current_keyframe_interval"], int)
    assert isinstance(snap["window"], int)
    assert isinstance(snap["samples"], int)

    # samples reflects the deque length; current_keyframe_interval is the configured value.
    assert snap["samples"] == 30
    assert snap["current_keyframe_interval"] == 100
    assert snap["window"] == 200

    # The whole snapshot is JSON-serializable (no custom encoder needed).
    import json

    json.dumps(snap)


# --------------------------------------------------------------------------- #
# No observations: sensible neutral defaults.
# --------------------------------------------------------------------------- #
def test_fresh_analyzer_neutral_defaults():
    """A fresh analyzer: churn 0.0, interval == current_interval, mode == configured."""
    a = PatternAnalyzer(current_interval=137, mode=CompressionMode.MAX)
    assert a.observed_churn() == pytest.approx(0.0)
    assert a.recommended_keyframe_interval() == 137  # == current_interval, not a bound
    assert a.recommended_mode() == CompressionMode.MAX
    assert a.snapshot()["samples"] == 0


# --------------------------------------------------------------------------- #
# Bounded sliding window.
# --------------------------------------------------------------------------- #
def test_window_is_bounded_to_maxlen():
    """A 100-entry batch (99 pairs) into window=10 keeps only the last 10 samples."""
    a = PatternAnalyzer(window=10)
    batch = [{"a": i} for i in range(100)]  # 99 adjacent pairs, all churn 1.0
    a.observe(batch)
    assert a.snapshot()["samples"] == 10


# --------------------------------------------------------------------------- #
# Reset.
# --------------------------------------------------------------------------- #
def test_reset_clears_samples_but_keeps_config():
    """reset() empties the window (churn back to 0.0) while config is preserved."""
    a = PatternAnalyzer(window=200, current_interval=100, mode=CompressionMode.BALANCED)
    _churn_window(a, churn=1.0, n=40)
    assert a.snapshot()["samples"] > 0

    a.reset()

    snap = a.snapshot()
    assert snap["samples"] == 0
    assert a.observed_churn() == pytest.approx(0.0)
    # Config kept: with the window cleared, the recommended interval falls back to
    # current_interval and the mode to the configured one.
    assert snap["current_keyframe_interval"] == 100
    assert snap["window"] == 200
    assert snap["mode"] == "balanced"
    assert a.recommended_keyframe_interval() == 100
    assert a.recommended_mode() == CompressionMode.BALANCED


# --------------------------------------------------------------------------- #
# Light thread-safety.
# --------------------------------------------------------------------------- #
def test_concurrent_observe_and_snapshot_no_errors():
    """Several threads observing + snapshotting concurrently never race or raise."""
    a = PatternAnalyzer(window=500)
    errors: list[BaseException] = []
    barrier = threading.Barrier(8)

    def worker_observe():
        try:
            barrier.wait()
            for i in range(200):
                a.observe([{"a": i, "b": i}, {"a": i + 1, "b": i}])
        except BaseException as exc:  # noqa: BLE001 — capture for the assert below.
            errors.append(exc)

    def worker_snapshot():
        try:
            barrier.wait()
            for _ in range(200):
                snap = a.snapshot()
                # Internally consistent: samples never exceeds the window cap, and
                # observed_churn is a valid fraction.
                assert 0 <= snap["samples"] <= 500
                assert 0.0 <= snap["observed_churn"] <= 1.0
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker_observe) for _ in range(4)]
    threads += [threading.Thread(target=worker_snapshot) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent access raised: {errors!r}"
    # Final snapshot is still well-formed and bounded.
    final = a.snapshot()
    assert 0 <= final["samples"] <= 500

"""Unit tests for the adaptive learning loop's drift monitor (Commit 12).

Exercises :class:`src.adaptive.DriftMonitor` in isolation — no FastAPI app, no
server, no model. The monitor is the pure *policy* half of the adaptive loop: it
tracks the served model's severity correctness over a rolling window and decides
(via :meth:`~src.adaptive.DriftMonitor.should_retrain`) when recent accuracy has
slipped far enough to warrant a retrain. These tests pin that policy down:

* the "no evidence ⇒ no drift" empty-window behaviour,
* per-record correctness reporting,
* rolling recent-accuracy arithmetic,
* the conservative full-window-AND-below-threshold retrain trigger,
* re-arming via :meth:`~src.adaptive.DriftMonitor.mark_retrained`,
* the ``deque`` ``maxlen`` (capacity) enforcement and oldest-bit eviction, and
* snapshot consistency + lifetime counters.

A tiny ``window`` keeps every threshold/full-window assertion exact and fast.
"""

from __future__ import annotations

from src.adaptive import DriftMonitor

# The full set of keys ``DriftMonitor.snapshot`` is contracted to emit.
SNAPSHOT_KEYS = {
    "recent_accuracy",
    "window_size",
    "window_capacity",
    "threshold",
    "total_feedback",
    "retrains_triggered",
    "is_window_full",
}


def test_fresh_monitor_reports_no_drift():
    """A brand-new monitor: accuracy 1.0, window not full, no retrain, full snapshot."""
    monitor = DriftMonitor(window=4, threshold=0.9)

    assert monitor.recent_accuracy() == 1.0  # empty window => "no evidence of drift"
    assert monitor.is_window_full() is False
    assert monitor.should_retrain() is False

    snapshot = monitor.snapshot()
    assert set(snapshot) == SNAPSHOT_KEYS, f"unexpected keys: {sorted(snapshot)}"
    assert snapshot["window_size"] == 0
    assert snapshot["window_capacity"] == 4
    assert snapshot["threshold"] == 0.9
    assert snapshot["total_feedback"] == 0
    assert snapshot["retrains_triggered"] == 0
    assert snapshot["is_window_full"] is False
    assert snapshot["recent_accuracy"] == 1.0


def test_record_returns_correctness():
    """``record`` returns True when severities match, False when they differ."""
    monitor = DriftMonitor(window=10, threshold=0.9)

    assert monitor.record("ERROR", "ERROR") is True  # correct prediction
    assert monitor.record("ERROR", "INFO") is False  # wrong prediction


def test_recent_accuracy_reflects_window_contents():
    """recent_accuracy is the mean of the window (3 correct + 1 wrong => 0.75)."""
    monitor = DriftMonitor(window=10, threshold=0.9)

    monitor.record("ERROR", "ERROR")  # correct
    monitor.record("INFO", "INFO")    # correct
    monitor.record("WARN", "WARN")    # correct
    monitor.record("ERROR", "DEBUG")  # wrong

    assert monitor.recent_accuracy() == 0.75


def test_should_retrain_only_when_full_and_below_threshold():
    """should_retrain fires only with a FULL window AND accuracy below threshold."""
    # --- full window, all wrong (0.0 < 0.9) -> should retrain. ---
    full_wrong = DriftMonitor(window=4, threshold=0.9)
    for _ in range(4):
        full_wrong.record("ERROR", "INFO")  # all wrong
    assert full_wrong.is_window_full() is True
    assert full_wrong.recent_accuracy() == 0.0
    assert full_wrong.should_retrain() is True

    # --- not full (3 wrong of capacity 4) -> never retrain, even at 0.0 accuracy. ---
    not_full = DriftMonitor(window=4, threshold=0.9)
    for _ in range(3):
        not_full.record("ERROR", "INFO")  # all wrong, but window not full
    assert not_full.is_window_full() is False
    assert not_full.should_retrain() is False

    # --- full window, all correct (1.0 >= 0.9) -> no retrain. ---
    full_correct = DriftMonitor(window=4, threshold=0.9)
    for _ in range(4):
        full_correct.record("ERROR", "ERROR")  # all correct
    assert full_correct.is_window_full() is True
    assert full_correct.recent_accuracy() == 1.0
    assert full_correct.should_retrain() is False


def test_should_retrain_boundary_at_threshold():
    """Accuracy exactly AT the threshold does not fire (the floor is strict <)."""
    # window=4, threshold=0.75 -> 3 correct + 1 wrong == exactly 0.75, NOT below.
    monitor = DriftMonitor(window=4, threshold=0.75)
    monitor.record("ERROR", "ERROR")  # correct
    monitor.record("INFO", "INFO")    # correct
    monitor.record("WARN", "WARN")    # correct
    monitor.record("ERROR", "DEBUG")  # wrong
    assert monitor.is_window_full() is True
    assert monitor.recent_accuracy() == 0.75
    assert monitor.should_retrain() is False  # 0.75 < 0.75 is False

    # Drop just below the threshold (2 correct + 2 wrong = 0.5) -> now it fires.
    below = DriftMonitor(window=4, threshold=0.75)
    below.record("ERROR", "ERROR")  # correct
    below.record("INFO", "INFO")    # correct
    below.record("WARN", "DEBUG")   # wrong
    below.record("ERROR", "INFO")   # wrong
    assert below.recent_accuracy() == 0.5
    assert below.should_retrain() is True


def test_mark_retrained_clears_window_and_rearms():
    """mark_retrained clears the window, re-arms, bumps retrains_triggered, accuracy 1.0."""
    monitor = DriftMonitor(window=4, threshold=0.9)
    for _ in range(4):
        monitor.record("ERROR", "INFO")  # fill below threshold
    assert monitor.should_retrain() is True

    monitor.mark_retrained()

    # Window cleared -> size 0, accuracy back to the empty-window 1.0, not full.
    assert monitor.recent_accuracy() == 1.0
    assert monitor.is_window_full() is False
    # Re-armed: a single dip can't immediately re-trigger right after a retrain.
    assert monitor.should_retrain() is False
    # Lifetime retrain counter bumped exactly once.
    assert monitor.retrains_triggered == 1

    snapshot = monitor.snapshot()
    assert snapshot["window_size"] == 0
    assert snapshot["retrains_triggered"] == 1
    assert snapshot["recent_accuracy"] == 1.0


def test_total_feedback_survives_mark_retrained():
    """total_feedback is a lifetime counter — mark_retrained does NOT reset it."""
    monitor = DriftMonitor(window=4, threshold=0.9)
    for _ in range(4):
        monitor.record("ERROR", "INFO")
    assert monitor.total_feedback == 4

    monitor.mark_retrained()
    assert monitor.total_feedback == 4  # window cleared, but lifetime count kept

    monitor.record("ERROR", "ERROR")
    assert monitor.total_feedback == 5


def test_window_maxlen_caps_size_and_evicts_oldest():
    """Recording beyond capacity caps window_size and ages out the oldest bits."""
    monitor = DriftMonitor(window=3, threshold=0.9)

    # 3 wrong fills the window below threshold.
    for _ in range(3):
        monitor.record("ERROR", "INFO")  # wrong
    assert monitor.snapshot()["window_size"] == 3
    assert monitor.recent_accuracy() == 0.0
    assert monitor.should_retrain() is True

    # 3 correct records push out the 3 wrong ones (maxlen=3): window now all-correct.
    for _ in range(3):
        monitor.record("ERROR", "ERROR")  # correct
    assert monitor.snapshot()["window_size"] == 3  # capped, not 6
    assert monitor.recent_accuracy() == 1.0        # oldest (wrong) bits evicted
    assert monitor.should_retrain() is False


def test_total_feedback_counts_records_beyond_capacity():
    """total_feedback increments on EVERY record, even past the window capacity."""
    monitor = DriftMonitor(window=3, threshold=0.9)
    for _ in range(10):  # far more than capacity 3
        monitor.record("ERROR", "ERROR")

    assert monitor.total_feedback == 10          # every record counted
    assert monitor.snapshot()["window_size"] == 3  # but the window stays capped


def test_snapshot_values_are_consistent():
    """Snapshot fields agree: size <= capacity, threshold echoed, totals consistent."""
    monitor = DriftMonitor(window=5, threshold=0.8)
    # 4 correct + 1 wrong -> accuracy 0.8 (not below 0.8), window full.
    for _ in range(4):
        monitor.record("ERROR", "ERROR")
    monitor.record("ERROR", "INFO")

    snapshot = monitor.snapshot()
    assert set(snapshot) == SNAPSHOT_KEYS
    assert 0 <= snapshot["window_size"] <= snapshot["window_capacity"]
    assert snapshot["window_capacity"] == 5
    assert snapshot["threshold"] == 0.8
    assert snapshot["total_feedback"] == 5
    assert snapshot["is_window_full"] == (
        snapshot["window_size"] == snapshot["window_capacity"]
    )
    assert snapshot["is_window_full"] is True
    assert snapshot["recent_accuracy"] == 0.8


def test_window_coerced_to_at_least_one():
    """A non-positive window is coerced to capacity 1 (deque needs a positive maxlen)."""
    monitor = DriftMonitor(window=0, threshold=0.9)
    assert monitor.snapshot()["window_capacity"] == 1

    monitor.record("ERROR", "INFO")  # one wrong fills the capacity-1 window
    assert monitor.is_window_full() is True
    assert monitor.recent_accuracy() == 0.0
    assert monitor.should_retrain() is True

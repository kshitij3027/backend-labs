"""Unit tests for :mod:`app.metrics` — the live-operation observability registry.

These tests pin the runtime metrics contract that the success-criteria gates in
*plan.md* read through :class:`~app.metrics.MetricsRegistry`:

* **Recording & lifetime counters** — ``record`` / ``record_latency`` fold each call
  into per-op ``calls`` / ``entries`` totals visible in ``snapshot()["operations"]``.
* **Latency percentiles** — a known 1ms..100ms sample set drives the
  linear-interpolation percentile, asserting the ``p50 <= p90 <= p99 <= max`` ordering
  guarantee, the median / p99 / mean / max landing where numpy's ``method="linear"``
  puts them, and the empty / single-sample edge cases (all-zero block, no KeyError).
* **Throughput** — lifetime ``entries_total / time_total`` with the zero-time guard.
* **Errors** — the global ``system.errors`` counter via :meth:`incr_error`.
* **``time_block``** — times a ``with`` block outside the lock and records even on raise.
* **Snapshot shape** — every required top-level key, including the convenience hooks
  ``reconstruct_p50_ms`` / ``reconstruct_p99_ms`` / ``compress_throughput_eps``.
* **Uptime / reset** — monotonic, non-decreasing uptime that restarts on :meth:`reset`.
* **Bounded window** — the ``deque(maxlen=max_samples)`` caps the percentile window
  while the lifetime ``calls`` / ``entries`` totals keep counting past the cap.
* **Lock correctness** — concurrent ``record`` / ``incr_error`` from many threads land
  with no lost updates.

Timing-sensitive assertions are deliberately lenient (short sleeps, wide bands): the
point is that *a* sample was recorded and ordering holds, never an exact duration.
"""
from __future__ import annotations

import threading
import time

import pytest

from app.metrics import MetricsRegistry, _percentile_linear


# --------------------------------------------------------------------------- #
# Recording & lifetime counters                                               #
# --------------------------------------------------------------------------- #
def test_record_accumulates_calls_and_entries():
    reg = MetricsRegistry()
    reg.record("compress", entries=10, seconds=0.01)
    reg.record("compress", entries=5, seconds=0.02)
    reg.record("compress", entries=7, seconds=0.03)

    op = reg.snapshot()["operations"]["compress"]
    assert op["calls"] == 3
    assert op["entries"] == 22


def test_record_isolates_distinct_ops():
    reg = MetricsRegistry()
    reg.record("compress", entries=100, seconds=0.05)
    reg.record("reconstruct", entries=1, seconds=0.001)
    reg.record("reconstruct", entries=1, seconds=0.002)

    ops = reg.snapshot()["operations"]
    assert ops["compress"]["calls"] == 1
    assert ops["compress"]["entries"] == 100
    assert ops["reconstruct"]["calls"] == 2
    assert ops["reconstruct"]["entries"] == 2


def test_record_latency_equivalent_to_record_entries_1():
    """record_latency(op, s) must be exactly record(op, entries=1, seconds=s)."""
    reg_a = MetricsRegistry()
    reg_b = MetricsRegistry()

    for s in (0.001, 0.005, 0.010):
        reg_a.record_latency("reconstruct", s)
        reg_b.record("reconstruct", entries=1, seconds=s)

    a = reg_a.snapshot()["operations"]["reconstruct"]
    b = reg_b.snapshot()["operations"]["reconstruct"]
    assert a["calls"] == b["calls"] == 3
    assert a["entries"] == b["entries"] == 3
    # Same samples in → same percentile block out.
    assert reg_a.percentiles("reconstruct") == reg_b.percentiles("reconstruct")


# --------------------------------------------------------------------------- #
# Percentiles                                                                 #
# --------------------------------------------------------------------------- #
def test_percentiles_known_distribution_1ms_to_100ms():
    """Feed 1ms..100ms; assert ordering + landmark values under linear interpolation."""
    reg = MetricsRegistry()
    # seconds 0.001 .. 0.100 -> 1ms .. 100ms, fed out of order to exercise the sort.
    seconds = [0.001 * i for i in range(1, 101)]
    for s in reversed(seconds):
        reg.record_latency("op", s)

    p = reg.percentiles("op")
    assert p["count"] == 100

    # Monotone ordering guarantee the latency gates rely on.
    assert p["p50_ms"] <= p["p90_ms"] <= p["p99_ms"] <= p["max_ms"]

    # max is the largest sample = 100ms exactly.
    assert p["max_ms"] == pytest.approx(100.0, abs=1e-6)

    # Median around 50ms (linear interp of rank 49.5 over [1..100] -> 50.5ms).
    assert 45.0 <= p["p50_ms"] <= 55.0
    # p99 near the top of the range.
    assert 95.0 <= p["p99_ms"] <= 100.0
    # mean of 1..100 ms = 50.5ms.
    assert p["mean_ms"] == pytest.approx(50.5, abs=0.5)


def test_percentiles_empty_op_is_all_zero_no_keyerror():
    reg = MetricsRegistry()
    p = reg.percentiles("never_recorded")
    assert p == {
        "count": 0,
        "p50_ms": 0.0,
        "p90_ms": 0.0,
        "p99_ms": 0.0,
        "mean_ms": 0.0,
        "max_ms": 0.0,
    }


def test_percentiles_single_sample_collapses_to_that_value():
    reg = MetricsRegistry()
    reg.record_latency("op", 0.042)  # 42ms
    p = reg.percentiles("op")
    assert p["count"] == 1
    assert p["p50_ms"] == pytest.approx(42.0, abs=1e-6)
    assert p["p90_ms"] == pytest.approx(42.0, abs=1e-6)
    assert p["p99_ms"] == pytest.approx(42.0, abs=1e-6)
    assert p["max_ms"] == pytest.approx(42.0, abs=1e-6)
    assert p["mean_ms"] == pytest.approx(42.0, abs=1e-6)
    # Ordering still trivially holds.
    assert p["p50_ms"] <= p["p90_ms"] <= p["p99_ms"] <= p["max_ms"]


def test_percentile_linear_module_function_landmarks():
    """The pure helper is the convention; pin it directly on a 1..100 list."""
    values = [float(i) for i in range(1, 101)]  # ascending, already sorted
    assert _percentile_linear([], 50.0) == 0.0
    assert _percentile_linear([7.0], 99.0) == 7.0
    # rank 0.99*99 = 98.01 -> interpolate between values[98]=99 and values[99]=100.
    assert _percentile_linear(values, 100.0) == pytest.approx(100.0)
    assert _percentile_linear(values, 0.0) == pytest.approx(1.0)
    # Monotone non-decreasing across P on the fixed sorted window.
    p50 = _percentile_linear(values, 50.0)
    p90 = _percentile_linear(values, 90.0)
    p99 = _percentile_linear(values, 99.0)
    assert p50 <= p90 <= p99 <= values[-1]


# --------------------------------------------------------------------------- #
# Throughput                                                                  #
# --------------------------------------------------------------------------- #
def test_throughput_eps_lifetime_formula():
    reg = MetricsRegistry()
    # 1000 entries total over 0.5s total, spread across calls.
    reg.record("compress", entries=400, seconds=0.2)
    reg.record("compress", entries=600, seconds=0.3)
    # entries_total=1000, time_total=0.5 -> 2000.0 eps
    assert reg.throughput_eps("compress") == pytest.approx(round(1000 / 0.5, 2))
    assert reg.throughput_eps("compress") == 2000.0


def test_throughput_eps_unknown_op_is_zero():
    reg = MetricsRegistry()
    assert reg.throughput_eps("never_seen") == 0.0


def test_throughput_eps_zero_time_is_zero():
    reg = MetricsRegistry()
    # Recorded entries but zero elapsed time -> guarded division, returns 0.0.
    reg.record("compress", entries=500, seconds=0.0)
    assert reg.throughput_eps("compress") == 0.0


def test_throughput_eps_is_rounded_to_two_decimals():
    reg = MetricsRegistry()
    reg.record("compress", entries=1, seconds=0.003)  # 333.333... eps
    assert reg.throughput_eps("compress") == round(1 / 0.003, 2)


# --------------------------------------------------------------------------- #
# Errors                                                                      #
# --------------------------------------------------------------------------- #
def test_incr_error_accumulates_default_and_explicit():
    reg = MetricsRegistry()
    assert reg.errors == 0
    reg.incr_error()
    reg.incr_error(5)
    reg.incr_error()
    assert reg.errors == 7
    assert reg.snapshot()["errors"] == 7


# --------------------------------------------------------------------------- #
# time_block                                                                  #
# --------------------------------------------------------------------------- #
def test_time_block_records_a_sample():
    reg = MetricsRegistry()
    with reg.time_block("reconstruct", entries=1):
        time.sleep(0.01)

    op = reg.snapshot()["operations"]["reconstruct"]
    assert op["calls"] == 1
    assert op["entries"] == 1
    p = reg.percentiles("reconstruct")
    assert p["count"] == 1
    # Sleep timing is fuzzy; just assert a positive latency was captured.
    assert p["p50_ms"] > 0.0


def test_time_block_records_even_when_block_raises():
    reg = MetricsRegistry()
    with pytest.raises(ValueError):
        with reg.time_block("reconstruct", entries=1):
            raise ValueError("boom")

    # The finally-clause still recorded the latency despite the exception.
    op = reg.snapshot()["operations"]["reconstruct"]
    assert op["calls"] == 1
    assert op["entries"] == 1
    assert reg.percentiles("reconstruct")["count"] == 1


def test_time_block_default_entries_is_one():
    reg = MetricsRegistry()
    with reg.time_block("reconstruct"):
        pass
    assert reg.snapshot()["operations"]["reconstruct"]["entries"] == 1


# --------------------------------------------------------------------------- #
# Snapshot shape                                                              #
# --------------------------------------------------------------------------- #
def test_snapshot_has_all_required_keys_when_empty():
    reg = MetricsRegistry()
    snap = reg.snapshot()

    # Top-level required keys.
    for key in (
        "uptime_seconds",
        "errors",
        "operations",
        "reconstruct_p50_ms",
        "reconstruct_p99_ms",
        "compress_throughput_eps",
    ):
        assert key in snap

    assert isinstance(snap["uptime_seconds"], float)
    assert snap["uptime_seconds"] >= 0.0
    assert isinstance(snap["errors"], int)
    assert snap["errors"] == 0
    assert isinstance(snap["operations"], dict)
    assert snap["operations"] == {}

    # Convenience keys are floats and 0.0 before any samples exist.
    assert isinstance(snap["reconstruct_p50_ms"], float)
    assert isinstance(snap["reconstruct_p99_ms"], float)
    assert isinstance(snap["compress_throughput_eps"], float)
    assert snap["reconstruct_p50_ms"] == 0.0
    assert snap["reconstruct_p99_ms"] == 0.0
    assert snap["compress_throughput_eps"] == 0.0


def test_snapshot_convenience_keys_reflect_op_blocks():
    reg = MetricsRegistry()
    # A few reconstructs so p50/p99 become non-zero.
    for s in [0.001 * i for i in range(1, 21)]:
        reg.record_latency("reconstruct", s)
    # Compress with known throughput.
    reg.record("compress", entries=1000, seconds=0.5)

    snap = reg.snapshot()
    reconstruct_block = snap["operations"]["reconstruct"]["latency_ms"]
    compress_block = snap["operations"]["compress"]

    # Convenience keys mirror the op blocks exactly.
    assert snap["reconstruct_p50_ms"] == reconstruct_block["p50_ms"]
    assert snap["reconstruct_p99_ms"] == reconstruct_block["p99_ms"]
    assert snap["compress_throughput_eps"] == compress_block["throughput_eps"]

    assert snap["reconstruct_p50_ms"] > 0.0
    assert snap["compress_throughput_eps"] == 2000.0


def test_snapshot_operation_block_shape():
    reg = MetricsRegistry()
    reg.record("compress", entries=100, seconds=0.05)
    block = reg.snapshot()["operations"]["compress"]
    assert set(block.keys()) == {"calls", "entries", "throughput_eps", "latency_ms"}
    assert block["calls"] == 1
    assert block["entries"] == 100
    assert block["throughput_eps"] == reg.throughput_eps("compress")
    assert block["latency_ms"] == reg.percentiles("compress")


# --------------------------------------------------------------------------- #
# Uptime                                                                      #
# --------------------------------------------------------------------------- #
def test_uptime_is_nonnegative_and_nondecreasing():
    reg = MetricsRegistry()
    first = reg.uptime_seconds()
    assert first >= 0.0
    time.sleep(0.02)
    second = reg.uptime_seconds()
    assert second >= first


# --------------------------------------------------------------------------- #
# Reset                                                                       #
# --------------------------------------------------------------------------- #
def test_reset_clears_ops_errors_and_restarts_uptime():
    reg = MetricsRegistry()
    reg.record("compress", entries=10, seconds=0.01)
    reg.record_latency("reconstruct", 0.002)
    reg.incr_error(3)

    # Let some uptime accrue, capture it, then reset.
    time.sleep(0.03)
    pre_reset_uptime = reg.uptime_seconds()
    assert pre_reset_uptime > 0.0

    reg.reset()

    snap = reg.snapshot()
    assert snap["operations"] == {}
    assert snap["errors"] == 0
    assert reg.errors == 0

    # Uptime clock restarted: the post-reset reading is small and below the pre-reset one.
    post_reset_uptime = reg.uptime_seconds()
    assert post_reset_uptime < pre_reset_uptime

    # Convenience keys fall back to zero after the wipe.
    assert snap["reconstruct_p50_ms"] == 0.0
    assert snap["compress_throughput_eps"] == 0.0


# --------------------------------------------------------------------------- #
# Bounded latency window                                                      #
# --------------------------------------------------------------------------- #
def test_bounded_window_caps_percentile_count_but_not_lifetime_totals():
    reg = MetricsRegistry(max_samples=10)
    for i in range(1, 101):
        # entries=2 each so the lifetime entries total is verifiable (200), and the
        # latencies form a known increasing stream the deque will truncate to the last 10.
        reg.record("compress", entries=2, seconds=0.001 * i)

    # The percentile window is capped at the deque maxlen.
    p = reg.percentiles("compress")
    assert p["count"] == 10
    # The last 10 samples are 0.091..0.100s -> max 100ms.
    assert p["max_ms"] == pytest.approx(100.0, abs=1e-6)

    # Lifetime counters are NOT capped.
    op = reg.snapshot()["operations"]["compress"]
    assert op["calls"] == 100
    assert op["entries"] == 200


# --------------------------------------------------------------------------- #
# Lazy creation / no KeyError                                                 #
# --------------------------------------------------------------------------- #
def test_readers_on_unseen_op_never_raise():
    reg = MetricsRegistry()
    # Neither of these should raise; both return the empty/zero result.
    assert reg.percentiles("never_seen")["count"] == 0
    assert reg.throughput_eps("never_seen") == 0.0
    # The unseen op was not lazily created by reading.
    assert "never_seen" not in reg.snapshot()["operations"]


# --------------------------------------------------------------------------- #
# Light thread-safety                                                         #
# --------------------------------------------------------------------------- #
def test_concurrent_record_and_incr_error_no_lost_updates():
    reg = MetricsRegistry()
    n_threads = 8
    per_thread = 500

    def worker() -> None:
        for _ in range(per_thread):
            reg.record("compress", entries=1, seconds=0.001)
            reg.incr_error()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    expected = n_threads * per_thread
    op = reg.snapshot()["operations"]["compress"]
    assert op["calls"] == expected
    assert op["entries"] == expected
    assert reg.errors == expected

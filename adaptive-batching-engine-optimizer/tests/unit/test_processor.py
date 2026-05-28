"""Unit tests for src.processor — the concave batch-cost model.

All tests are deterministic. The default processor uses ``noise_std=0.0`` so
``process_batch`` is reproducible without an RNG; the noise-path tests inject a
seeded ``random.Random`` so they never flake.
"""

from __future__ import annotations

import math
import random

import pytest

from src.processor import BatchProcessor, ProcessResult
from src.settings import get_settings

MAX_BATCH = get_settings().max_batch_size  # 5000 with defaults


# --- Concavity / interior optimum -------------------------------------------


def test_throughput_curve_has_strict_interior_peak_near_790() -> None:
    """T(B) rises then falls; its grid argmax is interior and near B* ≈ 790."""
    proc = BatchProcessor()
    sizes = list(range(50, 5001, 50))
    best = max(sizes, key=proc.throughput_for)

    # The maximiser is strictly inside the swept window (not at either endpoint),
    # which is what makes the curve concave rather than monotone.
    assert best != sizes[0]
    assert best != sizes[-1]
    assert 50 < best < 5000

    # And it lands close to the analytic optimum B* = sqrt(5.0 / 8e-6) ≈ 790.
    assert abs(best - 790) <= 150


def test_peak_beats_both_extremes() -> None:
    proc = BatchProcessor()
    assert proc.throughput_for(790) > proc.throughput_for(50)
    assert proc.throughput_for(790) > proc.throughput_for(5000)


# --- optimal_batch_size -----------------------------------------------------


def test_optimal_batch_size_matches_analytic_formula() -> None:
    proc = BatchProcessor()
    assert proc.optimal_batch_size() == pytest.approx(math.sqrt(5.0 / 8e-6))


def test_optimal_batch_size_custom_params() -> None:
    proc = BatchProcessor(overhead_ms=10.0, saturation_coeff=1e-5)
    assert proc.optimal_batch_size() == pytest.approx(math.sqrt(10.0 / 1e-5))


def test_optimal_batch_size_infinite_when_no_saturation() -> None:
    """With no quadratic term the curve never turns over → no interior optimum."""
    assert BatchProcessor(saturation_coeff=0.0).optimal_batch_size() == math.inf
    assert BatchProcessor(saturation_coeff=-1.0).optimal_batch_size() == math.inf


# --- Determinism ------------------------------------------------------------


def test_process_batch_is_deterministic_without_noise() -> None:
    proc = BatchProcessor()  # noise_std defaults to 0.0
    a = proc.process_batch(500)
    b = proc.process_batch(500)
    assert a.throughput == b.throughput
    assert a.latency_ms == b.latency_ms


def test_noise_is_reproducible_with_seeded_rng() -> None:
    """Two processors seeded identically produce identical noisy results."""
    proc_a = BatchProcessor(noise_std=0.05, rng=random.Random(42))
    proc_b = BatchProcessor(noise_std=0.05, rng=random.Random(42))

    for _ in range(5):
        ra = proc_a.process_batch(500)
        rb = proc_b.process_batch(500)
        assert ra.throughput == rb.throughput
        assert ra.latency_ms == rb.latency_ms


def test_noise_differs_from_noise_free_value() -> None:
    """Applied noise shifts latency/throughput away from the clean curve."""
    proc = BatchProcessor(noise_std=0.05, rng=random.Random(42))
    clean = proc.throughput_for(500)
    noisy = proc.process_batch(500)
    assert noisy.throughput != clean
    # ...but only modestly: ~5% relative noise keeps it in the same ballpark.
    assert noisy.throughput == pytest.approx(clean, rel=0.5)


# --- Latency monotonicity ---------------------------------------------------


def test_latency_strictly_increases_with_batch_size() -> None:
    proc = BatchProcessor()  # noise-free
    sizes = list(range(100, 2001, 100))
    latencies = [proc.process_batch(b).latency_ms for b in sizes]
    for earlier, later in zip(latencies, latencies[1:]):
        assert later > earlier


# --- Simulated pressure -----------------------------------------------------


def test_pressure_high_at_max_batch() -> None:
    proc = BatchProcessor()
    res = proc.process_batch(MAX_BATCH)
    assert res.mem_pressure == pytest.approx(100.0)
    assert res.cpu_pressure >= 90.0


def test_pressure_low_at_small_batch() -> None:
    proc = BatchProcessor()
    res = proc.process_batch(50)
    assert res.cpu_pressure < 20.0
    assert res.mem_pressure < 20.0


def test_pressure_clamped_above_max_batch() -> None:
    """Even an oversized batch cannot push pressure past 100."""
    proc = BatchProcessor()
    res = proc.process_batch(MAX_BATCH * 3, messages_per_second=10_000.0)
    assert res.mem_pressure <= 100.0
    assert res.cpu_pressure <= 100.0


def test_cpu_pressure_monotone_in_message_rate() -> None:
    """A higher incoming rate adds backlog → higher CPU at the same batch size."""
    proc = BatchProcessor()
    low = proc.process_batch(500, messages_per_second=0.0)
    high = proc.process_batch(500, messages_per_second=1000.0)
    assert high.cpu_pressure > low.cpu_pressure
    # Memory pressure ignores the rate entirely.
    assert high.mem_pressure == pytest.approx(low.mem_pressure)


def test_batch_size_dominates_message_rate_for_cpu() -> None:
    """Batch size is the dominant CPU driver; the backlog term stays small."""
    proc = BatchProcessor()
    big_batch_no_rate = proc.process_batch(4000, messages_per_second=0.0)
    small_batch_high_rate = proc.process_batch(200, messages_per_second=1000.0)
    assert big_batch_no_rate.cpu_pressure > small_batch_high_rate.cpu_pressure


# --- Throughput consistency -------------------------------------------------


def test_throughput_equals_size_over_latency_noise_free() -> None:
    """For the noise-free path, throughput = batch_size / (latency_ms / 1000)."""
    proc = BatchProcessor()
    for b in (50, 500, 790, 2000, 5000):
        res: ProcessResult = proc.process_batch(b)
        assert res.throughput == pytest.approx(b / (res.latency_ms / 1000.0))

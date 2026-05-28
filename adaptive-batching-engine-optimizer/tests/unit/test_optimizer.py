"""Unit tests for src.optimizer — the gradient-ascent batch-size controller.

All tests are deterministic. ``OptimizationEngine.update`` is pure arithmetic, so
the step-by-step cases assert exact integer batch sizes. The convergence test
drives the engine with a noise-free :class:`BatchProcessor` (``noise_std=0.0``)
so the trajectory is fully reproducible without any RNG seeding.
"""

from __future__ import annotations

import time

import pytest

from src.models import OptimizerConfigUpdate
from src.optimizer import OptimizationEngine, OptimizerStep
from src.processor import BatchProcessor


# --- compute_utility --------------------------------------------------------


def test_compute_utility_known_value() -> None:
    """U(20000, 100) with w_t=0.7, w_l=0.3 = 0.7*1.0 + 0.3*0.5 = 0.85."""
    eng = OptimizationEngine(
        weight_throughput=0.7,
        weight_latency=0.3,
        throughput_scale=20000.0,
        latency_scale=100.0,
    )
    assert eng.compute_utility(20000.0, 100.0) == pytest.approx(0.85)


def test_compute_utility_zero_latency_zero_throughput() -> None:
    """At throughput 0 the t-term is 0; at latency 0 the l-benefit is 1."""
    eng = OptimizationEngine(
        weight_throughput=0.7,
        weight_latency=0.3,
        throughput_scale=20000.0,
        latency_scale=100.0,
    )
    # 0.7*0 + 0.3*(1/(1+0)) = 0.3
    assert eng.compute_utility(0.0, 0.0) == pytest.approx(0.3)


def test_higher_throughput_raises_utility() -> None:
    eng = OptimizationEngine(weight_throughput=0.7, weight_latency=0.3)
    low = eng.compute_utility(10000.0, 100.0)
    high = eng.compute_utility(30000.0, 100.0)
    assert high > low


def test_higher_latency_lowers_utility() -> None:
    eng = OptimizationEngine(weight_throughput=0.7, weight_latency=0.3)
    fast = eng.compute_utility(20000.0, 50.0)
    slow = eng.compute_utility(20000.0, 500.0)
    assert fast > slow


# --- First-step smoothing + factor exactness --------------------------------


def test_first_step_smoothing_is_exact() -> None:
    """initial=1000, alpha=0.2, inc=1.1 -> optimal=1100, smoothed=1020."""
    eng = OptimizationEngine(
        initial_batch_size=1000,
        smoothing_alpha=0.2,
        increase_factor=1.1,
        min_batch_size=50,
        max_batch_size=5000,
    )
    step = eng.update(15000.0, 80.0)

    assert isinstance(step, OptimizerStep)
    assert step.old_batch_size == 1000
    assert step.new_batch_size == 1020  # 1000*0.8 + 1100*0.2
    assert step.gradient == 0.0  # first step has no prior point
    assert step.direction == 1  # stays +1 on the first observation
    assert eng.batch_size == 1020
    assert eng.last_gradient == 0.0


# --- Direction flip on utility drop -----------------------------------------


def test_direction_flips_on_utility_drop() -> None:
    """A second sample with lower utility flips direction to -1 and shrinks B."""
    eng = OptimizationEngine(
        initial_batch_size=1000,
        smoothing_alpha=0.2,
        increase_factor=1.1,
        decrease_factor=0.9,
        min_batch_size=50,
        max_batch_size=5000,
    )
    first = eng.update(15000.0, 80.0)
    assert first.new_batch_size == 1020
    assert first.direction == 1

    # Lower throughput at the same latency -> U2 < U1 -> flip to -1.
    second = eng.update(5000.0, 80.0)
    assert second.direction == -1
    assert second.new_batch_size < 1020
    assert eng.batch_size < 1020
    # Gradient was computed from a real second-difference now (non-None prev).
    assert second.gradient != 0.0


# --- Clamp upper ------------------------------------------------------------


def test_clamp_upper_bound() -> None:
    """alpha=1 makes smoothed==optimal=5390, clamped down to max=5000."""
    eng = OptimizationEngine(
        initial_batch_size=4900,
        max_batch_size=5000,
        smoothing_alpha=1.0,
        increase_factor=1.1,
    )
    step = eng.update(18000.0, 200.0)
    # optimal = 4900 * 1.1 = 5390, smoothed = 5390 (alpha=1) -> clamp to 5000
    assert step.new_batch_size == 5000
    assert eng.batch_size == 5000


# --- Clamp lower / floor at min ---------------------------------------------


def test_floor_at_min_batch_size() -> None:
    """Driving the engine downward never produces a batch below min."""
    eng = OptimizationEngine(
        initial_batch_size=50,
        min_batch_size=50,
        max_batch_size=5000,
        smoothing_alpha=1.0,
        increase_factor=1.1,
        decrease_factor=0.9,
    )
    observed: list[int] = []

    # First an up-step to seed a prior utility, then a utility drop to flip down.
    up = eng.update(12000.0, 80.0)
    observed.append(up.new_batch_size)
    # Now repeatedly feed dropping utility so the controller keeps backing off.
    for throughput in (4000.0, 3000.0, 2000.0, 1000.0, 500.0):
        step = eng.update(throughput, 80.0)
        observed.append(step.new_batch_size)

    assert all(b >= 50 for b in observed), observed
    # And it actually sits on the floor by the end (decrease pushes below 50).
    assert eng.batch_size == 50


# --- reset ------------------------------------------------------------------


def test_reset_restores_initial_state_and_first_step_semantics() -> None:
    eng = OptimizationEngine(
        initial_batch_size=1000,
        smoothing_alpha=0.2,
        increase_factor=1.1,
        decrease_factor=0.9,
        min_batch_size=50,
        max_batch_size=5000,
    )
    eng.update(15000.0, 80.0)
    eng.update(6000.0, 90.0)
    eng.update(7000.0, 85.0)
    assert eng.batch_size != 1000  # state has moved

    eng.reset()
    assert eng.batch_size == 1000
    assert eng.last_gradient == 0.0

    # The very next update must behave like a brand-new first step.
    step = eng.update(15000.0, 80.0)
    assert step.gradient == 0.0
    assert step.direction == 1
    assert step.old_batch_size == 1000
    assert step.new_batch_size == 1020  # identical to the original first step


# --- apply_config -----------------------------------------------------------


def test_apply_config_updates_fields_and_reclamps() -> None:
    eng = OptimizationEngine(
        initial_batch_size=1000,
        smoothing_alpha=0.2,
        increase_factor=1.1,
        max_batch_size=5000,
        min_batch_size=50,
    )
    # Push the batch well above the soon-to-be-applied 300 cap.
    eng.set_batch_size(1000)
    assert eng.batch_size == 1000

    update = OptimizerConfigUpdate(smoothing_alpha=0.5, max_batch_size=300)
    eng.apply_config(update)

    assert eng.smoothing_alpha == 0.5
    assert eng.max_batch_size == 300
    # Current batch re-projected into the narrowed window.
    assert eng.batch_size <= 300


def test_apply_config_ignores_unrelated_fields() -> None:
    """Fields the optimizer does not own (e.g. optimization_interval) are no-ops."""
    eng = OptimizationEngine(
        initial_batch_size=500,
        smoothing_alpha=0.2,
        max_batch_size=5000,
        min_batch_size=50,
    )
    before_alpha = eng.smoothing_alpha
    before_max = eng.max_batch_size

    # These are all loop/safety fields the engine must ignore without error.
    update = OptimizerConfigUpdate(
        optimization_interval=99.0,
        cpu_constraint_threshold=42.0,
        memory_constraint_threshold=42.0,
        latency_constraint_threshold=42.0,
    )
    eng.apply_config(update)  # must not raise

    assert eng.smoothing_alpha == before_alpha
    assert eng.max_batch_size == before_max
    assert eng.batch_size == 500  # unchanged


def test_apply_config_partial_weights() -> None:
    eng = OptimizationEngine(weight_throughput=0.7, weight_latency=0.3)
    eng.apply_config(
        OptimizerConfigUpdate(
            weight_throughput=0.9,
            batch_increase_factor=1.25,
            batch_decrease_factor=0.8,
        )
    )
    assert eng.weight_throughput == 0.9
    assert eng.weight_latency == 0.3  # untouched
    assert eng.increase_factor == 1.25
    assert eng.decrease_factor == 0.8


# --- set_batch_size ---------------------------------------------------------


def test_set_batch_size_clamps_to_bounds() -> None:
    eng = OptimizationEngine(
        initial_batch_size=1000, min_batch_size=50, max_batch_size=5000
    )
    eng.set_batch_size(10_000)
    assert eng.batch_size == 5000  # clamped to max

    eng.set_batch_size(1)
    assert eng.batch_size == 50  # clamped to min

    eng.set_batch_size(777)
    assert eng.batch_size == 777  # in-range value passes through


# --- Convergence (deterministic, real processor) ----------------------------


def test_converges_to_interior_optimum_and_improves_throughput(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The controller climbs the noise-free concave curve and settles interior.

    Asserts a generous-tolerance convergence: a strictly interior final batch, a
    >=30% throughput gain over the initial batch, and a stable last-20 tail. The
    exact convergence point (~roughly 400-600 due to the latency penalty) is not
    asserted precisely.
    """
    proc = BatchProcessor(noise_std=0.0)  # fully deterministic curve
    opt = OptimizationEngine(initial_batch_size=100)

    initial_batch = opt.batch_size
    trajectory: list[int] = []
    for _ in range(120):
        b = opt.batch_size
        res = proc.process_batch(b)
        opt.update(res.throughput, res.latency_ms)
        trajectory.append(opt.batch_size)

    final_batch = opt.batch_size
    init_tput = proc.throughput_for(initial_batch)
    final_tput = proc.throughput_for(final_batch)
    improvement_pct = (final_tput / init_tput - 1.0) * 100.0

    tail = trajectory[-20:]
    tail_mean = sum(tail) / len(tail)
    tail_spread = max(tail) - min(tail)

    # Reported so the orchestrator can see the observed behaviour (needs -s).
    print(
        f"\n[convergence] initial_batch={initial_batch} "
        f"final_batch={final_batch} "
        f"improvement={improvement_pct:.1f}% "
        f"tail_spread={tail_spread} tail_mean={tail_mean:.1f}"
    )

    # (a) strictly interior — not pinned to either bound.
    assert opt.min_batch_size < final_batch < opt.max_batch_size, trajectory[-10:]

    # (b) >= 30% throughput improvement over the initial batch.
    assert final_tput >= 1.30 * init_tput, (
        f"only {improvement_pct:.1f}% improvement; "
        f"init={init_tput:.0f} final={final_tput:.0f}"
    )

    # (c) stable tail: no runaway / oscillation blow-up.
    assert tail_spread <= 0.30 * tail_mean, f"tail unstable: {tail}"


# --- Timing budget ----------------------------------------------------------


def test_update_is_under_10ms_average() -> None:
    """1000 update() calls must average well under the 10ms/calculation budget."""
    eng = OptimizationEngine(initial_batch_size=500)

    start = time.perf_counter()
    for i in range(1000):
        # Vary inputs a little so the branch logic actually runs; no sleeps.
        eng.update(10000.0 + (i % 7) * 100.0, 100.0 + (i % 5))
    elapsed = time.perf_counter() - start

    avg_ms = (elapsed / 1000) * 1000.0
    assert avg_ms < 10.0, f"average update took {avg_ms:.4f} ms"

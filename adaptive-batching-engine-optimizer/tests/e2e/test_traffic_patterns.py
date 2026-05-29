"""Behaviour validation across the four required traffic patterns.

Spec Success Criteria: *"Behavior validation across four traffic patterns:
steady load, burst traffic, resource constraint, recovery."* Each test below
exercises one pattern with a fresh, fully deterministic batcher — a noise-free
:class:`~src.processor.BatchProcessor`, a seeded :class:`~src.loadsim.LoadSimulator`,
a fixed :class:`FakeMonitor`, and explicit ``timestamp``/``interval`` per tick —
so emergencies are driven only by simulated workload pressure and every run is
reproducible on any host. Assertions are deliberately tolerant: the point is the
qualitative behaviour of each pattern, not exact numbers.
"""

from __future__ import annotations

import random

from src.batcher import AdaptiveBatcher
from src.loadsim import LoadSimulator
from src.metrics import ResourceReading
from src.models import OptimizerConfigUpdate, OptimizerState
from src.processor import BatchProcessor

# States that count as "the loop is actively running / settled at the optimum",
# i.e. it has left LEARNING/EMERGENCY.
_SETTLED = {OptimizerState.OPTIMIZING, OptimizerState.STABLE}


class FakeMonitor:
    """Fixed-reading resource monitor (keeps the real host out of the loop)."""

    def __init__(self, cpu: float = 5.0, mem: float = 40.0, avail: float = 8000.0) -> None:
        self._r = ResourceReading(cpu, mem, avail)

    def sample(self) -> ResourceReading:
        return self._r


def _batcher(
    *,
    messages_per_second: float = 200.0,
    burst_probability: float = 0.0,
    burst_multiplier: float = 10.0,
    seed: int = 0,
) -> tuple[AdaptiveBatcher, BatchProcessor]:
    """Build a deterministic batcher and return it with its noise-free processor."""
    proc = BatchProcessor(noise_std=0.0)
    batcher = AdaptiveBatcher(
        processor=proc,
        load_simulator=LoadSimulator(
            messages_per_second=messages_per_second,
            burst_probability=burst_probability,
            burst_multiplier=burst_multiplier,
            rng=random.Random(seed),
        ),
        resource_monitor=FakeMonitor(),
    )
    return batcher, proc


def test_steady_load_pattern() -> None:
    """Pattern 1 — steady load.

    Under a constant, burst-free arrival rate the loop must leave LEARNING,
    reach OPTIMIZING (and likely settle STABLE), climb the batch from the
    initial seed into an interior mid-range, and beat the static baseline by
    >= 30%. The settled tail must never park in an unsafe (>90%) region.
    """
    batcher, proc = _batcher(messages_per_second=200.0, burst_probability=0.0, seed=0)
    initial_batch = batcher.optimizer.batch_size

    saw_optimizing = False
    for i in range(120):
        batcher.tick(timestamp=float(i), interval=1.0)
        if batcher.state_machine.state is OptimizerState.OPTIMIZING:
            saw_optimizing = True

    final_batch = batcher.optimizer.batch_size
    series = batcher.metrics_series()
    tail_tput = sum(series["throughput"][-20:]) / 20
    static_tput = proc.throughput_for(100)

    print(
        f"\n[steady] initial={initial_batch} final={final_batch} "
        f"state={batcher.state_machine.state.value} "
        f"tail_tput={tail_tput:.0f} static@100={static_tput:.0f} "
        f"improvement={(tail_tput / static_tput - 1) * 100:.1f}%"
    )

    assert saw_optimizing
    assert batcher.state_machine.state in _SETTLED
    # Climbed off the seed into an interior mid-range.
    assert final_batch > initial_batch
    assert batcher.optimizer.min_batch_size < final_batch < batcher.optimizer.max_batch_size
    # >= 30% more throughput than the static baseline.
    assert tail_tput >= 1.30 * static_tput, (
        f"tail throughput {tail_tput:.0f} < 1.3 * {static_tput:.0f}"
    )
    # No unsafe parking in the settled tail.
    assert max(series["cpu_percent"][-20:]) <= 90.0
    assert max(series["memory_percent"][-20:]) <= 90.0


def test_burst_traffic_pattern() -> None:
    """Pattern 2 — burst traffic.

    With a high burst probability the effective arrival rate spikes well above
    the base rate on some ticks (the 100->1000->100 behaviour from the spec).
    The loop must absorb the bursts without error, still end OPTIMIZING/STABLE,
    keep the batch within [min, max] throughout, and we confirm that real bursts
    actually occurred during the run.
    """
    base_rate = 200.0
    batcher, _ = _batcher(
        messages_per_second=base_rate,
        burst_probability=0.5,
        burst_multiplier=10.0,
        seed=1,
    )

    saw_burst = False
    min_batch = batcher.optimizer.max_batch_size
    max_batch = batcher.optimizer.min_batch_size
    for i in range(120):
        batcher.tick(timestamp=float(i), interval=1.0)
        # current_rate() reflects whether the just-processed interval was a burst.
        if batcher.load_simulator.current_rate() > base_rate:
            saw_burst = True
        size = batcher.optimizer.batch_size
        min_batch = min(min_batch, size)
        max_batch = max(max_batch, size)

    print(
        f"\n[burst] final={batcher.optimizer.batch_size} "
        f"state={batcher.state_machine.state.value} saw_burst={saw_burst} "
        f"batch_range=[{min_batch},{max_batch}]"
    )

    # Bursts actually fired during the run.
    assert saw_burst, "no burst interval was ever generated"
    # Survived the bursts and ended in an active/settled state.
    assert batcher.state_machine.state in _SETTLED
    # Batch stayed inside the feasible region the entire time.
    assert batcher.optimizer.min_batch_size <= min_batch
    assert max_batch <= batcher.optimizer.max_batch_size


def test_resource_constraint_pattern() -> None:
    """Pattern 3 — resource constraint.

    After the loop is actively optimizing, tightening the CPU constraint
    threshold below the current simulated pressure must trip EMERGENCY on the
    next tick and slash the batch size below its pre-constraint value (floored
    at min_batch_size).
    """
    batcher, _ = _batcher(messages_per_second=200.0, burst_probability=0.0, seed=0)

    # Run a stretch so we leave LEARNING and are actively optimizing.
    for i in range(30):
        batcher.tick(timestamp=float(i), interval=1.0)
    assert batcher.state_machine.state is OptimizerState.OPTIMIZING
    pre_batch = batcher.optimizer.batch_size

    # Force the simulated CPU pressure (already well above 5%) to breach.
    batcher.apply_config(OptimizerConfigUpdate(cpu_constraint_threshold=5.0))
    for i in range(3):
        batcher.tick(timestamp=float(30 + i), interval=1.0)

    print(
        f"\n[constraint] pre_batch={pre_batch} "
        f"state={batcher.state_machine.state.value} "
        f"batch={batcher.optimizer.batch_size}"
    )

    assert batcher.state_machine.state is OptimizerState.EMERGENCY
    # Batch was reduced below the pre-constraint value (and never below the floor).
    assert batcher.optimizer.batch_size < pre_batch
    assert batcher.optimizer.batch_size >= batcher.optimizer.min_batch_size


def test_recovery_pattern() -> None:
    """Pattern 4 — recovery (gradual re-optimization after a sustained emergency).

    Drives a *sustained* emergency: the CPU constraint is held tight long enough
    for the optimizer to slam the batch all the way down to (or near) the floor.
    Relaxing the constraint must then let recovery hysteresis clear, the loop
    resume OPTIMIZING, and — crucially — the batch climb back up substantially
    toward the interior optimum rather than latching STABLE at the floor.

    This is the regression guard for the "fresh window after emergency" fix in
    :meth:`AdaptiveBatcher._is_stable`: without it, the flat batch trace left by
    the sustained emergency would trip STABLE the instant the loop resumed,
    freezing the batch at ``min_batch_size`` and violating the spec's "gradual
    re-optimization on recovery" criterion.
    """
    batcher, _ = _batcher(messages_per_second=200.0, burst_probability=0.0, seed=0)
    floor = batcher.optimizer.min_batch_size

    # Drive into active optimization.
    for i in range(30):
        batcher.tick(timestamp=float(i), interval=1.0)
    assert batcher.state_machine.state is OptimizerState.OPTIMIZING
    pre_constraint_batch = batcher.optimizer.batch_size

    # Hold a tight constraint for a sustained run so the batch is repeatedly
    # halved down to (or very near) the floor — the worst case for STABLE latch.
    batcher.apply_config(OptimizerConfigUpdate(cpu_constraint_threshold=5.0))
    for i in range(10):
        batcher.tick(timestamp=float(30 + i), interval=1.0)
    assert batcher.state_machine.state is OptimizerState.EMERGENCY
    emergency_batch = batcher.optimizer.batch_size
    # The batch was driven down hard — to the floor (within a small margin) and
    # far below where it started.
    assert emergency_batch <= floor + 5, (
        f"sustained emergency did not reach the floor: {emergency_batch} > {floor}+5"
    )
    assert emergency_batch < pre_constraint_batch

    # Relax the constraint and run well past recovery_cycles (3) + stable_window
    # plus a generous margin so the re-climb has room to develop.
    batcher.apply_config(OptimizerConfigUpdate(cpu_constraint_threshold=90.0))
    saw_optimizing = False
    max_batch_after = emergency_batch
    for i in range(60):
        batcher.tick(timestamp=float(40 + i), interval=1.0)
        if batcher.state_machine.state is OptimizerState.OPTIMIZING:
            saw_optimizing = True
        max_batch_after = max(max_batch_after, batcher.optimizer.batch_size)

    final_batch = batcher.optimizer.batch_size
    print(
        f"\n[recovery] pre_constraint_batch={pre_constraint_batch} "
        f"emergency_batch={emergency_batch} "
        f"final_state={batcher.state_machine.state.value} "
        f"final_batch={final_batch} max_batch_after={max_batch_after} "
        f"saw_optimizing={saw_optimizing}"
    )

    # Recovery hysteresis cleared and the loop genuinely re-optimized.
    assert saw_optimizing, "loop never returned to OPTIMIZING after recovery"
    assert batcher.state_machine.state in _SETTLED
    # Robust climb-back: the batch re-optimized to at least 2x the emergency
    # floor, heading back toward the interior optimum (not frozen at the floor).
    assert final_batch >= 2 * emergency_batch, (
        f"batch did not climb back: final {final_batch} < 2 * floor {emergency_batch}"
    )
    assert batcher.optimizer.min_batch_size < final_batch < batcher.optimizer.max_batch_size

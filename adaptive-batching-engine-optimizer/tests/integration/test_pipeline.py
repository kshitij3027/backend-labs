"""Integration tests for the full AdaptiveBatcher control loop.

These exercise the *real* components wired together — a noise-free
:class:`BatchProcessor`, a seeded burst-free :class:`LoadSimulator`, a real
:class:`OptimizationEngine`, :class:`ConstraintHandler`, and :class:`StateMachine`
— with only the host resource monitor replaced by a fixed :class:`FakeMonitor`
so emergencies are driven by simulated workload pressure, never the real host.

Every run is deterministic: fixed seed, fixed host reading, explicit
``timestamp``/``interval`` per tick. No sleeps, no network. Each scenario prints
its observed trajectory (run with ``-s``) so the orchestrator can see real
numbers, then asserts generous-tolerance behavioural properties.
"""

from __future__ import annotations

import random

import pytest

from src.batcher import AdaptiveBatcher
from src.constraints import ConstraintHandler
from src.loadsim import LoadSimulator
from src.metrics import MetricsCollector, ResourceReading
from src.models import OptimizerState
from src.optimizer import OptimizationEngine
from src.processor import BatchProcessor
from src.states import StateMachine


class FakeMonitor:
    """Fixed-reading resource monitor (keeps the real host out of the loop)."""

    def __init__(self, cpu: float = 5.0, mem: float = 40.0, avail: float = 8000.0) -> None:
        self._r = ResourceReading(cpu, mem, avail)

    def sample(self) -> ResourceReading:
        return self._r


def _real_batcher(*, initial_batch_size: int = 100, monitor: FakeMonitor | None = None) -> AdaptiveBatcher:
    """Build an AdaptiveBatcher from real, deterministic components."""
    return AdaptiveBatcher(
        processor=BatchProcessor(noise_std=0.0),
        load_simulator=LoadSimulator(
            messages_per_second=100, burst_probability=0.0, rng=random.Random(0)
        ),
        optimizer=OptimizationEngine(initial_batch_size=initial_batch_size),
        constraints=ConstraintHandler(),
        state_machine=StateMachine(),
        resource_monitor=monitor or FakeMonitor(cpu=5.0, mem=40.0),
        collector=MetricsCollector(),
    )


def _unique_transitions(states: list[OptimizerState]) -> list[str]:
    """Collapse a per-tick state list into the sequence of unique transitions."""
    seq: list[str] = []
    for s in states:
        if not seq or seq[-1] != s.value:
            seq.append(s.value)
    return seq


# --- Climb & improve --------------------------------------------------------


def test_steady_load_climbs_and_improves_throughput(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Over a steady load the loop leaves LEARNING, reaches OPTIMIZING, and the
    batch climbs from 100 into an interior mid-range with >= 30% more throughput.
    """
    b = _real_batcher(initial_batch_size=100, monitor=FakeMonitor(cpu=5.0, mem=40.0))
    proc = BatchProcessor(noise_std=0.0)  # reference noise-free curve

    initial_batch = b.optimizer.batch_size
    assert b.state_machine.state is OptimizerState.LEARNING

    states: list[OptimizerState] = []
    saw_optimizing = False
    for i in range(120):
        b.tick(timestamp=i * 1.0, interval=1.0)
        states.append(b.state_machine.state)
        if b.state_machine.state is OptimizerState.OPTIMIZING:
            saw_optimizing = True

    final_batch = b.optimizer.batch_size
    init_tput = proc.throughput_for(initial_batch)
    final_tput = proc.throughput_for(final_batch)
    improvement_pct = (final_tput / init_tput - 1.0) * 100.0
    transitions = _unique_transitions(states)

    print(
        f"\n[climb] initial_batch={initial_batch} final_batch={final_batch} "
        f"final_state={b.state_machine.state.value} "
        f"improvement={improvement_pct:.1f}% "
        f"init_tput={init_tput:.0f} final_tput={final_tput:.0f} "
        f"state_sequence={transitions}"
    )

    # Reached active optimization at some point past LEARNING.
    assert saw_optimizing, transitions
    # Interior mid-range: climbed off the initial seed, strictly inside bounds.
    assert b.optimizer.min_batch_size < final_batch < b.optimizer.max_batch_size
    assert final_batch > initial_batch
    # >= 30% throughput improvement at the final batch vs the initial batch.
    assert final_tput >= 1.30 * init_tput, (
        f"only {improvement_pct:.1f}% improvement; init={init_tput:.0f} final={final_tput:.0f}"
    )


# --- Emergency -> recovery --------------------------------------------------


def test_emergency_then_recovery(capsys: pytest.CaptureFixture[str]) -> None:
    """Forcing a very large batch trips EMERGENCY (batch slashed); a run of healthy
    ticks then satisfies recovery hysteresis and the loop resumes OPTIMIZING with
    the batch pulled back down toward the interior optimum.
    """
    b = _real_batcher(initial_batch_size=100, monitor=FakeMonitor(cpu=5.0, mem=40.0))

    # Force a dangerously large batch and measure it once.
    b.optimizer.set_batch_size(4800)
    forced = b.optimizer.batch_size

    trajectory: list[tuple[int, str, float]] = []  # (measured_batch, state, cpu)

    snap = b.tick(timestamp=0.0, interval=1.0)
    trajectory.append((snap.batch_size, b.state_machine.state.value, snap.cpu_percent))

    assert b.state_machine.state is OptimizerState.EMERGENCY
    # The breaching batch (4800) was measured; optimizer was slashed below it.
    assert snap.batch_size == forced
    assert b.optimizer.batch_size < forced

    # Now run a stretch of healthy ticks; recovery hysteresis should re-arm.
    healthy_cpu_max = 0.0
    healthy_mem_max = 0.0
    for i in range(15):
        snap = b.tick(timestamp=(i + 1) * 1.0, interval=1.0)
        trajectory.append((snap.batch_size, b.state_machine.state.value, snap.cpu_percent))
        healthy_cpu_max = max(healthy_cpu_max, snap.cpu_percent)
        healthy_mem_max = max(healthy_mem_max, snap.memory_percent)

    final_batch = b.optimizer.batch_size

    print(
        f"\n[emergency->recovery] forced_batch={forced} final_batch={final_batch} "
        f"final_state={b.state_machine.state.value} "
        f"healthy_cpu_max={healthy_cpu_max:.1f} healthy_mem_max={healthy_mem_max:.1f}\n"
        f"  trajectory (measured_batch, state, cpu%):"
    )
    for batch, state, cpu in trajectory:
        print(f"    {batch:>5}  {state:<10}  cpu={cpu:.1f}")

    # Recovered out of EMERGENCY back into active optimization.
    assert b.state_machine.state is OptimizerState.OPTIMIZING
    # Batch was pulled far down from the forced value toward the interior optimum.
    assert final_batch < forced
    assert b.optimizer.min_batch_size < final_batch < b.optimizer.max_batch_size
    # No constraint breach during the healthy recovery phase (excludes the first
    # deliberately-breaching tick).
    assert healthy_cpu_max <= 90.0
    assert healthy_mem_max <= 90.0


# --- No constraint violations in steady state -------------------------------


def test_no_unsafe_region_near_optimum(capsys: pytest.CaptureFixture[str]) -> None:
    """Once settled near the optimum the optimizer never parks in an unsafe region.

    Over the steady-load climb, the tail of snapshots (after the loop has left
    LEARNING and converged) must never show cpu/memory above 90%.
    """
    b = _real_batcher(initial_batch_size=100, monitor=FakeMonitor(cpu=5.0, mem=40.0))

    for i in range(120):
        b.tick(timestamp=i * 1.0, interval=1.0)

    # Inspect the settled tail (well past the 5-sample LEARNING warm-up).
    tail = b.collector.snapshot(60)
    max_cpu = max(s.cpu_percent for s in tail)
    max_mem = max(s.memory_percent for s in tail)

    print(
        f"\n[steady-state safety] tail_len={len(tail)} "
        f"max_cpu={max_cpu:.1f} max_mem={max_mem:.1f} "
        f"final_batch={b.optimizer.batch_size}"
    )

    assert max_cpu <= 90.0, f"parked in unsafe CPU region: {max_cpu:.1f}%"
    assert max_mem <= 90.0, f"parked in unsafe memory region: {max_mem:.1f}%"
    # Sanity: the loop did not collapse to the floor or peg the ceiling.
    assert b.optimizer.min_batch_size < b.optimizer.batch_size < b.optimizer.max_batch_size

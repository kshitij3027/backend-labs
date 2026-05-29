"""Deterministic demonstration of adaptive-vs-static batching throughput.

This standalone script proves the headline success criterion from the spec
(Feature Area A): the self-tuning :class:`~src.batcher.AdaptiveBatcher` delivers
a **30%+ throughput improvement over static batching**. It runs entirely
in-process with no host/network/time dependence:

* the cost model has noise disabled (``noise_std=0.0``),
* the load simulator uses a *seeded* RNG with bursts off,
* the resource monitor is a fixed :class:`FakeMonitor` (the real host never
  enters the loop, so the numbers are reproducible on any machine).

The comparison is intentionally conservative. The *static baseline* is the naive
default fixed batch — ``settings.initial_batch_size`` (100) — and the headline
assertion is made against ``processor.throughput_for(100)``. The adaptive figure
is the mean noise-free throughput over the converged tail of the run. A sweep of
fixed batch sizes and the analytic optimum ``B*`` are printed for context.

Run it inside the tester container::

    python scripts/improvement.py          # exit 0 iff improvement >= 30%

or import :func:`measure_improvement` from a test to assert on the same numbers.
"""

from __future__ import annotations

import random
import sys

from src.batcher import AdaptiveBatcher
from src.loadsim import LoadSimulator
from src.metrics import ResourceReading
from src.processor import BatchProcessor
from src.settings import get_settings

# Tunables for the demonstration run. Kept module-level so the test and the CLI
# share exactly one definition of "the experiment".
ADAPTIVE_TICKS = 150  # control-loop iterations before measuring convergence
TAIL_WINDOW = 20  # snapshots averaged to characterise the converged throughput
STEADY_RATE = 200.0  # msg/s fed to the adaptive loop (steady, burst-free)
TARGET_IMPROVEMENT = 0.30  # success threshold from the spec (30%+)
SWEEP = (50, 100, 200, 400, 800)  # fixed batch sizes shown for context


class FakeMonitor:
    """Fixed-reading resource monitor that keeps the real host out of the loop.

    Returns the same :class:`~src.metrics.ResourceReading` on every
    :meth:`sample`, so emergencies are driven only by simulated workload
    pressure and the run is byte-for-byte reproducible.
    """

    def __init__(self, cpu: float = 5.0, mem: float = 40.0, avail: float = 8000.0) -> None:
        self._r = ResourceReading(cpu, mem, avail)

    def sample(self) -> ResourceReading:
        return self._r


def measure_improvement() -> dict:
    """Run the deterministic adaptive-vs-static experiment and return the result.

    Builds a noise-free processor, drives an :class:`AdaptiveBatcher` over a
    steady seeded load for :data:`ADAPTIVE_TICKS` ticks, then compares the mean
    throughput of the converged tail against the static baseline at batch 100.

    Returns:
        A dict with keys: ``converged_batch`` (int), ``adaptive_throughput``
        (float, mean over the last :data:`TAIL_WINDOW` snapshots),
        ``static_batch`` (int, the baseline batch = 100),
        ``static_throughput`` (float, ``throughput_for(100)``),
        ``improvement`` (float fraction, e.g. ``0.578`` for +57.8%),
        ``optimal_batch`` (float, the analytic ``B*``), and
        ``sweep`` (dict mapping each fixed batch size to its noise-free
        throughput).
    """
    proc = BatchProcessor(noise_std=0.0)

    # --- Adaptive: let the control loop discover the batch size on its own. ---
    batcher = AdaptiveBatcher(
        processor=proc,
        load_simulator=LoadSimulator(
            messages_per_second=STEADY_RATE,
            burst_probability=0.0,
            rng=random.Random(0),
        ),
        resource_monitor=FakeMonitor(),
    )
    for i in range(ADAPTIVE_TICKS):
        batcher.tick(timestamp=float(i), interval=1.0)

    series = batcher.metrics_series()
    tail = series["throughput"][-TAIL_WINDOW:]
    adaptive_throughput = sum(tail) / len(tail)
    converged_batch = batcher.optimizer.batch_size

    # --- Static baseline: the naive fixed default batch (settings = 100). ---
    static_batch = get_settings().initial_batch_size
    static_throughput = proc.throughput_for(static_batch)

    improvement = (adaptive_throughput - static_throughput) / static_throughput

    return {
        "converged_batch": converged_batch,
        "adaptive_throughput": adaptive_throughput,
        "static_batch": static_batch,
        "static_throughput": static_throughput,
        "improvement": improvement,
        "optimal_batch": proc.optimal_batch_size(),
        "sweep": {b: proc.throughput_for(b) for b in SWEEP},
    }


def _print_report(result: dict) -> None:
    """Print a human-readable summary of :func:`measure_improvement`'s result."""
    print("=" * 64)
    print("Adaptive vs. Static Batching — Throughput Improvement")
    print("=" * 64)
    print(f"  analytic optimum  B* = {result['optimal_batch']:.1f} records")
    print(f"  converged batch      = {result['converged_batch']} records")
    print(
        f"  adaptive throughput  = {result['adaptive_throughput']:.1f} rec/s "
        f"(mean of last {TAIL_WINDOW} ticks)"
    )
    print(
        f"  static  throughput   = {result['static_throughput']:.1f} rec/s "
        f"(fixed batch = {result['static_batch']})"
    )
    print(f"  improvement          = {result['improvement'] * 100:.1f}%")
    print("-" * 64)
    print("  fixed-batch sweep (noise-free throughput_for):")
    for batch, tput in result["sweep"].items():
        print(f"    batch={batch:>5}  ->  {tput:>10.1f} rec/s")
    print("=" * 64)


def main() -> int:
    """Print the report and return ``0`` iff the 30% target is met."""
    result = measure_improvement()
    _print_report(result)

    if result["improvement"] >= TARGET_IMPROVEMENT:
        print(
            f"IMPROVEMENT OK: {result['improvement'] * 100:.1f}% "
            f">= {TARGET_IMPROVEMENT * 100:.0f}% target"
        )
        return 0

    print(
        f"IMPROVEMENT FAIL: {result['improvement'] * 100:.1f}% "
        f"< {TARGET_IMPROVEMENT * 100:.0f}% target"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())

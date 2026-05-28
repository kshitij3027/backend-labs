"""Simulated batch-processing cost model with a concave throughput curve.

This module fakes the work of shipping a batch of log records to a downstream
sink. It never touches CPU, memory, disk, or the network — every figure is a
cheap closed-form arithmetic expression — yet it reproduces the *shape* of a
real batched pipeline so the adaptive control loop has something meaningful to
climb.

Throughput vs. batch size — why it is concave
---------------------------------------------
Per-batch processing time is modelled as::

    time(B) = overhead_ms + per_record_ms * B + saturation_coeff * B**2

and throughput is ``B / (time(B) / 1000)`` records per second.

* For **small** batches the fixed ``overhead_ms`` (serialization / RPC / framing)
  dominates and is amortised over too few records, so throughput is poor.
* As ``B`` grows the overhead is spread across more records and throughput
  *rises*.
* The quadratic ``saturation_coeff * B**2`` term models memory pressure / GC /
  cache thrashing that makes very large batches disproportionately expensive,
  so throughput eventually *declines*.

The result is a strictly concave ``T(B)`` (rise → peak → decline) whose interior
maximum sits at ``B* = sqrt(overhead_ms / saturation_coeff)`` (found by setting
``dT/dB = 0``). With the defaults below ``B* = sqrt(5.0 / 8e-6) ≈ 790`` records,
comfortably inside the configured ``[min_batch_size, max_batch_size]`` window.

Simulated resource pressure
----------------------------
``cpu_pressure`` and ``mem_pressure`` are **purely simulated** values in ``0..100``
that rise monotonically with batch size (and, for CPU, slightly with the incoming
message rate to mimic a growing backlog). They let the optimizer's EMERGENCY
state be exercised — pressure approaches/exceeds the 90% thresholds near
``max_batch_size`` — *without* placing any real load on the host machine.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from src.settings import get_settings


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Immutable outcome of simulating one batch through :class:`BatchProcessor`.

    Attributes:
        batch_size: Number of records in the simulated batch.
        throughput: Records processed per second for this batch.
        latency_ms: Per-batch processing time in milliseconds.
        cpu_pressure: Simulated CPU contribution in ``0..100`` (not real load).
        mem_pressure: Simulated memory contribution in ``0..100`` (not real load).
    """

    batch_size: int
    throughput: float
    latency_ms: float
    cpu_pressure: float
    mem_pressure: float


class BatchProcessor:
    """Closed-form, side-effect-free model of a batched log-shipping stage.

    The cost model is concave in batch size, giving the adaptive optimizer a
    well-defined interior optimum to discover. All parameters are constructor
    overridable so tests can reshape the curve or disable noise for determinism.

    Args:
        overhead_ms: Fixed per-batch cost (serialization / RPC / framing), in ms.
        per_record_ms: Marginal processing cost per record, in ms.
        saturation_coeff: Quadratic coefficient modelling memory/GC pressure on
            large batches. Larger values push the throughput peak
            ``B* = sqrt(overhead_ms / saturation_coeff)`` toward smaller batches.
        noise_std: Relative standard deviation of multiplicative Gaussian noise
            applied to processing time. ``0.0`` makes the model deterministic;
            production uses a small value (e.g. ``0.03``).
        rng: Injectable :class:`random.Random` for reproducible noise. Defaults
            to a fresh, unseeded ``random.Random()``.
    """

    def __init__(
        self,
        overhead_ms: float = 5.0,
        per_record_ms: float = 0.05,
        saturation_coeff: float = 8e-6,
        noise_std: float = 0.0,
        rng: random.Random | None = None,
    ) -> None:
        self.overhead_ms = overhead_ms
        self.per_record_ms = per_record_ms
        self.saturation_coeff = saturation_coeff
        self.noise_std = noise_std
        self._rng = rng if rng is not None else random.Random()
        # Reference scale for the simulated pressure curves; read once so a later
        # settings change cannot retune a live processor mid-run.
        self._max_batch_size: int = get_settings().max_batch_size

    def _processing_time_ms(self, batch_size: int, *, with_noise: bool) -> float:
        """Return the (optionally noisy) processing time in ms, floored at 0.001."""
        time_ms = (
            self.overhead_ms
            + batch_size * self.per_record_ms
            + self.saturation_coeff * batch_size**2
        )
        if with_noise and self.noise_std > 0.0:
            time_ms *= 1.0 + self._rng.gauss(0.0, self.noise_std)
        # Clamp to a small positive minimum so throughput stays finite even if
        # noise drives the multiplier negative.
        return max(time_ms, 0.001)

    def _cpu_pressure(self, batch_size: int, messages_per_second: float) -> float:
        """Simulated CPU load (0..100): concave-in-B with a small backlog term.

        Batch size is the dominant driver via a ``(B / max_batch_size) ** 0.6``
        curve (sub-linear, so pressure climbs quickly then eases toward the cap).
        The incoming rate adds a tiny backlog contribution. The result is clamped
        to ``[0, 100]``; near ``max_batch_size`` it sits above 90.
        """
        ratio = max(0.0, batch_size / self._max_batch_size)
        base = 100.0 * ratio**0.6
        # Backlog effect: keep it small so batch size stays dominant. A rate of
        # 1000 msg/s adds ~5 points; ordinary rates add a fraction of a point.
        backlog = 0.005 * max(0.0, messages_per_second)
        return min(100.0, base + backlog)

    def _mem_pressure(self, batch_size: int) -> float:
        """Simulated memory load (0..100): linear in batch size, clamped at 100.

        A full ``max_batch_size`` batch occupies ~100% of the simulated buffer,
        so this reaches the 90% threshold a little below the maximum.
        """
        ratio = max(0.0, batch_size / self._max_batch_size)
        return min(100.0, 100.0 * ratio)

    def process_batch(
        self, batch_size: int, messages_per_second: float = 0.0
    ) -> ProcessResult:
        """Simulate processing one batch and return the resulting metrics.

        Args:
            batch_size: Records in the batch (the value the optimizer tunes).
            messages_per_second: Current incoming rate; adds a small backlog
                term to the simulated CPU pressure only.

        Returns:
            A :class:`ProcessResult` with throughput, latency, and simulated
            CPU/memory pressure. No real system resources are consumed.
        """
        processing_time_ms = self._processing_time_ms(batch_size, with_noise=True)
        latency_ms = processing_time_ms
        throughput = batch_size / (processing_time_ms / 1000.0)
        return ProcessResult(
            batch_size=batch_size,
            throughput=throughput,
            latency_ms=latency_ms,
            cpu_pressure=self._cpu_pressure(batch_size, messages_per_second),
            mem_pressure=self._mem_pressure(batch_size),
        )

    def throughput_for(self, batch_size: int) -> float:
        """Return the deterministic, noise-free throughput for ``batch_size``.

        Useful for tests and the static-vs-adaptive comparison: it ignores
        ``noise_std`` entirely, yielding the underlying concave ``T(B)`` curve.
        """
        time_ms = self._processing_time_ms(batch_size, with_noise=False)
        return batch_size / (time_ms / 1000.0)

    def optimal_batch_size(self) -> float:
        """Return the analytic throughput-maximising batch size ``B*``.

        Derived from ``dT/dB = 0``: ``B* = sqrt(overhead_ms / saturation_coeff)``.
        Returns ``inf`` if ``saturation_coeff`` is zero (no interior optimum).
        """
        if self.saturation_coeff <= 0.0:
            return math.inf
        return math.sqrt(self.overhead_ms / self.saturation_coeff)

"""Hard safety constraints and recovery hysteresis for the control loop.

This module is the optimizer's safety layer. It enforces *hard* resource
limits — CPU %, memory %, and per-batch latency — that, when breached, must
immediately drive the state machine into ``EMERGENCY`` and shrink the batch
size. Breach checks are deliberately blunt: any single metric exceeding its
threshold trips the whole constraint.

Leaving ``EMERGENCY`` is governed by *hysteresis* rather than a simple inverse
of the breach test. Recovery requires ``recovery_cycles`` consecutive healthy
cycles in which every metric sits below its dedicated *recovery* threshold.
Those recovery thresholds are set well below the breach thresholds (e.g. CPU
70% vs. 90%), creating a dead band so the system cannot flap rapidly between
``EMERGENCY`` and ``OPTIMIZING`` when a metric hovers near a single limit.

The handler is pure and synchronous: callers feed it observed metrics, it
returns immutable status objects and a healthy-streak counter, and the
``AdaptiveBatcher`` translates that into state transitions.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.settings import get_settings


@dataclass(frozen=True, slots=True)
class ConstraintStatus:
    """Immutable result of a single constraint evaluation.

    ``breach`` is the logical OR of the three per-metric breach flags. ``reason``
    is a compact, human-readable explanation suitable for logs, decision
    records, and the dashboard (e.g. ``"cpu 95.0%>90.0%; latency 1200ms>1000ms"``
    or ``"ok"`` when nothing is breached).
    """

    breach: bool
    cpu_breach: bool
    memory_breach: bool
    latency_breach: bool
    reason: str


class ConstraintHandler:
    """Enforce hard safety limits and track recovery hysteresis.

    Thresholds default to the process-wide :func:`get_settings` values when not
    supplied. Two threshold families are tracked: the *breach* thresholds that
    trip ``EMERGENCY`` and the lower *recovery* thresholds that, sustained over
    ``recovery_cycles`` consecutive healthy cycles, permit leaving it.
    """

    def __init__(
        self,
        *,
        cpu_threshold: float | None = None,
        memory_threshold: float | None = None,
        latency_threshold: float | None = None,
        recovery_cpu: float | None = None,
        recovery_memory: float | None = None,
        recovery_latency: float | None = None,
        recovery_cycles: int | None = None,
        min_batch_size: int | None = None,
        emergency_reduction_factor: float = 0.5,
    ) -> None:
        settings = get_settings()
        self.cpu_threshold = (
            cpu_threshold if cpu_threshold is not None else settings.cpu_constraint_threshold
        )
        self.memory_threshold = (
            memory_threshold
            if memory_threshold is not None
            else settings.memory_constraint_threshold
        )
        self.latency_threshold = (
            latency_threshold
            if latency_threshold is not None
            else settings.latency_constraint_threshold
        )
        self.recovery_cpu = (
            recovery_cpu if recovery_cpu is not None else settings.recovery_cpu_threshold
        )
        self.recovery_memory = (
            recovery_memory
            if recovery_memory is not None
            else settings.recovery_memory_threshold
        )
        self.recovery_latency = (
            recovery_latency
            if recovery_latency is not None
            else settings.recovery_latency_threshold
        )
        self.recovery_cycles = (
            recovery_cycles if recovery_cycles is not None else settings.recovery_cycles
        )
        self.min_batch_size = (
            min_batch_size if min_batch_size is not None else settings.min_batch_size
        )
        self.emergency_reduction_factor = emergency_reduction_factor
        self._healthy_streak = 0

    def check(
        self, cpu_percent: float, memory_percent: float, latency_ms: float
    ) -> ConstraintStatus:
        """Evaluate the three hard limits and return an immutable status.

        A metric breaches when it is *strictly greater than* its threshold. The
        overall ``breach`` flag is true if any individual metric breaches.
        """
        cpu_breach = cpu_percent > self.cpu_threshold
        memory_breach = memory_percent > self.memory_threshold
        latency_breach = latency_ms > self.latency_threshold
        breach = cpu_breach or memory_breach or latency_breach

        if not breach:
            reason = "ok"
        else:
            parts: list[str] = []
            if cpu_breach:
                parts.append(f"cpu {cpu_percent:.1f}%>{self.cpu_threshold:.1f}%")
            if memory_breach:
                parts.append(f"memory {memory_percent:.1f}%>{self.memory_threshold:.1f}%")
            if latency_breach:
                parts.append(
                    f"latency {latency_ms:.0f}ms>{self.latency_threshold:.0f}ms"
                )
            reason = "; ".join(parts)

        return ConstraintStatus(
            breach=breach,
            cpu_breach=cpu_breach,
            memory_breach=memory_breach,
            latency_breach=latency_breach,
            reason=reason,
        )

    def is_breach(
        self, cpu_percent: float, memory_percent: float, latency_ms: float
    ) -> bool:
        """Convenience predicate: ``True`` if any hard limit is breached."""
        return self.check(cpu_percent, memory_percent, latency_ms).breach

    def emergency_batch_size(self, current_batch: int) -> int:
        """Return the hard, immediate batch-size reduction for an emergency.

        Halves (by default) the current batch but never drops below
        ``min_batch_size``. This is a step change, not a smoothed update.
        """
        return max(
            self.min_batch_size, int(current_batch * self.emergency_reduction_factor)
        )

    def note_cycle(
        self, cpu_percent: float, memory_percent: float, latency_ms: float
    ) -> None:
        """Record one cycle toward recovery hysteresis.

        Increments the healthy streak only when *all* metrics sit below their
        (lower) recovery thresholds; any non-healthy cycle resets the streak to
        zero, so recovery requires an uninterrupted run of healthy cycles.
        """
        healthy = (
            cpu_percent < self.recovery_cpu
            and memory_percent < self.recovery_memory
            and latency_ms < self.recovery_latency
        )
        if healthy:
            self._healthy_streak += 1
        else:
            self._healthy_streak = 0

    def recovery_ready(self) -> bool:
        """Return ``True`` once enough consecutive healthy cycles have elapsed."""
        return self._healthy_streak >= self.recovery_cycles

    def reset(self) -> None:
        """Clear the healthy streak (e.g. on optimizer reset)."""
        self._healthy_streak = 0

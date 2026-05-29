"""Online gradient-ascent controller that tunes batch size.

The optimizer treats batch size ``B`` as a single decision variable and climbs a
*noisy, concave* multi-objective utility ``U(B)`` that trades raw throughput
against per-batch latency. It never sees the cost model directly: each tick it is
handed the throughput and latency that were *measured at the current* ``B`` and
returns the next ``B`` to try.

Algorithm — hill-climbing with direction memory
------------------------------------------------
Because only a scalar utility is observed (not its analytic derivative), the
engine estimates the slope with a one-step finite difference and remembers which
way it last moved:

* **Direction memory.** If the most recent move *raised* utility we keep heading
  the same way; if it *lowered* utility we flip direction. This is the classic
  "keep going while it helps" heuristic that walks a unimodal/concave curve
  toward its peak and then dithers around it.
* **Multiplicative step.** The raw next batch is ``B * increase_factor`` when
  climbing up and ``B * decrease_factor`` when backing off. Multiplicative steps
  are scale-free, so the controller moves in proportionally sized jumps whether
  ``B`` is 50 or 5000.
* **Exponential smoothing.** The raw target is blended with the current ``B``
  (``smoothing_alpha`` weight on the new value) so a single noisy sample cannot
  yank the batch size around — this damps oscillation near the optimum.
* **Clamping (projection).** The smoothed value is projected back into the
  feasible region ``[min_batch_size, max_batch_size]`` and rounded to an int.

The finite-difference slope ``dU/dB`` is exposed as :attr:`last_gradient`; a
higher-level state machine uses it to decide when the loop has gone STABLE.

Performance
-----------
:meth:`update` is pure, synchronous, and O(1) — a fixed handful of float
operations with no allocation in the hot path — comfortably under the spec's
10ms-per-calculation budget.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.models import OptimizerConfigUpdate
from src.settings import get_settings


@dataclass(frozen=True, slots=True)
class OptimizerStep:
    """Immutable record of a single gradient-ascent decision.

    Attributes:
        old_batch_size: Batch size in effect when this step was computed.
        new_batch_size: Batch size chosen for the next tick (clamped, integer).
        gradient: Finite-difference slope ``dU/dB`` estimated from the last move
            (``0.0`` on the very first step, when no prior sample exists).
        utility: Multi-objective utility measured at ``old_batch_size``.
        direction: Current climb direction, ``+1`` (probe larger) or ``-1``
            (probe smaller), after applying this step's direction memory.
    """

    old_batch_size: int
    new_batch_size: int
    gradient: float
    utility: float
    direction: int


class OptimizationEngine:
    """Gradient-ascent hill-climber for adaptive batch-size tuning.

    Combines direction memory, multiplicative probe factors, exponential
    smoothing, and feasible-region clamping into a single O(1) update step that
    maximizes a throughput-vs-latency utility. All dynamics parameters are stored
    as mutable instance attributes so they can be retuned at runtime via
    :meth:`apply_config`.

    Args:
        initial_batch_size: Starting batch size; defaults to the configured seed.
        min_batch_size: Lower clamp bound; defaults to settings.
        max_batch_size: Upper clamp bound; defaults to settings.
        smoothing_alpha: Weight (``0..1``) placed on the new target when blending
            with the current batch size. Higher = more responsive, more jittery.
        increase_factor: Multiplier applied when climbing upward (``> 1``).
        decrease_factor: Multiplier applied when backing off (``< 1``).
        weight_throughput: Weight of the throughput term in the utility.
        weight_latency: Weight of the latency-benefit term in the utility.
        throughput_scale: Normaliser for throughput so the term sits near ``1``
            around the curve's peak.
        latency_scale: Reference latency (ms) for the diminishing-returns latency
            benefit ``1 / (1 + latency_ms / latency_scale)``.
    """

    def __init__(
        self,
        *,
        initial_batch_size: int | None = None,
        min_batch_size: int | None = None,
        max_batch_size: int | None = None,
        smoothing_alpha: float | None = None,
        increase_factor: float | None = None,
        decrease_factor: float | None = None,
        weight_throughput: float | None = None,
        weight_latency: float | None = None,
        throughput_scale: float = 20000.0,
        latency_scale: float = 100.0,
    ) -> None:
        settings = get_settings()

        # --- Dynamics parameters (mutable; retunable via apply_config) ---
        self.min_batch_size: int = (
            min_batch_size if min_batch_size is not None else settings.min_batch_size
        )
        self.max_batch_size: int = (
            max_batch_size if max_batch_size is not None else settings.max_batch_size
        )
        self.smoothing_alpha: float = (
            smoothing_alpha
            if smoothing_alpha is not None
            else settings.smoothing_alpha
        )
        self.increase_factor: float = (
            increase_factor
            if increase_factor is not None
            else settings.batch_increase_factor
        )
        self.decrease_factor: float = (
            decrease_factor
            if decrease_factor is not None
            else settings.batch_decrease_factor
        )
        self.weight_throughput: float = (
            weight_throughput
            if weight_throughput is not None
            else settings.weight_throughput
        )
        self.weight_latency: float = (
            weight_latency if weight_latency is not None else settings.weight_latency
        )
        self.throughput_scale: float = throughput_scale
        self.latency_scale: float = latency_scale

        # --- Seed for resets, then mutable control-loop state ---
        self._initial_batch_size: int = (
            initial_batch_size
            if initial_batch_size is not None
            else settings.initial_batch_size
        )
        self._batch_size: int = self._initial_batch_size
        self._prev_batch_size: int | None = None
        self._prev_utility: float | None = None
        self._direction: int = 1  # +1 probes larger batches, -1 probes smaller
        self._last_gradient: float = 0.0

    @property
    def batch_size(self) -> int:
        """Current batch size the optimizer recommends."""
        return self._batch_size

    @property
    def last_gradient(self) -> float:
        """Most recent finite-difference slope ``dU/dB`` (``0.0`` before any move)."""
        return self._last_gradient

    def compute_utility(self, throughput: float, latency_ms: float) -> float:
        """Scalarize throughput and latency into a single objective ``U``.

        Both terms are designed to live roughly in ``[0, 1]`` so the configured
        weights are comparable:

        * ``t_norm`` divides throughput by :attr:`throughput_scale`, chosen so the
          term approaches ``1`` near the throughput peak (it is *not* hard-clamped,
          so an unusually fast batch can score slightly above ``1``).
        * ``l_benefit`` is a diminishing-returns reward for low latency,
          ``1 / (1 + latency_ms / latency_scale)``: ``1`` at zero latency, ``0.5``
          at one ``latency_scale``, decaying toward ``0`` as latency grows.

        Args:
            throughput: Records processed per second at the current batch size.
            latency_ms: Per-batch processing latency, in milliseconds.

        Returns:
            The weighted-sum utility ``U`` to be maximized.
        """
        t_norm = throughput / self.throughput_scale
        l_benefit = 1.0 / (1.0 + latency_ms / self.latency_scale)
        return self.weight_throughput * t_norm + self.weight_latency * l_benefit

    def update(self, throughput: float, latency_ms: float) -> OptimizerStep:
        """Perform one gradient-ascent step and return the resulting decision.

        The supplied ``throughput`` and ``latency_ms`` are assumed to have been
        measured at the *current* :attr:`batch_size`. The method estimates the
        slope from the previous move, updates the climb direction, takes a
        smoothed multiplicative step, clamps it into the feasible region, and
        advances internal state.

        Args:
            throughput: Records per second measured at the current batch size.
            latency_ms: Per-batch latency (ms) measured at the current batch size.

        Returns:
            An :class:`OptimizerStep` describing the transition.
        """
        utility = self.compute_utility(throughput, latency_ms)

        if self._prev_utility is None:
            # First observation: no prior point to difference against. Leave the
            # gradient at zero and keep probing upward to gather a second sample.
            gradient = 0.0
        else:
            delta_b = self._batch_size - self._prev_batch_size
            delta_u = utility - self._prev_utility
            gradient = delta_u / delta_b if delta_b != 0 else 0.0
            if delta_u >= 0.0:
                # Last move helped (or was neutral) — keep heading the same way.
                pass
            else:
                # Last move hurt — reverse and climb back toward the peak.
                self._direction *= -1

        factor = self.increase_factor if self._direction > 0 else self.decrease_factor
        optimal = self._batch_size * factor

        # Exponential smoothing: blend the raw target with the current size so a
        # single noisy sample cannot jerk the batch size around.
        smoothed = (
            self._batch_size * (1 - self.smoothing_alpha)
            + optimal * self.smoothing_alpha
        )

        # Clamp (project) onto the feasible region and snap to an integer.
        new_batch = int(
            round(min(max(smoothed, self.min_batch_size), self.max_batch_size))
        )

        # Anti-stuck nudge: for small batches the smoothed step can round straight
        # back to the current integer, freezing the climb. If we have not actually
        # moved and there is still room in the chosen direction, force a minimal
        # one-record step so the hill-climber keeps making progress.
        if new_batch == self._batch_size:
            if self._direction > 0 and self._batch_size < self.max_batch_size:
                new_batch = self._batch_size + 1
            elif self._direction < 0 and self._batch_size > self.min_batch_size:
                new_batch = self._batch_size - 1

        old = self._batch_size
        self._prev_batch_size = self._batch_size
        self._prev_utility = utility
        self._batch_size = new_batch
        self._last_gradient = gradient

        return OptimizerStep(
            old_batch_size=old,
            new_batch_size=new_batch,
            gradient=gradient,
            utility=utility,
            direction=self._direction,
        )

    def reset(self) -> None:
        """Restore the engine to its initial batch size and clear climb history."""
        self._batch_size = self._initial_batch_size
        self._prev_batch_size = None
        self._prev_utility = None
        self._direction = 1
        self._last_gradient = 0.0

    def apply_config(self, update: OptimizerConfigUpdate) -> None:
        """Apply a partial reconfiguration of the parameters this engine owns.

        Only the fields the optimizer is responsible for are honoured; control-loop
        timing and safety thresholds (``optimization_interval`` and the various
        ``*_constraint_threshold`` values) are ignored here, as they belong to the
        surrounding state machine. After applying any new bounds, the current batch
        size is re-clamped so it never sits outside the (possibly narrowed) window.

        Args:
            update: Patch object whose non-``None`` fields are applied in place.
        """
        if update.smoothing_alpha is not None:
            self.smoothing_alpha = update.smoothing_alpha
        if update.min_batch_size is not None:
            self.min_batch_size = update.min_batch_size
        if update.max_batch_size is not None:
            self.max_batch_size = update.max_batch_size
        if update.batch_increase_factor is not None:
            self.increase_factor = update.batch_increase_factor
        if update.batch_decrease_factor is not None:
            self.decrease_factor = update.batch_decrease_factor
        if update.weight_throughput is not None:
            self.weight_throughput = update.weight_throughput
        if update.weight_latency is not None:
            self.weight_latency = update.weight_latency

        # Re-project the current batch into the (possibly new) feasible region.
        self._batch_size = int(
            min(max(self._batch_size, self.min_batch_size), self.max_batch_size)
        )

    def set_batch_size(self, value: int) -> None:
        """Force the current batch size to ``value``, clamped to the bounds.

        Used by the batcher for emergency overrides (e.g. dropping to a safe
        default under resource stress) without going through a gradient step.

        An out-of-band jump invalidates the finite-difference climb history: the
        stored ``_prev_batch_size`` / ``_prev_utility`` were measured at the old
        (pre-jump) batch, so differencing the next sample against them would
        compare two unrelated operating points. In particular, after an emergency
        slams the batch down to the floor, that stale pair makes "smaller" look
        better and pins the optimizer at ``min_batch_size`` on recovery. We
        therefore clear the climb memory and reset the probe direction to ``+1``
        so the next :meth:`update` starts a fresh upward climb from ``value``.

        Args:
            value: Desired batch size; clamped into ``[min, max]`` before taking effect.
        """
        self._batch_size = int(
            min(max(value, self.min_batch_size), self.max_batch_size)
        )
        self._prev_batch_size = None
        self._prev_utility = None
        self._direction = 1
        self._last_gradient = 0.0

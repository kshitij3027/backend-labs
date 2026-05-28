"""The explicit operating-state machine of the control loop (spec Feature Area B).

The optimizer moves through four states:

* ``LEARNING`` — gather baseline ``(batch_size, throughput)`` samples. After
  ``learning_samples`` ticks the loop has enough signal to start climbing.
* ``OPTIMIZING`` — active gradient ascent: probe larger/smaller batches and
  follow the multi-objective gradient.
* ``STABLE`` — the gradient has settled near zero; hold the current batch size
  and stop probing.
* ``EMERGENCY`` — a hard constraint was breached; the batch is slashed to a
  safe size and held until recovery hysteresis is satisfied.

Transitions::

    LEARNING --(N samples)--> OPTIMIZING
    OPTIMIZING --(stable)--> STABLE
    STABLE --(drift)--> OPTIMIZING
    any --(breach)--> EMERGENCY
    EMERGENCY --(recovery_ready)--> OPTIMIZING

A constraint breach always wins and forces ``EMERGENCY`` from any state. Once
recovery hysteresis (handled externally by the ``ConstraintHandler``) reports
ready, the machine resumes in ``OPTIMIZING`` rather than re-learning.

This module is intentionally decoupled from the safety layer: :meth:`update`
takes plain booleans (``breach``, ``recovery_ready``, ``stable``) that the
``AdaptiveBatcher`` computes and passes in. It does **not** import
``ConstraintHandler``.
"""

from __future__ import annotations

from src.models import OptimizerState
from src.settings import get_settings


class StateMachine:
    """Drive :class:`OptimizerState` transitions from per-tick boolean signals.

    The machine holds only the current state and a sample counter used while
    ``LEARNING``. All decision inputs are supplied by the caller, keeping this
    component free of any dependency on metrics or constraint logic.
    """

    def __init__(self, *, learning_samples: int | None = None) -> None:
        self.learning_samples = (
            learning_samples
            if learning_samples is not None
            else get_settings().learning_samples
        )
        self._state = OptimizerState.LEARNING
        self._samples = 0

    @property
    def state(self) -> OptimizerState:
        """The current operating state."""
        return self._state

    def update(
        self, *, breach: bool, recovery_ready: bool, stable: bool
    ) -> OptimizerState:
        """Apply one transition from the supplied signals and return the new state.

        Precedence:

        * ``breach`` forces ``EMERGENCY`` from any state.
        * From ``EMERGENCY``: advance to ``OPTIMIZING`` when ``recovery_ready``,
          otherwise stay.
        * From ``LEARNING``: count the sample; once ``learning_samples`` have
          accumulated, advance to ``OPTIMIZING``.
        * From ``OPTIMIZING``: settle to ``STABLE`` when ``stable``.
        * From ``STABLE``: drop back to ``OPTIMIZING`` when no longer ``stable``
          (drift detected).
        """
        if breach:
            self._state = OptimizerState.EMERGENCY
        elif self._state is OptimizerState.EMERGENCY:
            if recovery_ready:
                self._state = OptimizerState.OPTIMIZING
        elif self._state is OptimizerState.LEARNING:
            self._samples += 1
            if self._samples >= self.learning_samples:
                self._state = OptimizerState.OPTIMIZING
        elif self._state is OptimizerState.OPTIMIZING:
            if stable:
                self._state = OptimizerState.STABLE
        elif self._state is OptimizerState.STABLE:
            if not stable:
                self._state = OptimizerState.OPTIMIZING

        return self._state

    def reset(self) -> None:
        """Return to ``LEARNING`` and clear the sample counter."""
        self._state = OptimizerState.LEARNING
        self._samples = 0

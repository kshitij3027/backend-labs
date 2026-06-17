"""Deterministic synthetic workload generator.

:class:`LoadModel` models *demand* — the arrival rate of messages over time — as a
pure function of the clock. It is the counterpart to the worker pool in
:mod:`src.workers`, which models *capacity*. Keeping demand and capacity in
separate modules lets the orchestrator reason about the gap between them.

Determinism is a design goal: there is no randomness anywhere in this module, so a
given ``now`` always maps to the same arrival rate. Every method that depends on the
current time accepts an injectable ``now`` argument (seconds since the epoch, as from
:func:`time.time`). Tests pass an explicit ``now`` to get stable, repeatable values;
production callers omit it and the wall clock is used.

The instantaneous arrival rate is the product of three factors::

    arrival_rate(now) = base_arrival_rate
                        * time_of_day_factor(now)   # mild diurnal swing, ~[0.5, 1.5]
                        * ramp_factor(now)           # 1.0 unless a ramp is active

with the result floored at zero. While a ramp injected via :meth:`ramp` is active (or
holding after completion), the ramp *overrides* the ``base * time_of_day`` product and
linearly interpolates toward the target rate — this is the hook the end-to-end load
test uses to drive demand far above baseline on command.
"""

import math
import time

# Period of one day in seconds — used to derive an hour-of-day from a UNIX timestamp.
_SECONDS_PER_DAY = 24.0 * 60.0 * 60.0
_SECONDS_PER_HOUR = 60.0 * 60.0

# Amplitude of the diurnal multiplier. Kept deliberately mild so that an injected ramp
# (used in E2E load tests) clearly dominates the time-of-day swing.
_TOD_AMPLITUDE = 0.5


def _clamp(value: float, low: float, high: float) -> float:
    """Return ``value`` constrained to the closed interval ``[low, high]``."""
    if value < low:
        return low
    if value > high:
        return high
    return value


class LoadModel:
    """A deterministic, time-driven model of incoming message demand.

    Args:
        base_arrival_rate: Baseline demand in messages/second. The diurnal factor
            swings around this value; an active ramp interpolates away from it.

    The ``now`` argument accepted by :meth:`arrival_rate` and :meth:`ramp` is seconds
    since the epoch and is injectable purely so tests can be deterministic. When it is
    ``None`` the current wall-clock time (:func:`time.time`) is used.
    """

    def __init__(self, base_arrival_rate: float) -> None:
        self.base_arrival_rate = float(base_arrival_rate)

        # Active-ramp state. All None when no ramp is in effect (see ``reset``).
        self._ramp_base: float | None = None      # arrival_rate at ramp start (base*tod)
        self._ramp_target: float | None = None     # rate to interpolate toward / hold at
        self._ramp_start: float | None = None       # ``now`` captured when ramp() was called
        self._ramp_seconds: float | None = None      # interpolation duration in seconds

    # -- factors -----------------------------------------------------------------

    def time_of_day_factor(self, now: float) -> float:
        """Return a mild smooth diurnal multiplier for timestamp ``now``.

        The factor follows a sine wave over the 24-hour day, peaking in the
        afternoon and dipping overnight, and stays within roughly ``[0.5, 1.5]``.
        It is intentionally gentle so that ramps injected for load tests dominate.
        Deterministic: depends only on the hour-of-day derived from ``now``.
        """
        hour_of_day = (now % _SECONDS_PER_DAY) / _SECONDS_PER_HOUR
        # Phase-shifted so the trough/peak sit at sensible times of day; amplitude
        # bounded by _TOD_AMPLITUDE keeps the result inside ~[0.5, 1.5].
        return 1.0 + _TOD_AMPLITUDE * math.sin(2.0 * math.pi * (hour_of_day - 9.0) / 24.0)

    def ramp_factor(self, now: float) -> float:
        """Return the ramp multiplier at ``now`` (``1.0`` when no ramp is active).

        This multiplier exists for completeness; the actual ramp behaviour is applied
        as an *override* inside :meth:`arrival_rate` (the interpolated rate replaces the
        ``base * time_of_day`` product rather than scaling it). When no ramp is active
        this returns ``1.0``.
        """
        if not self._ramp_active():
            return 1.0
        base = self.base_arrival_rate * self.time_of_day_factor(now)
        if base <= 0.0:
            return 1.0
        return self._ramped_rate(now) / base

    # -- public API --------------------------------------------------------------

    def arrival_rate(self, now: float) -> float:
        """Return the instantaneous arrival rate (msgs/sec) at timestamp ``now``.

        When a ramp is active, the linearly interpolated ramp rate is returned
        directly (overriding the base/diurnal product). Otherwise the rate is
        ``base_arrival_rate * time_of_day_factor(now)``. The result is never negative.

        Args:
            now: Seconds since the epoch. Injectable for deterministic tests.
        """
        if self._ramp_active():
            return max(0.0, self._ramped_rate(now))
        rate = self.base_arrival_rate * self.time_of_day_factor(now)
        return max(0.0, rate)

    def ramp(self, target_rate: float, seconds: float, now: float | None = None) -> None:
        """Begin a linear ramp from the current rate up (or down) to ``target_rate``.

        This is the load-injection hook used by the end-to-end test to push demand
        well above baseline. After this call, :meth:`arrival_rate` linearly
        interpolates from the rate captured *now* (the ``base * time_of_day`` value at
        ramp start, before this ramp is applied) toward ``target_rate`` over
        ``seconds`` seconds, then holds at ``target_rate`` indefinitely.

        Args:
            target_rate: The arrival rate to ramp toward and then hold.
            seconds: Duration of the linear interpolation. Non-positive values make
                the ramp take effect immediately (jump straight to ``target_rate``).
            now: Ramp start time (seconds since epoch). Defaults to :func:`time.time`.
        """
        start = time.time() if now is None else float(now)
        # Capture the pre-ramp rate from base*tod (deliberately NOT recursing through
        # any previously active ramp — a new ramp anchors on the natural baseline).
        self._ramp_base = self.base_arrival_rate * self.time_of_day_factor(start)
        self._ramp_target = float(target_rate)
        self._ramp_start = start
        self._ramp_seconds = float(seconds)

    def reset(self) -> None:
        """Clear any active ramp, restoring pure ``base * time_of_day`` behaviour."""
        self._ramp_base = None
        self._ramp_target = None
        self._ramp_start = None
        self._ramp_seconds = None

    # -- internals ---------------------------------------------------------------

    def _ramp_active(self) -> bool:
        """Whether a ramp has been recorded (and not :meth:`reset`)."""
        return self._ramp_start is not None

    def _ramped_rate(self, now: float) -> float:
        """Linearly interpolate from ``_ramp_base`` to ``_ramp_target`` at ``now``.

        Before the ramp start the base rate is returned; once ``_ramp_seconds`` have
        elapsed the target is held. Assumes a ramp is active.
        """
        assert self._ramp_base is not None
        assert self._ramp_target is not None
        assert self._ramp_start is not None
        assert self._ramp_seconds is not None

        if self._ramp_seconds <= 0.0:
            frac = 1.0
        else:
            frac = _clamp((now - self._ramp_start) / self._ramp_seconds, 0.0, 1.0)
        return self._ramp_base + frac * (self._ramp_target - self._ramp_base)

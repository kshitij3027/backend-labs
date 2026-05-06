"""Async ``CircuitBreaker`` implementing the three-state machine.

This module exposes :class:`CircuitBreaker`, a per-dependency breaker that
wraps an async callable and trips OPEN when failures accumulate. The basic
opening rule used here (Commit 4) is a simple count-in-window check; the
smarter detection logic (error rate, slow calls, consecutive failures) lands
in Commit 5.

State machine:

* CLOSED   -> calls pass through; failures are counted.
* OPEN     -> calls short-circuit with :class:`CircuitBreakerOpenException`
              until ``recovery_timeout`` elapses, then moves to HALF_OPEN.
* HALF_OPEN-> a small number of probe calls are admitted. If they all
              succeed, the breaker closes; one failure reopens it.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, TypeVar

from src.config import CircuitBreakerConfig
from src.exceptions import (
    CircuitBreakerOpenException,
    CircuitBreakerTimeoutException,
)
from src.state import CircuitState
from src.stats import CallWindow, CircuitStats

T = TypeVar("T")


class CircuitBreaker:
    """Async-safe circuit breaker around a single dependency.

    The breaker holds a fast asyncio lock that protects state-mutation
    sections only; the wrapped ``await func(...)`` always runs OUTSIDE the
    lock so a slow downstream call cannot stall other pending callers.
    """

    def __init__(self, config: CircuitBreakerConfig) -> None:
        """Create a breaker bound to ``config``.

        Args:
            config: Tunable parameters for this breaker instance.
        """
        self.config: CircuitBreakerConfig = config
        self.name: str = config.name
        self._state: CircuitState = CircuitState.CLOSED
        self._stats: CircuitStats = CircuitStats()
        self._window: CallWindow = CallWindow(config.monitoring_window)
        self._lock: asyncio.Lock = asyncio.Lock()
        self._half_open_inflight: int = 0
        self._half_open_successes: int = 0
        self._consecutive_failures: int = 0
        self._listeners: list[Callable[..., Any]] = []

    # ------------------------------------------------------------------ #
    # Public properties / introspection                                  #
    # ------------------------------------------------------------------ #

    @property
    def state(self) -> CircuitState:
        """Live circuit state. Mirrors ``self._stats.current_state``."""
        return self._state

    def get_stats(self) -> dict:
        """Return a JSON-friendly snapshot of all stats for this breaker."""
        return {
            "name": self.name,
            "state": self._state.value,
            "success_rate": self._stats.success_rate(),
            **self._stats.to_dict(),
            "consecutive_failures": self._consecutive_failures,
            "window_volume": self._window.volume(),
        }

    def to_dict(self) -> dict:
        """Alias for :meth:`get_stats` (used by registries / serializers)."""
        return self.get_stats()

    def add_listener(
        self,
        listener: Callable[[str, CircuitState, CircuitState, str], Any],
    ) -> None:
        """Register a callable invoked on every state transition.

        Args:
            listener: Either a sync or async callable with the signature
                ``(name, from_state, to_state, reason)``. Async listeners
                are awaited; sync listeners are called directly.
        """
        self._listeners.append(listener)

    # ------------------------------------------------------------------ #
    # Core call path                                                     #
    # ------------------------------------------------------------------ #

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run ``func(*args, **kwargs)`` through the breaker.

        Behavior:
            * In CLOSED state, calls pass straight through.
            * In OPEN state, raises :class:`CircuitBreakerOpenException`
              unless ``recovery_timeout`` has elapsed, in which case the
              breaker transitions to HALF_OPEN and admits this call.
            * In HALF_OPEN state, admits at most
              ``config.half_open_max_calls`` concurrent probes. Excess
              callers see :class:`CircuitBreakerOpenException`.
            * Timeouts (``asyncio.TimeoutError``) become
              :class:`CircuitBreakerTimeoutException` and count as a failure.
            * Exceptions matching ``config.expected_exception`` count as
              failures and re-raise. Other exception types propagate
              without affecting the breaker.
        """
        # -- Fast path: state check + admission control ------------------
        async with self._lock:
            self._window.trim()

            if self._state == CircuitState.OPEN:
                opened_at = self._stats.opened_at or 0.0
                if time.time() - opened_at >= self.config.recovery_timeout:
                    await self._transition(
                        CircuitState.HALF_OPEN,
                        "recovery timeout elapsed",
                    )
                    self._half_open_inflight = 0
                    self._half_open_successes = 0
                    self._half_open_inflight += 1
                else:
                    raise CircuitBreakerOpenException(self.name, opened_at)
            elif self._state == CircuitState.HALF_OPEN:
                if self._half_open_inflight >= self.config.half_open_max_calls:
                    raise CircuitBreakerOpenException(
                        self.name, self._stats.opened_at or 0.0
                    )
                self._half_open_inflight += 1
            # CLOSED: nothing special.

        # -- Slow path: actually invoke the wrapped callable -------------
        start = time.time()
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout_duration,
            )
        except asyncio.TimeoutError:
            await self._on_failure(
                latency=self.config.timeout_duration,
                is_timeout=True,
            )
            raise CircuitBreakerTimeoutException(
                self.name, self.config.timeout_duration
            )
        except self.config.expected_exception:
            latency = time.time() - start
            await self._on_failure(latency=latency, is_timeout=False)
            raise
        except BaseException:
            # Exceptions outside the configured ``expected_exception`` type
            # are not breaker failures; they propagate but we still need to
            # release any HALF_OPEN admission slot we took.
            if self._state == CircuitState.HALF_OPEN:
                async with self._lock:
                    if self._half_open_inflight > 0:
                        self._half_open_inflight -= 1
            raise
        else:
            latency = time.time() - start
            await self._on_success(latency=latency)
            return result

    # ------------------------------------------------------------------ #
    # Outcome handlers                                                   #
    # ------------------------------------------------------------------ #

    async def _on_success(self, latency: float) -> None:
        """Record a successful call and possibly close a HALF_OPEN breaker."""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.successful_calls += 1
            self._stats.last_success_time = time.time()
            self._consecutive_failures = 0
            self._window.record(True, latency)

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_inflight > 0:
                    self._half_open_inflight -= 1
                if self._half_open_successes >= self.config.half_open_max_calls:
                    await self._transition(
                        CircuitState.CLOSED,
                        "half-open recovery succeeded",
                    )

    async def _on_failure(self, latency: float, is_timeout: bool) -> None:
        """Record a failure and possibly trip the breaker OPEN."""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.failed_calls += 1
            if is_timeout:
                self._stats.timeout_calls += 1
            self._stats.last_failure_time = time.time()
            self._consecutive_failures += 1
            self._window.record(False, latency)

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_inflight > 0:
                    self._half_open_inflight -= 1
                await self._transition(
                    CircuitState.OPEN,
                    "half-open probe failed",
                )
            elif self._state == CircuitState.CLOSED and self._should_open():
                await self._transition(CircuitState.OPEN, self._open_reason())

    # ------------------------------------------------------------------ #
    # Threshold logic (basic version for Commit 4)                       #
    # ------------------------------------------------------------------ #

    def _failed_in_window(self) -> int:
        """Count failures currently inside the sliding window."""
        self._window.trim()
        return sum(1 for r in self._window._records if not r.success)

    def _should_open(self) -> bool:
        """Basic Commit-4 rule: trip OPEN once failure count meets threshold."""
        return self._failed_in_window() >= self.config.failure_threshold

    def _open_reason(self) -> str:
        """Human-readable reason string used in state transitions."""
        return f"{self.config.failure_threshold} failures in window"

    # ------------------------------------------------------------------ #
    # State transition primitive                                         #
    # ------------------------------------------------------------------ #

    async def _transition(self, new_state: CircuitState, reason: str) -> None:
        """Mutate state, update stats, and notify listeners.

        Note:
            This method must be invoked from inside an ``async with self._lock``
            block. Listeners are invoked synchronously while the lock is held,
            so listener callbacks **must not** call back into this breaker —
            doing so would deadlock. Listeners are wrapped in try/except so a
            single bad listener cannot break the transition path.
        """
        old_state = self._state

        if old_state == new_state:
            return

        # Update cumulative OPEN duration when leaving OPEN.
        if old_state == CircuitState.OPEN and self._stats.opened_at is not None:
            self._stats.cumulative_open_duration += (
                time.time() - self._stats.opened_at
            )

        self._state = new_state
        self._stats.current_state = new_state
        self._stats.state_changes += 1

        if new_state == CircuitState.OPEN:
            self._stats.opened_at = time.time()
        elif new_state == CircuitState.CLOSED:
            # Fresh CLOSED cycle: clear half-open bookkeeping.
            self._half_open_inflight = 0
            self._half_open_successes = 0
            self._consecutive_failures = 0

        for listener in self._listeners:
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(self.name, old_state, new_state, reason)
                else:
                    listener(self.name, old_state, new_state, reason)
            except Exception:
                # Best-effort: never let a listener break the breaker.
                pass

    # ------------------------------------------------------------------ #
    # Test / operational helpers                                         #
    # ------------------------------------------------------------------ #

    async def force_open(self) -> None:
        """Force the breaker into the OPEN state (testing / ops escape hatch)."""
        async with self._lock:
            await self._transition(CircuitState.OPEN, "manually forced open")

    async def reset(self) -> None:
        """Reset the breaker to a fresh CLOSED state.

        Cumulative OPEN-duration is preserved across resets; everything else
        (counters, window, half-open bookkeeping) is zeroed.
        """
        async with self._lock:
            preserved_open_duration = self._stats.cumulative_open_duration
            self._state = CircuitState.CLOSED
            self._stats = CircuitStats(
                cumulative_open_duration=preserved_open_duration,
                current_state=CircuitState.CLOSED,
            )
            self._window.clear()
            self._half_open_inflight = 0
            self._half_open_successes = 0
            self._consecutive_failures = 0

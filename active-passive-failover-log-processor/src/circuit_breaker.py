"""Async-aware circuit breaker for inter-node calls.

Implements the classic three-state breaker (CLOSED -> OPEN -> HALF_OPEN ->
CLOSED) with no third-party dependency. Each :class:`CircuitBreaker` is a
self-contained piece of state, guarded by an :class:`asyncio.Lock` so that
concurrent callers see consistent transitions.

State semantics
---------------
* **CLOSED** — calls pass straight through. Each ``fn`` exception bumps
  the consecutive-failure count; once it reaches ``fail_max`` the breaker
  opens and the failing call's exception still propagates to the caller.
  Any successful call resets the failure count to zero.
* **OPEN** — calls are rejected with :class:`CircuitBreakerOpen` *without*
  invoking ``fn`` until ``reset_timeout`` seconds have elapsed since the
  breaker opened. After the cooldown the breaker enters HALF_OPEN.
* **HALF_OPEN** — exactly one trial call is allowed. Success -> CLOSED
  (with ``failure_count = 0``); failure -> OPEN (and the cooldown clock
  resets).

Why DIY: per the project plan we deliberately avoid the
`pybreaker` dependency. The implementation here is intentionally small —
the only behaviours we care about are the three transitions above and a
counter snapshot for /metrics.

Time source
-----------
We use :py:func:`time.monotonic` for the cooldown clock so we don't
break under wall-clock skew. Tests can monkeypatch ``time.monotonic`` via
the ``circuit_breaker`` module attribute (``cb_module.time.monotonic = ...``)
to drive the half-open transition deterministically.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class CircuitBreakerOpen(RuntimeError):
    """Raised by :meth:`CircuitBreaker.call` when the breaker is OPEN.

    Distinct from any underlying transport exception so callers can
    distinguish "we refused to even try" from "the call failed".
    """


class CircuitBreaker:
    """Three-state async circuit breaker.

    Parameters
    ----------
    name:
        Human-readable identifier — used only in log messages so a
        cluster running with many breakers can be debugged.
    fail_max:
        Number of consecutive failures (in CLOSED state) that trip the
        breaker into OPEN.
    reset_timeout:
        Seconds the breaker stays OPEN before allowing one HALF_OPEN
        trial. Reset on every transition into OPEN, including a
        HALF_OPEN -> OPEN demotion.
    """

    def __init__(
        self,
        name: str,
        fail_max: int = 5,
        reset_timeout: float = 30.0,
    ) -> None:
        self.name: str = name
        self.fail_max: int = fail_max
        self.reset_timeout: float = reset_timeout

        self._state: str = CLOSED
        self._failure_count: int = 0
        # Monotonic clock value at which the current OPEN state was
        # entered. Only meaningful when ``_state == OPEN``.
        self._opened_at: float = 0.0
        self._lock: asyncio.Lock = asyncio.Lock()

        # Counters — exposed via :pyattr:`metrics`.
        self._calls_total: int = 0
        self._successes_total: int = 0
        self._failures_total: int = 0
        self._opens_total: int = 0

    # --- public read-only accessors ----------------------------------------

    @property
    def state(self) -> str:
        """Current breaker state — one of ``"closed"``, ``"open"``, ``"half_open"``."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Consecutive-failure count. Reset on any success in CLOSED."""
        return self._failure_count

    @property
    def metrics(self) -> dict[str, int]:
        """Snapshot of breaker counters for /metrics aggregation."""
        return {
            "calls_total": self._calls_total,
            "successes_total": self._successes_total,
            "failures_total": self._failures_total,
            "opens_total": self._opens_total,
        }

    # --- main entry point --------------------------------------------------

    async def call(
        self,
        fn: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Run ``fn(*args, **kwargs)`` through the breaker.

        Raises :class:`CircuitBreakerOpen` immediately if the breaker is
        OPEN and the cooldown has not yet elapsed. Otherwise the call
        proceeds and any exception ``fn`` raises is re-raised after the
        breaker state is updated.
        """
        await self._before_call()

        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            await self._after_failure(exc)
            raise
        else:
            await self._after_success()
            return result

    # --- internal state transitions ---------------------------------------

    async def _before_call(self) -> None:
        """Decide whether to admit the call; transition OPEN -> HALF_OPEN if cooled."""
        async with self._lock:
            self._calls_total += 1
            if self._state == OPEN:
                if (time.monotonic() - self._opened_at) >= self.reset_timeout:
                    # Cooldown elapsed — let one trial through.
                    self._state = HALF_OPEN
                    logger.info(
                        "circuit breaker %s: open -> half_open (trial)",
                        self.name,
                    )
                else:
                    # Still cooling down; reject without invoking ``fn``.
                    raise CircuitBreakerOpen(
                        f"circuit breaker {self.name!r} is OPEN"
                    )

    async def _after_success(self) -> None:
        """Reset failure count on success; close the breaker if HALF_OPEN."""
        async with self._lock:
            self._successes_total += 1
            if self._state == HALF_OPEN:
                self._state = CLOSED
                logger.info(
                    "circuit breaker %s: half_open -> closed (recovered)",
                    self.name,
                )
            self._failure_count = 0

    async def _after_failure(self, exc: BaseException) -> None:
        """Increment failure count; open the breaker if it crosses ``fail_max``."""
        async with self._lock:
            self._failures_total += 1
            if self._state == HALF_OPEN:
                # The trial failed -> straight back to OPEN, reset clock.
                self._state = OPEN
                self._opens_total += 1
                self._opened_at = time.monotonic()
                logger.warning(
                    "circuit breaker %s: half_open -> open (trial failed: %s)",
                    self.name,
                    exc,
                )
                return

            # CLOSED path: bump consecutive-failure count.
            self._failure_count += 1
            if (
                self._state == CLOSED
                and self._failure_count >= self.fail_max
            ):
                self._state = OPEN
                self._opens_total += 1
                self._opened_at = time.monotonic()
                logger.warning(
                    "circuit breaker %s: closed -> open after %d consecutive failures",
                    self.name,
                    self._failure_count,
                )


__all__ = ["CircuitBreaker", "CircuitBreakerOpen", "CLOSED", "OPEN", "HALF_OPEN"]

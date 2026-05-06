"""Abstract base for breaker-guarded services."""
from __future__ import annotations
import logging
from typing import Any, Awaitable, Callable, TypeVar

from src.breaker import CircuitBreaker
from src.exceptions import CircuitBreakerOpenException, CircuitBreakerTimeoutException
from src.failure_injection import FailureInjector

logger = logging.getLogger(__name__)
T = TypeVar("T")


class BaseService:
    """Common scaffolding for services protected by a CircuitBreaker.

    Subclasses define the real work in their own methods and call
    ``await self._execute(real_op, fallback)`` to dispatch through the breaker.
    """

    def __init__(self, name: str, breaker: CircuitBreaker, injector: FailureInjector | None = None):
        self.name = name
        self.breaker = breaker
        self.injector = injector if injector is not None else FailureInjector()

    async def _execute(
        self,
        op: Callable[[], Awaitable[T]],
        fallback: Callable[[], T],
    ) -> T:
        """Dispatch ``op`` through the breaker; on rejection/timeout return ``fallback()``."""
        try:
            return await self.breaker.call(op)
        except CircuitBreakerOpenException:
            logger.info("breaker '%s' rejected call (OPEN) — using fallback", self.name)
            return fallback()
        except CircuitBreakerTimeoutException:
            logger.info("breaker '%s' timed out — using fallback", self.name)
            return fallback()

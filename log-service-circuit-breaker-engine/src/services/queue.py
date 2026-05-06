"""Synthetic message queue service guarded by a circuit breaker."""
from __future__ import annotations
from typing import Any

from src.breaker import CircuitBreaker
from src.failure_injection import FailureInjector
from src.services.base import BaseService


class MessageQueueService(BaseService):
    """Simulated message queue. ``publish`` appends to a topic with an
    incrementing offset; failure injection drives it to raise so tests can
    exercise breaker state transitions."""

    def __init__(
        self,
        name: str,
        breaker: CircuitBreaker,
        injector: FailureInjector | None = None,
    ):
        super().__init__(name, breaker, injector)
        self._offset = 0

    async def publish(self, log_entry: dict) -> dict:
        async def real():
            await self.injector.maybe_fail()
            offset = self._offset
            self._offset += 1
            return {
                "status": "ok",
                "topic": "logs",
                "offset": offset,
                "service": self.name,
            }

        def fallback():
            return {
                "status": "fallback",
                "service": self.name,
                "queued": False,
            }

        return await self._execute(real, fallback)

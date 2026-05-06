"""Synthetic database service guarded by a circuit breaker."""
from __future__ import annotations
import uuid
from typing import Any

from src.services.base import BaseService


class DatabaseService(BaseService):
    """Simulated DB. ``insert_log`` is the only public op; failure injection
    drives it to raise so tests can exercise breaker state transitions."""

    async def insert_log(self, log_entry: dict) -> dict:
        async def real():
            await self.injector.maybe_fail()
            return {
                "status": "ok",
                "id": uuid.uuid4().hex,
                "service": self.name,
                "log": log_entry,
            }

        def fallback():
            return {
                "status": "fallback",
                "service": self.name,
                "cached": True,
                "log": log_entry,
            }

        return await self._execute(real, fallback)

"""Synthetic external API service guarded by a circuit breaker."""
from __future__ import annotations
from typing import Any

from src.services.base import BaseService


class ExternalAPIService(BaseService):
    """Simulated external enrichment API. ``enrich`` returns geo/tier metadata
    on success; failure injection drives it to raise so tests can exercise
    breaker state transitions."""

    async def enrich(self, log_entry: dict) -> dict:
        async def real():
            await self.injector.maybe_fail()
            return {
                "status": "ok",
                "service": self.name,
                "enrichment": {"geo": "us-west", "tier": "premium"},
            }

        def fallback():
            return {
                "status": "fallback",
                "service": self.name,
                "enrichment": {"geo": "unknown", "tier": "unknown"},
            }

        return await self._execute(real, fallback)

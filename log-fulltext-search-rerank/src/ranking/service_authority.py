"""Service-authority scoring — weight logs by their originating service.

A ``payment`` log is more actionable than an unknown worker's
heartbeat, so the reranker multiplies the service weight into the
final score. Weights come from ``settings.service_authority_weights``
with a configurable fallback (``"unknown"`` key, default ``0.5``).
"""

from __future__ import annotations

from src.config import Settings


class ServiceAuthorityScorer:
    """Weight from ``settings.service_authority_weights`` with fallback.

    Services not in the table — or the empty string — receive the
    ``unknown`` weight so an unconfigured producer degrades
    gracefully instead of being treated as top-authority.
    """

    def __init__(self, settings: Settings) -> None:
        self._weights = dict(settings.service_authority_weights)
        self._fallback = self._weights.get("unknown", 0.5)

    def score(self, service: str) -> float:
        """Return the weight for ``service`` (case-insensitive)."""
        if not service:
            return self._fallback
        return self._weights.get(service.lower(), self._fallback)

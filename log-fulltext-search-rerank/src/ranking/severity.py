"""Severity scoring — map a log level string to a numeric weight.

Thin wrapper over ``settings.severity_weights`` with a dose of
normalisation: case-insensitive lookup plus ``WARNING -> WARN``
canonicalisation so either spelling behaves identically regardless
of whether the config table includes both keys.
"""

from __future__ import annotations

from src.config import Settings


class SeverityScorer:
    """Map a ``LogEntry.level`` to a weight from ``settings.severity_weights``.

    Unknown levels fall back to ``0.0`` so a level missing from
    config is treated as neutral rather than being silently ranked as
    an ERROR — that would mask malformed data.
    """

    def __init__(self, settings: Settings) -> None:
        # Copy so mutations of the settings elsewhere don't leak into
        # live scoring, and so the scorer can be constructed once and
        # cached on app.state without fear of dependency injection
        # games changing behaviour later.
        self._weights: dict[str, float] = dict(settings.severity_weights)

    def score(self, level: str) -> float:
        """Return the weight for ``level``.

        Empty or falsy input returns ``0.0`` immediately so a log
        entry with a missing level cannot silently inherit a
        positive ``severity`` contribution.
        """
        if not level:
            return 0.0
        norm = level.upper()
        if norm == "WARNING":
            norm = "WARN"
        # Prefer the normalised form, but fall back to the raw value
        # in case a caller has configured an entry under an exotic key.
        return self._weights.get(norm, self._weights.get(level, 0.0))

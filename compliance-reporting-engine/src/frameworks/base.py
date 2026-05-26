"""Abstract base class for framework-specific compliance rule sets.

Every supported framework (SOX, HIPAA, PCI-DSS, GDPR, FinHealth) ships a
small subclass of :class:`FrameworkRules` that declares:

  * ``name`` — the canonical framework code (e.g. ``"SOX"``).
  * ``categories`` — a fixed list of five evidence-category strings that
    the report's ``summary`` block is keyed against. The exporters render
    these in a stable order, so the list is treated as authoritative.
  * ``event_type_to_category`` — a flat mapping from ``LogEvent.event_type``
    to one of the declared categories. The seeder picks event_types from
    the same per-framework allowlist, so as long as the two stay in sync
    every relevant event lands in exactly one bucket.

The classmethods (``classify``, ``summarize``, ``findings``) are pure
functions of the input event list — no DB access, no clock reads — which
keeps the rules trivially unit-testable. ``summarize`` is intentionally
zero-fill: callers can iterate ``rules.categories`` once and trust that
every key exists in the resulting dict.

The ``LogEvent`` import lives under ``TYPE_CHECKING`` to avoid a circular
import: ``frameworks`` is imported during app startup, but
``persistence.models`` should remain free of framework knowledge.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


class FrameworkRules:
    """Base class for per-framework classification, summary, and findings logic.

    Subclasses MUST override ``name``, ``categories``, and
    ``event_type_to_category`` (the registry decorator wires the subclass
    up by ``name``). They MAY override ``findings`` to emit
    framework-specific human-readable strings.

    This base class is deliberately not registered: only concrete
    subclasses appear in ``FRAMEWORK_REGISTRY``.
    """

    name: ClassVar[str] = ""
    categories: ClassVar[list[str]] = []
    event_type_to_category: ClassVar[dict[str, str]] = {}

    @classmethod
    def classify(cls, event: "LogEvent") -> str | None:
        """Return the evidence category for ``event``, or ``None`` if irrelevant.

        Default behaviour: look ``event.event_type`` up in the
        ``event_type_to_category`` map. Subclasses may override for more
        nuanced classification (e.g. inspecting the payload).
        """
        return cls.event_type_to_category.get(event.event_type)

    @classmethod
    def summarize(cls, events: list["LogEvent"]) -> dict[str, int]:
        """Return a category -> count dict, zero-filled for every category.

        Events whose ``event_type`` isn't in the mapping (or whose
        ``classify`` returns ``None``) are silently ignored. Callers can
        iterate ``cls.categories`` and trust that every key is present
        even when the count is zero.
        """
        counts: dict[str, int] = {category: 0 for category in cls.categories}
        for event in events:
            category = cls.classify(event)
            if category is not None and category in counts:
                counts[category] += 1
        return counts

    @classmethod
    def findings(cls, events: list["LogEvent"]) -> list[str]:
        """Return framework-specific human-readable finding strings.

        Base implementation returns an empty list; subclasses override
        to emit insights like "3 SoD violations detected in period".
        """
        return []

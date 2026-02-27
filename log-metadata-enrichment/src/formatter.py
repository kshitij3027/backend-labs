"""Output formatting utilities for enriched log entries."""

from __future__ import annotations

from src.models import EnrichedLog


def format_enriched_log_dict(enriched: EnrichedLog) -> dict:
    """Convert an EnrichedLog to a dict, excluding None-valued fields."""
    return enriched.model_dump(exclude_none=True)


def format_enriched_log(enriched: EnrichedLog) -> str:
    """Convert an EnrichedLog to a pretty-printed JSON string, excluding None-valued fields."""
    return enriched.model_dump_json(exclude_none=True, indent=2)

"""Tests for output formatting utilities."""

import json

from src.formatter import format_enriched_log, format_enriched_log_dict
from src.models import EnrichedLog


def _make_enriched_log() -> EnrichedLog:
    """Helper to build a minimal EnrichedLog for testing."""
    return EnrichedLog(
        message="test log",
        source="test",
        hostname="node-1",
    )


class TestFormatEnrichedLogDict:
    """Verify dict formatting."""

    def test_excludes_none_values(self):
        enriched = _make_enriched_log()
        result = format_enriched_log_dict(enriched)
        assert None not in result.values()

    def test_includes_critical_fields(self):
        enriched = _make_enriched_log()
        result = format_enriched_log_dict(enriched)
        assert "message" in result
        assert "source" in result
        assert "timestamp" in result


class TestFormatEnrichedLog:
    """Verify JSON string formatting."""

    def test_returns_valid_json_string(self):
        enriched = _make_enriched_log()
        result = format_enriched_log(enriched)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_output_is_json_parseable(self):
        enriched = _make_enriched_log()
        result = format_enriched_log(enriched)
        parsed = json.loads(result)
        assert parsed["message"] == "test log"
        assert parsed["source"] == "test"

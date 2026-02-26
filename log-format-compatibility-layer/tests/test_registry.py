"""Tests for the adapter registry."""
import pytest
from src.adapters.base import LogFormatAdapter
from src.adapters import AdapterRegistry
from src.models import ParsedLog, SeverityLevel
from datetime import datetime


class MockHighConfidenceAdapter(LogFormatAdapter):
    """Mock adapter that always returns high confidence."""

    @property
    def format_name(self) -> str:
        return "mock_high"

    def can_handle(self, line: str) -> float:
        return 0.95

    def parse(self, line: str) -> ParsedLog:
        return ParsedLog(
            message=line,
            source_format=self.format_name,
            level=SeverityLevel.INFORMATIONAL,
        )


class MockLowConfidenceAdapter(LogFormatAdapter):
    """Mock adapter that always returns low confidence."""

    @property
    def format_name(self) -> str:
        return "mock_low"

    def can_handle(self, line: str) -> float:
        return 0.3

    def parse(self, line: str) -> ParsedLog:
        return ParsedLog(
            message=line,
            source_format=self.format_name,
        )


class MockZeroConfidenceAdapter(LogFormatAdapter):
    """Mock adapter that never matches."""

    @property
    def format_name(self) -> str:
        return "mock_zero"

    def can_handle(self, line: str) -> float:
        return 0.0

    def parse(self, line: str) -> ParsedLog:
        return ParsedLog(message=line, source_format=self.format_name)


class TestAdapterRegistry:
    def test_register_adapter(self):
        registry = AdapterRegistry()
        adapter = MockHighConfidenceAdapter()
        registry.register(adapter)
        assert len(registry.adapters) == 1
        assert registry.adapters[0].format_name == "mock_high"

    def test_detect_returns_highest_confidence(self):
        registry = AdapterRegistry()
        registry.register(MockLowConfidenceAdapter())
        registry.register(MockHighConfidenceAdapter())
        result = registry.detect("test line")
        assert result is not None
        adapter, confidence = result
        assert adapter.format_name == "mock_high"
        assert confidence == 0.95

    def test_detect_returns_none_when_no_adapters(self):
        registry = AdapterRegistry()
        result = registry.detect("test line")
        assert result is None

    def test_detect_returns_none_for_zero_confidence(self):
        registry = AdapterRegistry()
        registry.register(MockZeroConfidenceAdapter())
        result = registry.detect("test line")
        assert result is None

    def test_detect_and_parse(self):
        registry = AdapterRegistry()
        registry.register(MockHighConfidenceAdapter())
        parsed = registry.detect_and_parse("hello world")
        assert parsed is not None
        assert parsed.message == "hello world"
        assert parsed.source_format == "mock_high"
        assert parsed.confidence == 0.95

    def test_detect_and_parse_returns_none_for_unrecognized(self):
        registry = AdapterRegistry()
        registry.register(MockZeroConfidenceAdapter())
        parsed = registry.detect_and_parse("test line")
        assert parsed is None

    def test_short_circuit_on_high_confidence(self):
        """High confidence adapter should short-circuit detection."""
        registry = AdapterRegistry()
        # Register high confidence first - should short-circuit
        registry.register(MockHighConfidenceAdapter())
        registry.register(MockLowConfidenceAdapter())
        result = registry.detect("test")
        assert result is not None
        adapter, confidence = result
        assert adapter.format_name == "mock_high"

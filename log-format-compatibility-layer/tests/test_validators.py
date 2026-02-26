"""Tests for log validators."""
import pytest
from src.validators import validate_parsed_log
from src.models import ParsedLog, SeverityLevel


class TestValidators:
    def test_valid_log(self):
        log = ParsedLog(
            message="Test message",
            source_format="json",
            confidence=0.95,
        )
        is_valid, errors = validate_parsed_log(log)
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_message(self):
        log = ParsedLog(source_format="json", confidence=0.5)
        is_valid, errors = validate_parsed_log(log)
        assert is_valid is False
        assert any("message" in e.lower() for e in errors)

    def test_missing_source_format(self):
        log = ParsedLog(message="Test", confidence=0.5)
        is_valid, errors = validate_parsed_log(log)
        assert is_valid is False
        assert any("source_format" in e.lower() for e in errors)

    def test_invalid_confidence(self):
        log = ParsedLog(message="Test", source_format="json", confidence=1.5)
        is_valid, errors = validate_parsed_log(log)
        assert is_valid is False
        assert any("confidence" in e.lower() for e in errors)

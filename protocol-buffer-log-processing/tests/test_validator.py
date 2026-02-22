"""Tests for src.validator."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from src.validator import ValidationError, validate_log_entry


def _make_entry(**overrides: object) -> dict:
    """Return a valid log entry dict, optionally overriding fields."""
    base: dict = {
        "timestamp": datetime.now(timezone.utc),
        "service_name": "test-service",
        "level": "INFO",
        "message": "Something happened",
        "metadata": {"request_id": "abc-123"},
    }
    base.update(overrides)
    return base


class TestValidEntry:
    """Entries that should pass validation."""

    def test_valid_entry_passes(self) -> None:
        entry = _make_entry()
        result = validate_log_entry(entry)
        assert result is entry

    def test_entry_without_metadata_passes(self) -> None:
        entry = _make_entry()
        del entry["metadata"]
        result = validate_log_entry(entry)
        assert result is entry

    def test_entry_with_empty_metadata_passes(self) -> None:
        entry = _make_entry(metadata={})
        result = validate_log_entry(entry)
        assert result is entry

    def test_all_valid_levels(self) -> None:
        for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            entry = _make_entry(level=level)
            assert validate_log_entry(entry) is entry


class TestMissingFields:
    """Required field checks."""

    @pytest.mark.parametrize(
        "field",
        ["timestamp", "service_name", "level", "message"],
    )
    def test_missing_required_field_raises(self, field: str) -> None:
        entry = _make_entry()
        del entry[field]
        with pytest.raises(ValidationError, match=f"Missing required field: '{field}'"):
            validate_log_entry(entry)


class TestInvalidTypes:
    """Type / value checks."""

    def test_not_a_dict_raises(self) -> None:
        with pytest.raises(ValidationError, match="Expected a dict"):
            validate_log_entry("not a dict")  # type: ignore[arg-type]

    def test_invalid_timestamp_type_raises(self) -> None:
        entry = _make_entry(timestamp="2024-01-01T00:00:00Z")
        with pytest.raises(ValidationError, match="must be a datetime"):
            validate_log_entry(entry)

    def test_invalid_level_raises(self) -> None:
        entry = _make_entry(level="TRACE")
        with pytest.raises(ValidationError, match="'level' must be one of"):
            validate_log_entry(entry)

    def test_empty_service_name_raises(self) -> None:
        entry = _make_entry(service_name="")
        with pytest.raises(ValidationError, match="non-empty string"):
            validate_log_entry(entry)

    def test_empty_message_raises(self) -> None:
        entry = _make_entry(message="")
        with pytest.raises(ValidationError, match="non-empty string"):
            validate_log_entry(entry)


class TestInvalidMetadata:
    """Metadata validation."""

    def test_metadata_non_dict_raises(self) -> None:
        entry = _make_entry(metadata=["not", "a", "dict"])
        with pytest.raises(ValidationError, match="'metadata' must be a dict"):
            validate_log_entry(entry)

    def test_metadata_non_string_value_raises(self) -> None:
        entry = _make_entry(metadata={"count": 42})
        with pytest.raises(ValidationError, match="must be a string"):
            validate_log_entry(entry)

    def test_metadata_non_string_key_raises(self) -> None:
        entry = _make_entry(metadata={123: "value"})
        with pytest.raises(ValidationError, match="metadata key must be a string"):
            validate_log_entry(entry)

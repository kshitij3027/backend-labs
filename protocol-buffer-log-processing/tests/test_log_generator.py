"""Tests for src.log_generator."""

from __future__ import annotations

from datetime import datetime

from src.log_generator import (
    LOG_LEVELS,
    SERVICE_NAMES,
    generate_log_batch,
    generate_log_entry,
)

REQUIRED_FIELDS = {"timestamp", "service_name", "level", "message", "metadata"}


class TestGenerateLogEntry:
    """Tests for generate_log_entry()."""

    def test_entry_has_all_required_fields(self, sample_log_entry: dict) -> None:
        assert set(sample_log_entry.keys()) == REQUIRED_FIELDS

    def test_entry_has_valid_log_level(self, sample_log_entry: dict) -> None:
        assert sample_log_entry["level"] in LOG_LEVELS

    def test_service_name_override(self) -> None:
        entry = generate_log_entry(service_name="custom-service")
        assert entry["service_name"] == "custom-service"

    def test_default_service_name_from_pool(self, sample_log_entry: dict) -> None:
        assert sample_log_entry["service_name"] in SERVICE_NAMES

    def test_timestamp_is_datetime(self, sample_log_entry: dict) -> None:
        assert isinstance(sample_log_entry["timestamp"], datetime)

    def test_metadata_is_dict_with_string_keys_and_values(
        self, sample_log_entry: dict
    ) -> None:
        meta = sample_log_entry["metadata"]
        assert isinstance(meta, dict)
        assert len(meta) >= 1
        for key, value in meta.items():
            assert isinstance(key, str), f"metadata key {key!r} is not a string"
            assert isinstance(value, str), f"metadata value {value!r} is not a string"

    def test_message_is_non_empty_string(self, sample_log_entry: dict) -> None:
        assert isinstance(sample_log_entry["message"], str)
        assert len(sample_log_entry["message"]) > 0


class TestGenerateLogBatch:
    """Tests for generate_log_batch()."""

    def test_batch_generates_correct_count(self, sample_log_batch: list[dict]) -> None:
        assert len(sample_log_batch) == 10

    def test_batch_with_explicit_count(self) -> None:
        batch = generate_log_batch(25)
        assert len(batch) == 25

    def test_batch_service_name_override(self) -> None:
        batch = generate_log_batch(5, service_name="test-svc")
        for entry in batch:
            assert entry["service_name"] == "test-svc"

    def test_batch_entries_have_required_fields(self) -> None:
        batch = generate_log_batch(3)
        for entry in batch:
            assert set(entry.keys()) == REQUIRED_FIELDS

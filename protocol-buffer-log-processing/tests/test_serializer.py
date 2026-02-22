"""Tests for src.serializer."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.log_generator import generate_log_batch, generate_log_entry
from src.serializer import (
    SerializationError,
    deserialize_json,
    deserialize_protobuf,
    serialize_json,
    serialize_protobuf,
)


def _normalize_microseconds(dt: datetime) -> datetime:
    """Truncate datetime to microsecond precision (protobuf resolution)."""
    return dt.replace(microsecond=dt.microsecond)


def _assert_entries_match(original: list[dict], restored: list[dict]) -> None:
    """Assert that *restored* entries match *original* (field by field)."""
    assert len(restored) == len(original)
    for orig, rest in zip(original, restored):
        # Timestamps: compare with microsecond precision
        orig_ts = _normalize_microseconds(orig["timestamp"])
        rest_ts = _normalize_microseconds(rest["timestamp"])
        # Strip tzinfo for comparison since JSON round-trip preserves tz
        # and protobuf returns UTC-aware datetimes.
        assert orig_ts.replace(tzinfo=None) == rest_ts.replace(tzinfo=None)

        assert rest["service_name"] == orig["service_name"]
        assert rest["level"] == orig["level"]
        assert rest["message"] == orig["message"]
        assert rest.get("metadata", {}) == orig.get("metadata", {})


# ---------------------------------------------------------------------------
# JSON round-trip
# ---------------------------------------------------------------------------


class TestJSONSerialization:
    """JSON serialize / deserialize round-trips."""

    def test_json_round_trip_single_entry(self) -> None:
        entries = [generate_log_entry()]
        data = serialize_json(entries)
        restored = deserialize_json(data)
        _assert_entries_match(entries, restored)

    def test_json_round_trip_batch(self) -> None:
        entries = generate_log_batch(20)
        data = serialize_json(entries)
        restored = deserialize_json(data)
        _assert_entries_match(entries, restored)

    def test_json_empty_batch(self) -> None:
        data = serialize_json([])
        restored = deserialize_json(data)
        assert restored == []

    def test_json_returns_bytes(self) -> None:
        data = serialize_json([generate_log_entry()])
        assert isinstance(data, bytes)


# ---------------------------------------------------------------------------
# Protobuf round-trip
# ---------------------------------------------------------------------------


class TestProtobufSerialization:
    """Protobuf serialize / deserialize round-trips."""

    def test_protobuf_round_trip_single_entry(self) -> None:
        entries = [generate_log_entry()]
        data = serialize_protobuf(entries)
        restored = deserialize_protobuf(data)
        _assert_entries_match(entries, restored)

    def test_protobuf_round_trip_batch(self) -> None:
        entries = generate_log_batch(20)
        data = serialize_protobuf(entries)
        restored = deserialize_protobuf(data)
        _assert_entries_match(entries, restored)

    def test_protobuf_empty_batch(self) -> None:
        data = serialize_protobuf([])
        restored = deserialize_protobuf(data)
        assert restored == []

    def test_protobuf_returns_bytes(self) -> None:
        data = serialize_protobuf([generate_log_entry()])
        assert isinstance(data, bytes)


# ---------------------------------------------------------------------------
# Size comparison
# ---------------------------------------------------------------------------


class TestSizeComparison:
    """Protobuf should be smaller than JSON for the same data."""

    def test_protobuf_smaller_than_json(self) -> None:
        entries = generate_log_batch(100)
        json_bytes = serialize_json(entries)
        proto_bytes = serialize_protobuf(entries)
        assert len(proto_bytes) < len(json_bytes), (
            f"Expected protobuf ({len(proto_bytes)} bytes) to be smaller "
            f"than JSON ({len(json_bytes)} bytes)"
        )


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSerializationErrors:
    """Error conditions raise SerializationError."""

    def test_deserialize_json_invalid_data(self) -> None:
        with pytest.raises(SerializationError, match="JSON deserialization failed"):
            deserialize_json(b"not valid json {{{{")

    def test_deserialize_protobuf_invalid_data(self) -> None:
        # ParseFromString on garbage data may not always raise, but an
        # obviously truncated/malformed payload should produce either
        # an empty result or raise.  We test that the function does not
        # raise an *unexpected* exception type.
        try:
            result = deserialize_protobuf(b"\xff\xfe\xfd")
            # If it didn't raise, the result should still be a list.
            assert isinstance(result, list)
        except SerializationError:
            pass  # also acceptable

    def test_serialize_protobuf_with_bad_entry_raises(self) -> None:
        bad_entries = [{"not": "a valid entry"}]
        with pytest.raises(SerializationError):
            serialize_protobuf(bad_entries)

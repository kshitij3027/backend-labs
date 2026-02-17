"""Tests for the batch serializer."""

from src.models import create_log_entry, entry_to_dict
from src.serializer import (
    MAGIC_HEADER,
    deserialize_batch,
    serialize_batch,
)


def _sample_entries() -> list[dict]:
    """Return a small list of realistic log-entry dicts for testing."""
    entries = [
        create_log_entry(level="INFO", message="server started"),
        create_log_entry(
            level="ERROR",
            message="connection refused",
            metadata={"host": "db.local", "port": 5432},
        ),
        create_log_entry(level="DEBUG", message="heartbeat ok", service="monitor"),
    ]
    return [entry_to_dict(e) for e in entries]


def test_round_trip_compressed():
    entries = _sample_entries()
    data = serialize_batch(entries, compress=True)
    result = deserialize_batch(data)
    assert result == entries


def test_round_trip_uncompressed():
    entries = _sample_entries()
    data = serialize_batch(entries, compress=False)
    result = deserialize_batch(data)
    assert result == entries


def test_magic_header_present():
    entries = _sample_entries()
    data = serialize_batch(entries, compress=True)
    assert data[:2] == MAGIC_HEADER


def test_no_magic_header_uncompressed():
    entries = _sample_entries()
    data = serialize_batch(entries, compress=False)
    assert data[:2] != MAGIC_HEADER


def test_empty_batch():
    data = serialize_batch([], compress=True)
    result = deserialize_batch(data)
    assert result == []

    data = serialize_batch([], compress=False)
    result = deserialize_batch(data)
    assert result == []


def test_deserialize_detects_compression():
    entries = _sample_entries()
    compressed_data = serialize_batch(entries, compress=True)

    # Verify the magic header and flags byte are present
    assert compressed_data[:2] == MAGIC_HEADER
    assert compressed_data[2] == 0x01

    # Verify decompression produces the correct entries
    result = deserialize_batch(compressed_data)
    assert result == entries

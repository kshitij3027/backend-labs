"""Tests for the batch splitter."""

import pytest
from src.splitter import split_batch, MAX_UDP_PAYLOAD
from src.serializer import deserialize_batch
from src.models import create_log_entry, entry_to_dict


def _small_batch() -> list[dict]:
    """Return a small batch of 5 log-entry dicts."""
    return [
        entry_to_dict(create_log_entry("INFO", f"short message {i}"))
        for i in range(5)
    ]


def _large_batch() -> list[dict]:
    """Return a large batch of 1000 entries with ~200-char messages."""
    return [
        entry_to_dict(
            create_log_entry("INFO", "X" * 200, metadata={"idx": i})
        )
        for i in range(1000)
    ]


def test_small_batch_single_chunk():
    entries = _small_batch()
    chunks = split_batch(entries)
    assert len(chunks) == 1
    assert len(chunks[0]) <= MAX_UDP_PAYLOAD


def test_all_chunks_under_limit():
    entries = _large_batch()
    chunks = split_batch(entries)
    for i, chunk in enumerate(chunks):
        assert len(chunk) <= MAX_UDP_PAYLOAD, (
            f"Chunk {i} is {len(chunk)} bytes, exceeds MAX_UDP_PAYLOAD"
        )


def test_all_chunks_deserializable():
    entries = _large_batch()
    chunks = split_batch(entries)
    for chunk in chunks:
        result = deserialize_batch(chunk)
        assert isinstance(result, list)
        assert len(result) > 0


def test_total_entries_preserved():
    entries = _large_batch()
    chunks = split_batch(entries)
    total = sum(len(deserialize_batch(chunk)) for chunk in chunks)
    assert total == len(entries)


def test_single_entry_not_split():
    entries = [entry_to_dict(create_log_entry("WARN", "only one entry"))]
    chunks = split_batch(entries)
    assert len(chunks) == 1
    result = deserialize_batch(chunks[0])
    assert result == entries


def test_uncompressed_split():
    entries = _large_batch()
    chunks = split_batch(entries, compress=False)
    for i, chunk in enumerate(chunks):
        assert len(chunk) <= MAX_UDP_PAYLOAD, (
            f"Uncompressed chunk {i} is {len(chunk)} bytes, exceeds MAX_UDP_PAYLOAD"
        )
    total = sum(len(deserialize_batch(chunk)) for chunk in chunks)
    assert total == len(entries)

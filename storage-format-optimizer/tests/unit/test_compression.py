"""Unit tests for :mod:`src.compression`.

Covers LZ4 frame round-trips (including the empty payload), the rule-based
:func:`codec_for_dtype` defaults, and the :class:`CompressionChooser` learning
path: a learned winner is remembered and reused, while a disabled chooser is a
no-op that falls back to the dtype default and stores nothing.
"""
from __future__ import annotations

import pyarrow as pa

from src.compression import (
    CompressionChooser,
    codec_for_dtype,
    frame_lz4,
    unframe_lz4,
)

LEARN_CODECS = {"SNAPPY", "GZIP", "ZSTD"}


def test_lz4_round_trip_non_empty() -> None:
    payload = b"hello world" * 100
    assert unframe_lz4(frame_lz4(payload)) == payload


def test_lz4_round_trip_empty() -> None:
    assert unframe_lz4(frame_lz4(b"")) == b""


def test_codec_for_dtype_mappings() -> None:
    assert codec_for_dtype("timestamp") == "SNAPPY"
    assert codec_for_dtype("category") == "GZIP"
    assert codec_for_dtype("message") == "ZSTD"
    assert codec_for_dtype("int") == "SNAPPY"
    assert codec_for_dtype("something_unknown") == "SNAPPY"


def _repetitive_table() -> pa.Table:
    # Deterministic, highly repetitive low-cardinality column.
    return pa.table({"level": ["INFO"] * 2000})


def test_chooser_learns_and_remembers_codec() -> None:
    chooser = CompressionChooser(enabled=True)
    table = _repetitive_table()

    learned = chooser.learn("k", "category", table)
    assert learned in LEARN_CODECS

    # The same winner must be remembered and surfaced via both accessors.
    assert chooser.learned_codec("k", "category") == learned
    assert chooser.codec_for("k", "category") == learned


def test_disabled_chooser_is_noop_and_stores_nothing() -> None:
    chooser = CompressionChooser(enabled=False)
    table = _repetitive_table()

    result = chooser.learn("k", "category", table)
    # Disabled -> dtype default, and nothing is remembered.
    assert result == codec_for_dtype("category")
    assert chooser.learned_codec("k", "category") is None

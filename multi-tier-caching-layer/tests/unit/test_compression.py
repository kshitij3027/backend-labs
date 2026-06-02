"""Unit tests for the value serialization + zstd compression codec.

Validates §3 Feature D: round-trip fidelity for both plain and compressed
encodings, a material (>=3x) size reduction on representative time-series
data, and self-describing decode via the leading flag byte.
"""
from __future__ import annotations

import pytest

from src.compression import (
    FLAG_PLAIN,
    FLAG_ZSTD,
    compress,
    decode_value,
    decompress,
    dumps_json,
    encode_value,
    loads_json,
)


# --- Round-trip fidelity -------------------------------------------------

@pytest.mark.parametrize(
    "value",
    [
        {"count": 7, "source": "api", "ok": True},
        [1, 2, 3, "four", 5.0, None],
        {
            "buckets": [{"ts": i, "value": float(i)} for i in range(20)],
            "meta": {"source": "api", "nested": {"deep": [True, False, None]}},
        },
        {},
        [],
        "a plain string value",
    ],
)
def test_round_trip_compressed(value: object) -> None:
    assert decode_value(encode_value(value, compress=True)) == value


@pytest.mark.parametrize(
    "value",
    [
        {"count": 7, "source": "api", "ok": True},
        [1, 2, 3, "four", 5.0, None],
        {
            "buckets": [{"ts": i, "value": float(i)} for i in range(20)],
            "meta": {"source": "api", "nested": {"deep": [True, False, None]}},
        },
        {},
        [],
        "a plain string value",
    ],
)
def test_round_trip_plain(value: object) -> None:
    assert decode_value(encode_value(value, compress=False)) == value


def test_dumps_loads_json_round_trip() -> None:
    obj = {"a": 1, "b": [1, 2, {"c": 3}]}
    assert loads_json(dumps_json(obj)) == obj


def test_dumps_json_is_compact() -> None:
    # Compact separators: no whitespace after ',' or ':'.
    assert dumps_json({"a": 1, "b": 2}) == b'{"a":1,"b":2}'


def test_compress_decompress_round_trip() -> None:
    raw = b"some repetitive payload " * 100
    assert decompress(compress(raw)) == raw


# --- Compression ratio on representative time-series ---------------------

def _time_series(n: int = 5000) -> list[dict[str, object]]:
    """A repetitive time-series, the workload §3D's 3-5x target targets."""
    return [{"ts": i, "value": 1.0, "source": "api"} for i in range(n)]


def test_compression_ratio_at_least_3x_on_time_series() -> None:
    series = _time_series()
    compressed = encode_value(series, compress=True)
    plain = encode_value(series, compress=False)

    # >=3x smaller — i.e. 3 * compressed size still fits under plain size.
    assert len(compressed) * 3 < len(plain)


def test_compressed_decodes_back_to_series() -> None:
    series = _time_series(1000)
    assert decode_value(encode_value(series, compress=True)) == series


# --- Self-describing decode (flag byte) ----------------------------------

def test_first_byte_flag_when_compressed() -> None:
    blob = encode_value({"x": 1}, compress=True)
    assert blob[0] == 1
    assert blob[0] == FLAG_ZSTD


def test_first_byte_flag_when_plain() -> None:
    blob = encode_value({"x": 1}, compress=False)
    assert blob[0] == 0
    assert blob[0] == FLAG_PLAIN


def test_decode_auto_detects_flag_without_hint() -> None:
    # decode_value is told nothing about compression; it reads byte 0 itself.
    compressed_blob = encode_value({"k": "v", "n": 42}, compress=True)
    plain_blob = encode_value({"k": "v", "n": 42}, compress=False)

    assert decode_value(compressed_blob) == {"k": "v", "n": 42}
    assert decode_value(plain_blob) == {"k": "v", "n": 42}


def test_decode_unknown_flag_raises_value_error() -> None:
    # Leading byte 0x09 is neither FLAG_PLAIN nor FLAG_ZSTD.
    with pytest.raises(ValueError):
        decode_value(b"\x09{\"x\":1}")


def test_decode_empty_raises_value_error() -> None:
    with pytest.raises(ValueError):
        decode_value(b"")

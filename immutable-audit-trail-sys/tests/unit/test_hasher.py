"""Unit tests for src.crypto.hasher.

These tests pin down the determinism guarantees that the rest of the audit
chain relies on: same logical payload → same bytes → same digest, regardless
of dict insertion order or nesting.
"""

import hashlib
import re

from src.crypto.hasher import (
    GENESIS_PREV_HASH,
    canonical_bytes,
    sha256_hex,
)


def test_canonical_bytes_is_order_stable():
    """Insertion order must not affect the canonical byte output."""
    assert canonical_bytes({"a": 1, "b": 2}) == canonical_bytes({"b": 2, "a": 1})


def test_canonical_bytes_handles_nested():
    """Order-stability must hold recursively for nested dicts."""
    assert canonical_bytes({"x": {"a": 1, "b": 2}}) == canonical_bytes(
        {"x": {"b": 2, "a": 1}}
    )


def test_canonical_bytes_no_whitespace():
    """Compact form: no spaces, no tabs, no newlines in the output."""
    out = canonical_bytes({"a": 1, "b": 2, "c": {"d": 3}})
    assert b" " not in out
    assert b"\t" not in out
    assert b"\n" not in out
    # Re-check via regex on the decoded string for completeness.
    assert not re.search(r"\s", out.decode("utf-8"))


def test_sha256_hex_known_vector():
    """sha256_hex({}) must equal SHA-256 over the literal bytes b'{}'."""
    expected = hashlib.sha256(b"{}").hexdigest()
    assert sha256_hex({}) == expected


def test_sha256_hex_changes_on_value_change():
    """Different values must produce different digests."""
    assert sha256_hex({"a": 1}) != sha256_hex({"a": 2})


def test_genesis_prev_hash_is_64_zeros():
    """GENESIS_PREV_HASH is the all-zeros 64-char hex sentinel for chain head."""
    assert GENESIS_PREV_HASH == "0" * 64
    assert len(GENESIS_PREV_HASH) == 64
    # Verify it parses as a hex string.
    int(GENESIS_PREV_HASH, 16)

"""Unit tests for the HMAC-SHA256 signer.

These tests exercise the canonical-JSON serializer plus the
``sign_payload`` / ``verify_payload`` round-trip, the
``load_signing_key`` validator, and confirm that tampering with
either the payload or the signature flips ``verify_payload`` to
``False``. A ``SimpleNamespace`` stands in for the real ``Settings``
object so the tests don't have to spin up pydantic-settings.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.signing.hmac_signer import (
    canonical_payload_json,
    load_signing_key,
    sign_payload,
    verify_payload,
)


# A 32-byte key (the minimum accepted length). Shared by every test
# that signs/verifies — the exact bytes don't matter as long as both
# sides of the round-trip use the same value.
KEY = b"unit-test-key-32-bytes-long-1234"


def test_sign_and_verify_round_trip() -> None:
    """Signing a payload and verifying with the same key + bytes returns True."""
    payload = {"framework": "SOX", "events": 17, "summary": {"admin_access": 4}}
    signature = sign_payload(payload, key=KEY)
    assert verify_payload(payload, signature, key=KEY) is True


def test_verify_fails_on_mutated_field() -> None:
    """Mutating a single value flips verification to False (tamper detection)."""
    payload = {"framework": "SOX", "events": 17, "summary": {"admin_access": 4}}
    signature = sign_payload(payload, key=KEY)
    # Change one field — the recomputed digest no longer matches.
    payload["events"] = 18
    assert verify_payload(payload, signature, key=KEY) is False


def test_verify_fails_on_mutated_signature() -> None:
    """Flipping one hex char in the signature flips verification to False."""
    payload = {"framework": "HIPAA", "events": 9}
    signature = sign_payload(payload, key=KEY)
    # Mutate exactly one hex character. Pick the first char and roll it
    # to a guaranteed-different value so the digest can't accidentally
    # equal the original.
    first = signature[0]
    replacement = "a" if first != "a" else "b"
    tampered = replacement + signature[1:]
    assert tampered != signature
    assert verify_payload(payload, tampered, key=KEY) is False


def test_short_key_raises() -> None:
    """A signing key shorter than 32 bytes is rejected at load time."""
    settings = SimpleNamespace(hmac_signing_key="too-short-key-16")
    assert len(settings.hmac_signing_key) == 16
    with pytest.raises(ValueError, match="at least 32 bytes"):
        load_signing_key(settings)


def test_canonical_json_is_order_independent() -> None:
    """Insertion order of dict keys must not affect the canonical bytes."""
    first = canonical_payload_json({"a": 1, "b": 2})
    second = canonical_payload_json({"b": 2, "a": 1})
    assert first == second
    # Sanity check the serialization is tight (no extra whitespace).
    assert first == '{"a":1,"b":2}'


def test_canonical_json_handles_datetime() -> None:
    """A datetime in the payload is stringified instead of raising TypeError."""
    now = datetime(2026, 5, 26, 12, 30, 0, tzinfo=timezone.utc)
    payload = {"framework": "GDPR", "generated_at": now}
    # No exception means default=str engaged.
    serialized = canonical_payload_json(payload)
    # The ISO date should appear somewhere in the serialized output.
    assert "2026-05-26" in serialized

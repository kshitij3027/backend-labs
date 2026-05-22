"""Unit tests for src.crypto.signer.

These tests verify Ed25519 sign/verify round-trips, deterministic signing
behaviour, and that bad inputs (wrong key length, malformed base64, tampered
signatures, or wrong messages) are rejected cleanly.
"""

import base64
import os

import pytest

from src.crypto.signer import Ed25519Signer, Ed25519Verifier


def _fresh_seed_b64() -> str:
    """Generate a fresh base64-encoded 32-byte Ed25519 seed."""
    return base64.b64encode(os.urandom(32)).decode("ascii")


# ---------------------------------------------------------------------------
# Ed25519Signer construction
# ---------------------------------------------------------------------------


def test_signer_init_rejects_bad_base64():
    """Malformed base64 input must raise (binascii.Error subclasses ValueError)."""
    with pytest.raises(ValueError):
        Ed25519Signer("not-base64-!@#")


def test_signer_init_rejects_wrong_length():
    """Seeds that don't decode to exactly 32 bytes must be rejected."""
    short_seed = base64.b64encode(os.urandom(16)).decode("ascii")
    with pytest.raises(ValueError):
        Ed25519Signer(short_seed)


# ---------------------------------------------------------------------------
# Ed25519Signer.sign
# ---------------------------------------------------------------------------


def test_sign_returns_base64_string():
    """sign() output must be a non-empty str that decodes to 64 raw bytes."""
    signer = Ed25519Signer(_fresh_seed_b64())
    sig = signer.sign("a" * 64)

    assert isinstance(sig, str)
    assert sig != ""

    raw = base64.b64decode(sig)
    assert len(raw) == 64  # Ed25519 signature is exactly 64 bytes.


def test_sign_is_deterministic():
    """Signing the same input twice with the same key must yield identical output."""
    signer = Ed25519Signer(_fresh_seed_b64())
    self_hash = "abcd" * 16  # 64-char hex-shaped string
    assert signer.sign(self_hash) == signer.sign(self_hash)


# ---------------------------------------------------------------------------
# Ed25519Verifier round-trip
# ---------------------------------------------------------------------------


def test_verify_round_trip():
    """Sign with a fresh key, then verify with the matching public key."""
    signer = Ed25519Signer(_fresh_seed_b64())
    verifier = Ed25519Verifier(signer.public_key_b64())

    value = "deadbeef" * 8  # 64-char hex-shaped string
    sig = signer.sign(value)

    assert verifier.verify(sig, value) is True


def test_verify_rejects_tampered_signature():
    """Flipping a byte in the signature must yield False, not raise."""
    signer = Ed25519Signer(_fresh_seed_b64())
    verifier = Ed25519Verifier(signer.public_key_b64())

    value = "feedface" * 8
    sig = signer.sign(value)

    # Tamper with the raw signature bytes (flip the first byte's low bit),
    # then re-encode. This guarantees a structurally valid base64 string
    # of the right length but with a wrong signature payload.
    raw = bytearray(base64.b64decode(sig))
    raw[0] ^= 0x01
    tampered = base64.b64encode(bytes(raw)).decode("ascii")
    assert tampered != sig

    assert verifier.verify(tampered, value) is False


def test_verify_rejects_wrong_message():
    """A valid signature for value A must not verify against value B."""
    signer = Ed25519Signer(_fresh_seed_b64())
    verifier = Ed25519Verifier(signer.public_key_b64())

    value_a = "aaaa" * 16
    value_b = "bbbb" * 16
    sig = signer.sign(value_a)

    assert verifier.verify(sig, value_b) is False


# ---------------------------------------------------------------------------
# Ed25519Verifier construction
# ---------------------------------------------------------------------------


def test_verifier_init_rejects_wrong_length():
    """Public keys that don't decode to exactly 32 bytes must be rejected."""
    short_key = base64.b64encode(os.urandom(16)).decode("ascii")
    with pytest.raises(ValueError):
        Ed25519Verifier(short_key)

"""HMAC-SHA256 signing primitives for tamper-evident compliance reports.

Reports are signed by serialising their payload dict into a canonical
JSON form (``sort_keys=True``, tight separators, ``default=str`` for
datetimes / UUIDs) and HMAC-SHA256-ing the bytes with a server-side
key. Verification recomputes the digest and compares with
``hmac.compare_digest`` so signature checks are constant-time and
therefore safe against timing-side-channel attacks.

The FinHealth framework signs each report twice — once with the
primary key and once with the HIPAA-scoped secondary key — to satisfy
the "dual signature" requirement without introducing asymmetric
crypto. Both keys are utf-8 strings of >= 32 bytes; shorter inputs
are rejected at load time.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


def canonical_payload_json(payload: dict[str, Any]) -> str:
    """Deterministic JSON serialization for signing.

    sort_keys=True so dict-insertion order can't change the bytes signed.
    separators tight so trailing whitespace doesn't affect the digest.
    default=str so datetime/UUID values become strings instead of raising.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def sign_payload(payload: dict[str, Any], *, key: bytes) -> str:
    """Return the HMAC-SHA256 hex digest of canonical_payload_json(payload) signed with key."""
    canonical = canonical_payload_json(payload).encode("utf-8")
    return hmac.new(key, canonical, hashlib.sha256).hexdigest()


def verify_payload(payload: dict[str, Any], signature_hex: str, *, key: bytes) -> bool:
    """Constant-time verify of payload against signature_hex using key."""
    expected = sign_payload(payload, key=key)
    return hmac.compare_digest(expected, signature_hex)


def load_signing_key(settings) -> bytes:
    """Return the primary HMAC signing key as bytes. Raises ValueError if < 32 bytes."""
    raw = settings.hmac_signing_key.strip()
    if len(raw) < 32:
        raise ValueError("HMAC signing key must be at least 32 bytes long")
    return raw.encode("utf-8")


def load_secondary_signing_key(settings) -> bytes:
    """Return the secondary HMAC signing key as bytes (for FinHealth dual-sign)."""
    raw = settings.hmac_signing_key_secondary.strip()
    if len(raw) < 32:
        raise ValueError("HMAC secondary signing key must be at least 32 bytes long")
    return raw.encode("utf-8")

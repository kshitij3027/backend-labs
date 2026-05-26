"""Signing and at-rest encryption primitives.

Re-exports the HMAC-SHA256 signer and Fernet store helpers so callers
can import from ``src.signing`` directly without reaching into the
submodules. Each helper is documented in its module of origin.
"""
from __future__ import annotations

from .fernet_store import decrypt_file_bytes, encrypt_file_in_place, load_or_create_fernet
from .hmac_signer import (
    load_secondary_signing_key,
    load_signing_key,
    sign_payload,
    verify_payload,
)

__all__ = [
    "sign_payload",
    "verify_payload",
    "load_signing_key",
    "load_secondary_signing_key",
    "load_or_create_fernet",
    "encrypt_file_in_place",
    "decrypt_file_bytes",
]

"""Cryptographic primitives for field-level encryption.

Public surface:

* :class:`EncryptedField`   — pydantic record describing one encrypted leaf
* :class:`AESGCMEncryptor`  — AES-256-GCM encrypt/decrypt of a single payload
* :class:`KeyProvider`      — KEK abstraction (wrap/unwrap DEKs)
* :class:`EnvKeyProvider`   — KEK loaded from the ``MASTER_KEY_B64`` env var
* :func:`generate_dek`      — helper that returns a fresh 32-byte DEK

The split keeps the keystore (C4) and log processor (C5) independent
of the underlying primitive: both code paths talk only to these names,
never to ``cryptography`` directly. That means swapping in a future
KMS-backed :class:`KeyProvider` requires no changes outside this module.
"""
from __future__ import annotations

from .aesgcm import AESGCMEncryptor, generate_dek
from .key_provider import EnvKeyProvider, KeyProvider
from .schema import EncryptedField

__all__ = [
    "AESGCMEncryptor",
    "EncryptedField",
    "EnvKeyProvider",
    "KeyProvider",
    "generate_dek",
]

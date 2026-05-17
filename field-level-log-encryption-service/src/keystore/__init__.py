"""Versioned, in-process DEK store with lifecycle management.

Public surface:

* :class:`KeyStore`            — holds DEK versions, supports rotation
* :class:`KeyRecord`           — one DEK version + its lifecycle metadata
* :data:`KeyStatus`            — the ``active|retired|destroyed`` literal
* :class:`KeyStoreError`       — base exception for keystore failures
* :class:`KeyNotFoundError`    — lookup by id missed (or no active key yet)
* :class:`KeyDestroyedError`   — caller tried to decrypt with a shredded key
* :class:`RotationManager`     — decides when to call :meth:`KeyStore.rotate`

The keystore composes the C3 primitives (``KeyProvider`` + ``AESGCMEncryptor``):
new encrypts go through the *active* :class:`KeyRecord`'s encryptor; decrypts
look up the matching :class:`KeyRecord` by ``key_id`` so retired keys remain
readable until they are explicitly destroyed (crypto-shredded). The processor
layer (C5) and the HTTP wiring (C7) only ever touch the names re-exported here.
"""
from __future__ import annotations

from .rotator import RotationManager
from .store import (
    KeyDestroyedError,
    KeyNotFoundError,
    KeyRecord,
    KeyStatus,
    KeyStore,
    KeyStoreError,
)

__all__ = [
    "KeyDestroyedError",
    "KeyNotFoundError",
    "KeyRecord",
    "KeyStatus",
    "KeyStore",
    "KeyStoreError",
    "RotationManager",
]

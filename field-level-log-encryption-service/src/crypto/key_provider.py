"""KEK abstraction for wrapping/unwrapping data encryption keys.

We use **envelope encryption**: a long-lived Key Encryption Key (KEK)
protects short-lived Data Encryption Keys (DEKs). Field ciphertext is
encrypted under a DEK; the DEK itself is encrypted under the KEK and
stored alongside the DEK record. This lets us rotate DEKs cheaply (just
generate a new one and wrap it) without ever touching the KEK or
re-encrypting field data.

The :class:`KeyProvider` interface is intentionally narrow — three
methods — so production deployments can swap :class:`EnvKeyProvider`
out for a real KMS (AWS KMS, GCP KMS, HashiCorp Vault, ...) by
implementing the same three methods. Nothing else in the codebase
should depend on KEK material directly.

Wrap format
-----------
Wrapped DEKs are stored as a single byte string laid out as::

    nonce_12  || ciphertext  || tag_16

The ``cryptography`` library appends the 16-byte GCM tag to the
ciphertext for us, so ``unwrap_dek`` simply splits off the first 12
bytes as the nonce and feeds the rest to AESGCM. A constant AAD
(``b"dek-wrap-v1"``) binds wrapped blobs to this format version — if
we ever change the layout we can bump the AAD string and refuse to
unwrap stale blobs.
"""
from __future__ import annotations

import base64
import os
from abc import ABC, abstractmethod

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


# Wrap-format identifier. Bumping this string invalidates every old
# wrapped DEK (they'd hit `InvalidTag`), which is exactly the desired
# behaviour if we ever change the on-disk wrap layout.
_WRAP_AAD = b"dek-wrap-v1"


class KeyProvider(ABC):
    """Abstract KEK that can wrap/unwrap DEKs.

    Implementations MUST be safe to call from multiple threads — the
    keystore (C4) may unwrap DEKs from any worker thread.
    """

    @abstractmethod
    def wrap_dek(self, dek: bytes) -> bytes:
        """Encrypt ``dek`` under the KEK and return the wrapped blob.

        Output format is implementation-defined but MUST round-trip
        through :meth:`unwrap_dek`.
        """

    @abstractmethod
    def unwrap_dek(self, wrapped: bytes) -> bytes:
        """Decrypt ``wrapped`` and return the original DEK bytes.

        Raises
        ------
        cryptography.exceptions.InvalidTag
            If the wrapped blob was produced under a different KEK,
            was tampered with, or was wrapped under a different AAD
            (i.e., a different wrap-format version).
        """

    @abstractmethod
    def kek_id(self) -> str:
        """Short identifier for the current KEK version (e.g. ``env-v1``)."""


class EnvKeyProvider(KeyProvider):
    """KEK loaded from the ``MASTER_KEY_B64`` environment variable.

    The KEK bytes are decoded once at construction and held in memory
    inside a single cached :class:`AESGCM` instance. The AES key
    schedule is amortized across every wrap/unwrap call.

    Parameters
    ----------
    kek_b64 : str | None, optional
        Base64-encoded 32-byte KEK. If ``None`` (the default), the
        value is sourced from :data:`src.settings.settings.master_key_b64`.
        Passing an explicit value is useful for tests that need to
        construct a *second* provider with a different KEK to verify
        cross-KEK isolation.

    Raises
    ------
    ValueError
        If the decoded key is not exactly 32 bytes (AES-256 requires
        a 256-bit key).
    """

    def __init__(self, kek_b64: str | None = None) -> None:
        if kek_b64 is None:
            # Import locally so test-time monkeypatching of `settings`
            # is respected; importing at module top would freeze the
            # value at first import.
            from src.settings import settings

            kek_b64 = settings.master_key_b64

        kek = base64.b64decode(kek_b64)
        if len(kek) != 32:
            raise ValueError(
                f"KEK must decode to exactly 32 bytes (AES-256), got {len(kek)}"
            )

        # Stash the bytes so subclasses / debug helpers can read the
        # length, but never log them. The cached AESGCM is the only
        # consumer of the material in normal operation.
        self._kek = kek
        self._aesgcm = AESGCM(self._kek)

    # -- KeyProvider interface ------------------------------------------

    def wrap_dek(self, dek: bytes) -> bytes:
        """Wrap a DEK under the KEK with a fresh 12-byte nonce.

        Returns ``nonce || ciphertext || tag`` as a single byte string.
        """
        nonce = os.urandom(12)
        ct_and_tag = self._aesgcm.encrypt(nonce, dek, _WRAP_AAD)
        return nonce + ct_and_tag

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        """Reverse :meth:`wrap_dek`. Raises ``InvalidTag`` on auth failure."""
        nonce = wrapped[:12]
        ct = wrapped[12:]
        return self._aesgcm.decrypt(nonce, ct, _WRAP_AAD)

    def kek_id(self) -> str:
        return "env-v1"

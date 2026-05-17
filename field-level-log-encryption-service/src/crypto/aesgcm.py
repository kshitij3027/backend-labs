"""AES-256-GCM single-field encryptor.

An :class:`AESGCMEncryptor` is a thin wrapper around the
:class:`cryptography.hazmat.primitives.ciphers.aead.AESGCM` primitive
that:

* Pins the algorithm to AES-256-GCM by validating the DEK length (32B).
* Caches one ``AESGCM`` instance so the AES key schedule is computed
  exactly once per DEK — at ~5-30 microseconds per encrypt this matters.
* Generates a fresh 12-byte nonce per call (NIST SP 800-38D recommends
  96-bit random nonces with the ``2^32 messages`` safety margin).
* Binds the ciphertext to its record + field path via Additional
  Authenticated Data (AAD) — see :meth:`AESGCMEncryptor._build_aad`.
* Returns an :class:`~src.crypto.schema.EncryptedField` ready to be
  spliced into a transformed log record.

The encryptor is **stateless** beyond its cached AESGCM instance and is
safe to share across threads — ``AESGCM.encrypt/decrypt`` release the
GIL via the underlying OpenSSL call, so this is the right object to
hand to the parallel pool in C5.

AAD design
----------
The AAD is::

    f"{key_id}|{record_id}|{field_path}".encode("utf-8")

This binds three properties into the ciphertext authenticator:

1. **key_id** — prevents replaying a ciphertext encrypted under a
   different DEK as if it were ours.
2. **record_id** — prevents ciphertext-swap attacks between two
   logs encrypted with the same DEK.
3. **field_path** — prevents swapping ciphertext between two fields
   of the same record (e.g., copying the encrypted ``email`` into
   the ``ssn`` slot).

Any of these differing at decrypt time triggers ``InvalidTag``.
"""
from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.crypto.schema import EncryptedField


def generate_dek() -> bytes:
    """Generate a fresh 256-bit random DEK.

    Thin wrapper around :meth:`AESGCM.generate_key` so callers (notably
    the C4 keystore) don't have to import the primitive directly.
    """
    return AESGCM.generate_key(bit_length=256)


class AESGCMEncryptor:
    """Encrypt/decrypt a single field's bytes payload with AES-256-GCM.

    One encryptor binds one DEK; rotation in the keystore (C4) hands
    out a fresh encryptor per DEK version. The instance caches its
    ``AESGCM`` to amortize the AES key schedule across all calls.

    Parameters
    ----------
    dek : bytes
        32-byte data encryption key. Anything else raises ``ValueError``.
    key_id : str
        Short identifier for the DEK version (matches
        :attr:`EncryptedField.key_id`).

    Raises
    ------
    ValueError
        If ``dek`` is not exactly 32 bytes.
    """

    def __init__(self, dek: bytes, key_id: str) -> None:
        if len(dek) != 32:
            raise ValueError(
                f"DEK must be exactly 32 bytes (AES-256), got {len(dek)}"
            )
        # Cache the AESGCM instance: ``cryptography`` runs the AES key
        # schedule on construction. Reusing the instance is the point
        # of this class.
        self._aesgcm = AESGCM(dek)
        self._key_id = key_id

    # -- public ----------------------------------------------------------

    @property
    def key_id(self) -> str:
        """The DEK version identifier this encryptor was built with."""
        return self._key_id

    def encrypt(
        self,
        plaintext: bytes,
        *,
        record_id: str,
        field_path: str,
        field_type: str,
    ) -> EncryptedField:
        """Encrypt ``plaintext`` and return a serializable record.

        A fresh random 12-byte nonce is generated for every call.

        Parameters
        ----------
        plaintext : bytes
            Already UTF-8-encoded (or raw-bytes) field value.
        record_id : str
            Identifies the parent log record (used in AAD binding).
        field_path : str
            Dotted JSON path to the leaf (used in AAD binding).
        field_type : str
            Detector-provided PII type label; carried into the output.
        """
        nonce = os.urandom(12)
        aad = self._build_aad(record_id=record_id, field_path=field_path)
        ct_and_tag = self._aesgcm.encrypt(nonce, plaintext, aad)
        return EncryptedField.from_raw(
            ciphertext_and_tag=ct_and_tag,
            nonce=nonce,
            key_id=self._key_id,
            field_type=field_type,
        )

    def decrypt(
        self,
        ef: EncryptedField,
        *,
        record_id: str,
        field_path: str,
    ) -> bytes:
        """Decrypt ``ef`` and return the original plaintext bytes.

        The caller MUST supply the same ``record_id`` and ``field_path``
        used at encrypt time — any divergence fails the AAD check and
        raises :class:`cryptography.exceptions.InvalidTag`.

        Raises
        ------
        ValueError
            If the record's algorithm is not AES-256-GCM, or if its
            ``key_id`` does not match this encryptor's. (Wrong-key
            routing should be caught by the keystore upstream; we
            double-check here so a mis-wired caller fails loudly
            before invoking the cipher.)
        cryptography.exceptions.InvalidTag
            On tag mismatch — wrong DEK, mutated ciphertext, mutated
            AAD, mutated nonce, or any combination.
        """
        if ef.algorithm != "AES-256-GCM":
            raise ValueError(
                f"Unsupported algorithm {ef.algorithm!r}; expected AES-256-GCM"
            )
        if ef.key_id != self._key_id:
            raise ValueError(
                f"key_id mismatch: encryptor={self._key_id!r}, "
                f"ciphertext={ef.key_id!r}"
            )
        aad = self._build_aad(record_id=record_id, field_path=field_path)
        return self._aesgcm.decrypt(ef.raw_iv(), ef.raw_ciphertext_and_tag(), aad)

    # -- internal --------------------------------------------------------

    def _build_aad(self, *, record_id: str, field_path: str) -> bytes:
        """Return the canonical AAD for this encryptor.

        Encoded UTF-8 of ``"{key_id}|{record_id}|{field_path}"``. Any
        of those three differing between encrypt and decrypt will
        cause GCM to reject the tag.
        """
        return f"{self._key_id}|{record_id}|{field_path}".encode("utf-8")

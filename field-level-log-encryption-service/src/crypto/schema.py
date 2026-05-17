"""Pydantic v2 schema for an encrypted field-at-rest record.

An :class:`EncryptedField` is the JSON-serializable shape that replaces a
detected PII leaf in a transformed log. It carries everything a future
decryption pass needs:

* ``encrypted_value`` — base64 of the concatenated ``ciphertext || tag``
  blob produced by AES-256-GCM. The 16-byte authentication tag is
  appended to the ciphertext by the underlying ``cryptography`` library,
  so we do **not** store it separately.
* ``iv`` — base64 of the 12-byte random nonce used for this specific
  encryption. Nonces are required at decrypt time and MUST be unique
  per (key, message); they are not secret.
* ``algorithm`` — pinned to ``"AES-256-GCM"`` so an older record that
  predates a future algorithm migration is unambiguously identified.
* ``key_id`` — names the DEK version this field was encrypted under.
  The keystore (C4) keeps retired DEKs decryptable as long as they
  aren't destroyed.
* ``field_type`` — propagated from the upstream detector
  (:class:`src.detection.patterns.Detection`) so downstream consumers
  know *what kind* of PII was here without decrypting (``"email"``,
  ``"ssn"``, ...).
* ``encryption_timestamp`` — UTC instant the ciphertext was produced;
  useful for forensics and time-based key rotation audits.

The model is sealed with ``extra="forbid"`` so any future schema drift
fails fast at validation time rather than silently swallowing unknown
fields.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict


class EncryptedField(BaseModel):
    """A single AES-256-GCM encrypted field, serialized for storage/transport.

    This is the on-the-wire and at-rest representation. The raw bytes are
    base64-encoded so the model round-trips through JSON without binary
    escaping; helpers :meth:`raw_ciphertext_and_tag` and :meth:`raw_iv`
    reverse the encoding for the encryptor.

    Attributes
    ----------
    encrypted_value : str
        Base64 of ``ciphertext || tag`` (the 16-byte GCM tag is
        appended to the ciphertext by ``cryptography``'s ``AESGCM``).
    algorithm : Literal["AES-256-GCM"]
        Pinned algorithm identifier. Validation rejects any other value
        so old records remain unambiguously typed if we ever add a new
        cipher suite.
    key_id : str
        DEK version identifier (see :mod:`src.keystore.store` in C4).
    iv : str
        Base64 of the 12-byte nonce used for this encryption.
    field_type : str
        Canonical PII type label from the detector — e.g. ``"email"``,
        ``"ssn"``. Not used by crypto; carried for downstream tooling.
    encryption_timestamp : datetime
        UTC moment the ciphertext was produced.
    """

    encrypted_value: str
    algorithm: Literal["AES-256-GCM"] = "AES-256-GCM"
    key_id: str
    iv: str
    field_type: str
    encryption_timestamp: datetime

    # `extra="forbid"` makes the model intolerant of unknown keys — a
    # tampered record with an extra field will fail validation rather
    # than silently dropping the field. We want loud failure here.
    model_config = ConfigDict(extra="forbid")

    # -- constructors ----------------------------------------------------

    @classmethod
    def from_raw(
        cls,
        ciphertext_and_tag: bytes,
        nonce: bytes,
        key_id: str,
        field_type: str,
    ) -> "EncryptedField":
        """Construct an :class:`EncryptedField` from raw crypto outputs.

        Base64-encodes the binary parts and stamps the current UTC
        timestamp. This is the path the encryptor uses; callers building
        the model from already-base64-encoded data should use the
        regular pydantic constructor (or :meth:`model_validate`).

        Parameters
        ----------
        ciphertext_and_tag : bytes
            The concatenated ciphertext + 16-byte GCM tag as returned by
            ``AESGCM.encrypt(...)``.
        nonce : bytes
            The 12-byte random nonce used for this encryption.
        key_id : str
            DEK version identifier.
        field_type : str
            PII type label from the detector.
        """
        return cls(
            encrypted_value=base64.b64encode(ciphertext_and_tag).decode("ascii"),
            iv=base64.b64encode(nonce).decode("ascii"),
            key_id=key_id,
            field_type=field_type,
            encryption_timestamp=datetime.now(timezone.utc),
        )

    # -- accessors -------------------------------------------------------

    def raw_ciphertext_and_tag(self) -> bytes:
        """Return the base64-decoded ``ciphertext || tag`` blob."""
        return base64.b64decode(self.encrypted_value)

    def raw_iv(self) -> bytes:
        """Return the base64-decoded 12-byte nonce."""
        return base64.b64decode(self.iv)

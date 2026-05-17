"""Unit tests for the C3 crypto primitives.

Coverage targets:

* :class:`TestEncryptedFieldSchema` — pydantic shape, round-trip, and the
  pinned ``algorithm`` literal.
* :class:`TestEnvKeyProvider`       — KEK wrap/unwrap, isolation between
  different KEKs, ``kek_id``, key-length validation.
* :class:`TestAESGCMEncryptor`      — round-trip on multiple sizes, nonce
  randomness, tamper detection, AAD binding (record_id + field_path),
  key-id mismatch, DEK length validation, ``generate_dek`` helper.

All tests use the zero-byte test KEK injected by :mod:`tests.conftest`,
except those that explicitly need a second distinct KEK.
"""
from __future__ import annotations

import base64
import os

import pytest
from cryptography.exceptions import InvalidTag
from pydantic import ValidationError

from src.crypto import (
    AESGCMEncryptor,
    EncryptedField,
    EnvKeyProvider,
    KeyProvider,
    generate_dek,
)
from src.crypto.key_provider import _WRAP_AAD


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

ALT_KEK_B64 = base64.b64encode(b"\xff" * 32).decode("ascii")  # not the test KEK
SHORT_KEK_B64 = base64.b64encode(b"\x00" * 16).decode("ascii")  # wrong length


def _make_encryptor(key_id: str = "k1") -> AESGCMEncryptor:
    """Build an encryptor with a deterministic DEK for tests that don't
    care about the key bytes themselves (round-trip, tamper, AAD)."""
    return AESGCMEncryptor(dek=b"\x42" * 32, key_id=key_id)


# ---------------------------------------------------------------------------
# TestEncryptedFieldSchema — pydantic model contract
# ---------------------------------------------------------------------------


class TestEncryptedFieldSchema:
    """The :class:`EncryptedField` pydantic model and its helpers."""

    def test_from_raw_populates_all_fields(self) -> None:
        ct = b"\x01\x02\x03ciphertext-bytes\xaa\xbb"
        nonce = b"\x10" * 12
        ef = EncryptedField.from_raw(
            ciphertext_and_tag=ct,
            nonce=nonce,
            key_id="k1",
            field_type="email",
        )
        assert ef.algorithm == "AES-256-GCM"
        assert ef.key_id == "k1"
        assert ef.field_type == "email"
        # Base64 is reversible — verify accessors give us the bytes back.
        assert ef.raw_ciphertext_and_tag() == ct
        assert ef.raw_iv() == nonce
        # Timestamp is UTC-aware (datetime.now(timezone.utc) is tz-aware).
        assert ef.encryption_timestamp.utcoffset() is not None
        assert ef.encryption_timestamp.utcoffset().total_seconds() == 0

    def test_model_dump_model_validate_round_trip(self) -> None:
        ef = EncryptedField.from_raw(
            ciphertext_and_tag=b"\xde\xad\xbe\xef" * 4,
            nonce=b"\x01" * 12,
            key_id="key-2026-05-16",
            field_type="ssn",
        )
        payload = ef.model_dump(mode="json")
        # All required keys present.
        assert {
            "encrypted_value",
            "algorithm",
            "key_id",
            "iv",
            "field_type",
            "encryption_timestamp",
        }.issubset(payload.keys())

        restored = EncryptedField.model_validate(payload)
        assert restored.encrypted_value == ef.encrypted_value
        assert restored.iv == ef.iv
        assert restored.key_id == ef.key_id
        assert restored.field_type == ef.field_type
        assert restored.algorithm == "AES-256-GCM"
        assert restored.raw_ciphertext_and_tag() == ef.raw_ciphertext_and_tag()
        assert restored.raw_iv() == ef.raw_iv()

    def test_algorithm_literal_rejects_other_values(self) -> None:
        # Crafting a payload with a non-pinned algorithm string must
        # fail validation — this is the schema's first defence against
        # a future record sneaking in under a weaker cipher.
        bad_payload = {
            "encrypted_value": base64.b64encode(b"x" * 32).decode("ascii"),
            "algorithm": "AES-128-CTR",  # not allowed
            "key_id": "k1",
            "iv": base64.b64encode(b"\x00" * 12).decode("ascii"),
            "field_type": "email",
            "encryption_timestamp": "2026-05-16T00:00:00+00:00",
        }
        with pytest.raises(ValidationError):
            EncryptedField.model_validate(bad_payload)

    def test_raw_accessors_reverse_base64(self) -> None:
        # Construct directly (not via from_raw) and check that the
        # accessors are pure base64 reversals.
        ct = os.urandom(48)
        nonce = os.urandom(12)
        ef = EncryptedField(
            encrypted_value=base64.b64encode(ct).decode("ascii"),
            key_id="kx",
            iv=base64.b64encode(nonce).decode("ascii"),
            field_type="phone",
            encryption_timestamp="2026-05-16T12:00:00+00:00",
        )
        assert ef.raw_ciphertext_and_tag() == ct
        assert ef.raw_iv() == nonce

    def test_extra_fields_forbidden(self) -> None:
        # `model_config = ConfigDict(extra="forbid")` should reject
        # unknown keys (loud failure for schema drift).
        payload = {
            "encrypted_value": base64.b64encode(b"x" * 32).decode("ascii"),
            "algorithm": "AES-256-GCM",
            "key_id": "k1",
            "iv": base64.b64encode(b"\x00" * 12).decode("ascii"),
            "field_type": "email",
            "encryption_timestamp": "2026-05-16T00:00:00+00:00",
            "unexpected_extra": "boom",
        }
        with pytest.raises(ValidationError):
            EncryptedField.model_validate(payload)


# ---------------------------------------------------------------------------
# TestEnvKeyProvider — KEK wrap/unwrap and isolation
# ---------------------------------------------------------------------------


class TestEnvKeyProvider:
    """The :class:`EnvKeyProvider` envelope-encryption KEK."""

    def test_wrap_unwrap_round_trip(self) -> None:
        provider = EnvKeyProvider()  # uses the zero-byte test KEK
        dek = generate_dek()
        wrapped = provider.wrap_dek(dek)
        # Wrap output should be: 12B nonce || ciphertext || 16B tag.
        # DEK is 32B → expect 12 + 32 + 16 = 60 bytes.
        assert len(wrapped) == 60
        assert provider.unwrap_dek(wrapped) == dek

    def test_wrap_uses_fresh_nonce_each_call(self) -> None:
        # Two wraps of the SAME DEK under the SAME KEK should still
        # produce different outputs — proof the 12B nonce is random.
        provider = EnvKeyProvider()
        dek = generate_dek()
        wrapped_a = provider.wrap_dek(dek)
        wrapped_b = provider.wrap_dek(dek)
        assert wrapped_a != wrapped_b
        # Nonces (first 12 bytes) must differ specifically.
        assert wrapped_a[:12] != wrapped_b[:12]

    def test_different_kek_cannot_unwrap(self) -> None:
        # A blob wrapped under KEK_A must not unwrap under KEK_B.
        provider_a = EnvKeyProvider()
        provider_b = EnvKeyProvider(kek_b64=ALT_KEK_B64)
        wrapped = provider_a.wrap_dek(generate_dek())
        with pytest.raises(InvalidTag):
            provider_b.unwrap_dek(wrapped)

    def test_kek_id_is_env_v1(self) -> None:
        # Pinning this string protects downstream callers that filter
        # on `kek_id()` for audit/forensics.
        assert EnvKeyProvider().kek_id() == "env-v1"

    def test_invalid_key_length_rejected(self) -> None:
        # AES-256 requires a 32-byte key; 16 bytes (AES-128) must
        # fail loudly at construction, not silently downgrade.
        with pytest.raises(ValueError, match="32 bytes"):
            EnvKeyProvider(kek_b64=SHORT_KEK_B64)

    def test_tampered_wrapped_dek_fails(self) -> None:
        # Flipping a byte in the wrapped blob breaks the GCM tag.
        provider = EnvKeyProvider()
        wrapped = bytearray(provider.wrap_dek(generate_dek()))
        # Flip a bit in the middle (ciphertext region, after the nonce).
        wrapped[20] ^= 0x01
        with pytest.raises(InvalidTag):
            provider.unwrap_dek(bytes(wrapped))

    def test_is_subclass_of_key_provider(self) -> None:
        # EnvKeyProvider must satisfy the KeyProvider ABC so downstream
        # code can rely on the interface.
        assert issubclass(EnvKeyProvider, KeyProvider)
        assert isinstance(EnvKeyProvider(), KeyProvider)

    def test_wrap_aad_constant_pinned(self) -> None:
        # Sanity check that the wrap-format constant hasn't drifted —
        # any change here invalidates every previously-wrapped DEK.
        assert _WRAP_AAD == b"dek-wrap-v1"


# ---------------------------------------------------------------------------
# TestAESGCMEncryptor — field-level encrypt/decrypt
# ---------------------------------------------------------------------------


class TestAESGCMEncryptor:
    """The :class:`AESGCMEncryptor` single-field cipher."""

    # ---- happy-path round-trips, multiple sizes ------------------------

    def test_round_trip_small_payload(self) -> None:
        enc = _make_encryptor()
        plaintext = b"user@example.com" + b"x" * 16  # 32 bytes
        ef = enc.encrypt(
            plaintext,
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        assert enc.decrypt(ef, record_id="r1", field_path="user.email") == plaintext

    def test_round_trip_medium_payload_1kb(self) -> None:
        enc = _make_encryptor()
        plaintext = b"A" * 1024
        ef = enc.encrypt(
            plaintext,
            record_id="r1",
            field_path="payload",
            field_type="text",
        )
        assert enc.decrypt(ef, record_id="r1", field_path="payload") == plaintext

    def test_round_trip_large_payload_100kb(self) -> None:
        # AESGCM is happy with anything up to 2^39 - 256 bits; 100KB is
        # well within bounds. This guards against silent buffer-size bugs.
        enc = _make_encryptor()
        plaintext = os.urandom(100 * 1024)
        ef = enc.encrypt(
            plaintext,
            record_id="r1",
            field_path="blob",
            field_type="binary",
        )
        assert enc.decrypt(ef, record_id="r1", field_path="blob") == plaintext

    # ---- nonce randomness ----------------------------------------------

    def test_two_encrypts_produce_different_nonces_and_ciphertext(self) -> None:
        # Same key, same plaintext, same AAD — but nonces are random,
        # so both `iv` and `encrypted_value` must differ. (If they
        # didn't, we'd be vulnerable to a two-time-pad disaster.)
        enc = _make_encryptor()
        plaintext = b"shared-plaintext"
        ef_a = enc.encrypt(
            plaintext, record_id="r", field_path="f", field_type="email"
        )
        ef_b = enc.encrypt(
            plaintext, record_id="r", field_path="f", field_type="email"
        )
        assert ef_a.iv != ef_b.iv
        assert ef_a.encrypted_value != ef_b.encrypted_value

    # ---- tamper detection ----------------------------------------------

    def test_tampered_ciphertext_fails_authentication(self) -> None:
        # Flip a single bit of the ciphertext+tag blob and verify the
        # GCM tag check rejects it. This is the property that prevents
        # an attacker from doctoring a record at rest.
        enc = _make_encryptor()
        ef = enc.encrypt(
            b"sensitive-bytes",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        raw = bytearray(ef.raw_ciphertext_and_tag())
        raw[0] ^= 0x01  # flip the first bit
        tampered = ef.model_copy(
            update={"encrypted_value": base64.b64encode(bytes(raw)).decode("ascii")}
        )
        with pytest.raises(InvalidTag):
            enc.decrypt(tampered, record_id="r1", field_path="user.email")

    # ---- AAD binding ---------------------------------------------------

    def test_decrypt_with_wrong_record_id_fails(self) -> None:
        # AAD includes record_id — decrypting with a different
        # record_id must fail authentication, blocking ciphertext-
        # swap attacks across records.
        enc = _make_encryptor()
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        with pytest.raises(InvalidTag):
            enc.decrypt(ef, record_id="r2", field_path="user.email")

    def test_decrypt_with_wrong_field_path_fails(self) -> None:
        # AAD includes field_path — decrypting with a different path
        # must fail, blocking ciphertext swaps between fields of the
        # same record (e.g., moving the encrypted email into the ssn
        # slot).
        enc = _make_encryptor()
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        with pytest.raises(InvalidTag):
            enc.decrypt(ef, record_id="r1", field_path="user.phone")

    # ---- key-id mismatch -----------------------------------------------

    def test_decrypt_rejects_mismatched_key_id_before_aesgcm(self) -> None:
        # An encryptor with key_id="k1" must refuse to decrypt an
        # EncryptedField whose key_id is "k2". This is a ValueError
        # raised by the encryptor itself BEFORE AESGCM is invoked —
        # not an InvalidTag — because the keystore should route to
        # the right encryptor and a mismatch here means a logic bug.
        enc = _make_encryptor(key_id="k1")
        # Fabricate a valid-looking EncryptedField but stamped with a
        # different key_id (the actual ciphertext was made with k1).
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        wrong_key_ef = ef.model_copy(update={"key_id": "k2"})
        with pytest.raises(ValueError, match="key_id mismatch"):
            enc.decrypt(wrong_key_ef, record_id="r1", field_path="user.email")

    def test_decrypt_rejects_unsupported_algorithm(self) -> None:
        # Bypass schema validation via model_construct to simulate a
        # legacy record claiming a different algorithm — encryptor
        # should refuse before invoking the cipher.
        enc = _make_encryptor(key_id="k1")
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        # model_copy with an unknown algorithm would fail schema
        # validation, so we bypass with model_construct.
        bad = EncryptedField.model_construct(
            **{**ef.model_dump(), "algorithm": "AES-128-CTR"}
        )
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            enc.decrypt(bad, record_id="r1", field_path="user.email")

    # ---- key length validation -----------------------------------------

    def test_short_dek_rejected(self) -> None:
        with pytest.raises(ValueError, match="32 bytes"):
            AESGCMEncryptor(dek=b"\x00" * 16, key_id="k1")

    def test_oversize_dek_rejected(self) -> None:
        with pytest.raises(ValueError, match="32 bytes"):
            AESGCMEncryptor(dek=b"\x00" * 64, key_id="k1")

    # ---- generate_dek --------------------------------------------------

    def test_generate_dek_returns_32_bytes(self) -> None:
        dek = generate_dek()
        assert isinstance(dek, bytes)
        assert len(dek) == 32

    def test_generate_dek_returns_distinct_keys(self) -> None:
        # Birthday-paradox space is 2^256; two consecutive calls
        # colliding would be a generator failure, not bad luck.
        assert generate_dek() != generate_dek()

    # ---- key_id propagation --------------------------------------------

    def test_encryptor_key_id_propagates_into_record(self) -> None:
        enc = _make_encryptor(key_id="dek-2026-05-16-abc123")
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )
        assert ef.key_id == "dek-2026-05-16-abc123"
        assert enc.key_id == "dek-2026-05-16-abc123"

    def test_field_type_propagates_into_record(self) -> None:
        # field_type is opaque to the cipher but carried through the
        # record so downstream consumers know the PII category
        # without decrypting.
        enc = _make_encryptor()
        ef = enc.encrypt(
            b"secret",
            record_id="r1",
            field_path="user.ssn",
            field_type="ssn",
        )
        assert ef.field_type == "ssn"

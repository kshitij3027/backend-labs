"""Unit tests for the Fernet at-rest encryption helpers.

These tests use pytest's ``tmp_path`` fixture so on-disk artefacts live
in an isolated temp directory and get cleaned up automatically. A
``SimpleNamespace`` stands in for the real ``Settings`` object — the
loader only reads ``fernet_encryption_key``, so a one-attribute
namespace is sufficient.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from cryptography.fernet import Fernet

from src.signing.fernet_store import (
    decrypt_file_bytes,
    encrypt_file_in_place,
    load_or_create_fernet,
)


def test_encrypt_decrypt_round_trip(tmp_path: Path) -> None:
    """Plaintext bytes survive a write -> encrypt-in-place -> decrypt round trip."""
    plaintext = b"this is a synthetic compliance report payload\n"
    target = tmp_path / "report.bin"
    target.write_bytes(plaintext)

    # Build a Fernet from a freshly minted key so the test is self-contained.
    key = Fernet.generate_key()
    fernet = Fernet(key)

    encrypt_file_in_place(target, fernet)

    # After encryption, the on-disk bytes must NOT equal the plaintext.
    ciphertext = target.read_bytes()
    assert ciphertext != plaintext

    # Decryption must restore the original bytes exactly.
    recovered = decrypt_file_bytes(target, fernet)
    assert recovered == plaintext


def test_blank_key_auto_generates_and_works(tmp_path: Path) -> None:
    """A blank fernet_encryption_key triggers auto-generation; round-trip still works."""
    settings = SimpleNamespace(fernet_encryption_key="")
    fernet = load_or_create_fernet(settings)
    # The auto-generated Fernet is a real, usable instance.
    assert isinstance(fernet, Fernet)

    plaintext = b"auto-key round trip"
    target = tmp_path / "auto.bin"
    target.write_bytes(plaintext)

    encrypt_file_in_place(target, fernet)
    assert target.read_bytes() != plaintext
    assert decrypt_file_bytes(target, fernet) == plaintext


def test_provided_key_used_as_is(tmp_path: Path) -> None:
    """A pre-generated key passed via settings is used verbatim and round-trips."""
    raw_key = Fernet.generate_key()
    settings = SimpleNamespace(fernet_encryption_key=raw_key.decode("utf-8"))
    fernet = load_or_create_fernet(settings)
    assert isinstance(fernet, Fernet)

    plaintext = b"provided-key round trip"
    target = tmp_path / "provided.bin"
    target.write_bytes(plaintext)

    encrypt_file_in_place(target, fernet)
    assert target.read_bytes() != plaintext

    # A *separate* Fernet built from the same raw key must decrypt the file —
    # this proves the loader actually used the supplied key, not a fresh one.
    sibling = Fernet(raw_key)
    assert decrypt_file_bytes(target, sibling) == plaintext


def test_encrypt_returns_size(tmp_path: Path) -> None:
    """encrypt_file_in_place returns the new (post-encryption) on-disk size."""
    plaintext = b"size assertion payload"
    target = tmp_path / "sized.bin"
    target.write_bytes(plaintext)

    fernet = Fernet(Fernet.generate_key())
    size = encrypt_file_in_place(target, fernet)

    # The returned size must match the actual on-disk size after writing
    # ciphertext back. Fernet ciphertext is strictly longer than the
    # plaintext, so this also implicitly checks we returned the *new* size.
    assert size == target.stat().st_size
    assert size > len(plaintext)

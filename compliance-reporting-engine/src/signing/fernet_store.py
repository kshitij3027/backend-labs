"""Fernet symmetric encryption for at-rest report files.

Each exported report file (PDF / CSV / JSON / XML) is encrypted in
place after the signer has run, so on-disk artefacts are unreadable
without the operator-supplied ``FERNET_ENCRYPTION_KEY``. Decryption
happens transparently when a caller hits the ``/reports/{id}/download``
endpoint.

If the operator leaves ``FERNET_ENCRYPTION_KEY`` blank the loader
generates a fresh ephemeral key and logs a WARNING with the value —
this is convenient for local dev but unsafe for production because a
regenerated key on restart would render every existing on-disk report
permanently undecryptable.
"""
from __future__ import annotations

from pathlib import Path

from cryptography.fernet import Fernet

from ..logging_config import get_logger

logger = get_logger("signing.fernet")


def load_or_create_fernet(settings) -> Fernet:
    """Return a Fernet instance using settings.fernet_encryption_key, or auto-generate.

    If the key is blank the function generates a fresh one and logs a WARNING
    so the operator knows to persist it for restarts (a regenerated key on
    restart would make all on-disk reports undecryptable). In production the
    operator MUST supply a persistent key via env var.
    """
    raw = (settings.fernet_encryption_key or "").strip()
    if not raw:
        key = Fernet.generate_key()
        logger.warning(
            "fernet_key_auto_generated",
            message="No FERNET_ENCRYPTION_KEY set — generated an ephemeral key. "
                    "Existing encrypted files will be unreadable after restart unless "
                    "this value is persisted to the environment.",
            generated_key=key.decode("utf-8"),
        )
        return Fernet(key)
    return Fernet(raw.encode("utf-8"))


def encrypt_file_in_place(path: Path, fernet: Fernet) -> int:
    """Read plaintext from path, encrypt, write back, return new byte size."""
    plaintext = path.read_bytes()
    ciphertext = fernet.encrypt(plaintext)
    path.write_bytes(ciphertext)
    return len(ciphertext)


def decrypt_file_bytes(path: Path, fernet: Fernet) -> bytes:
    """Return the decrypted bytes for an encrypted file at path."""
    return fernet.decrypt(path.read_bytes())

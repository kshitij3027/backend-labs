"""Bcrypt password hashing/verification helpers.

The module-level CryptContext is the single source of truth for hash settings.
Round count defaults to bcrypt library default (12) — sufficient for a demo,
slow enough to be safe.
"""
from __future__ import annotations

from passlib.context import CryptContext

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Return a bcrypt hash for the given plaintext password."""
    if not plain:
        raise ValueError("password must not be empty")
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Return True if `plain` matches `hashed`, else False. Constant-time."""
    if not plain or not hashed:
        return False
    try:
        return _pwd_context.verify(plain, hashed)
    except Exception:
        # passlib raises on malformed hashes; treat as a verification failure, not a crash.
        return False

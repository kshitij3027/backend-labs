from __future__ import annotations

import os

os.environ.setdefault("SECRET_KEY", "test-secret-key-please-change")

from src.auth.security import hash_password, verify_password


def test_hash_password_returns_bcrypt_format() -> None:
    hashed = hash_password("demo")
    assert hashed.startswith("$2")
    assert len(hashed) >= 50


def test_verify_password_round_trip_correct_password() -> None:
    hashed = hash_password("demo")
    assert verify_password("demo", hashed) is True


def test_verify_password_rejects_wrong_password() -> None:
    hashed = hash_password("demo")
    assert verify_password("wrong", hashed) is False


def test_verify_password_rejects_empty_hash() -> None:
    assert verify_password("demo", "") is False


def test_hash_password_produces_unique_salts() -> None:
    h1 = hash_password("demo")
    h2 = hash_password("demo")
    assert h1 != h2
    assert verify_password("demo", h1) is True
    assert verify_password("demo", h2) is True

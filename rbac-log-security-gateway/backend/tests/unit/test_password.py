"""Unit tests for bcrypt password hashing helpers."""
import pytest

from src.auth.passwords import hash_password, verify_password


def test_hash_differs_from_plaintext() -> None:
    plain = "correct horse battery staple"
    hashed = hash_password(plain)
    assert hashed != plain
    assert len(hashed) > 0
    # bcrypt hashes start with the algorithm identifier
    assert hashed.startswith(("$2a$", "$2b$", "$2y$"))


def test_verify_returns_true_for_matching_password() -> None:
    plain = "admin123"
    hashed = hash_password(plain)
    assert verify_password(plain, hashed) is True


def test_verify_returns_false_for_wrong_password() -> None:
    hashed = hash_password("admin123")
    assert verify_password("wrong-password", hashed) is False


def test_hash_uses_random_salt() -> None:
    """Two hashes of the same plaintext must differ (salt randomness)."""
    plain = "same-password"
    assert hash_password(plain) != hash_password(plain)


def test_verify_returns_false_for_malformed_inputs() -> None:
    """Empty or garbage hashes must not crash; they must return False."""
    assert verify_password("", "") is False
    assert verify_password("anything", "") is False
    assert verify_password("", hash_password("x")) is False
    assert verify_password("x", "not-a-real-hash") is False


def test_empty_plaintext_rejected_by_hash() -> None:
    """hash_password should refuse to hash an empty string."""
    with pytest.raises(ValueError):
        hash_password("")

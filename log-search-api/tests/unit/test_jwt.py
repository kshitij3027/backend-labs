from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

os.environ.setdefault("SECRET_KEY", "test-secret-key-please-change")

import jwt
import pytest
from fastapi import HTTPException

from src.auth.security import create_access_token, decode_token
from src.config import Settings


def _settings(secret: str = "test-secret-key-please-change") -> Settings:
    return Settings(SECRET_KEY=secret)


def test_create_access_token_round_trip() -> None:
    settings = _settings()
    token, expires_at = create_access_token("alice", settings)
    payload = decode_token(token, settings)
    assert payload["sub"] == "alice"
    assert payload["type"] == "access"
    assert isinstance(expires_at, datetime)
    assert expires_at.tzinfo is not None
    now = datetime.now(UTC)
    assert expires_at > now
    assert expires_at <= now + timedelta(minutes=settings.ACCESS_TOKEN_TTL_MINUTES + 1)


def test_decode_token_rejects_tampered_payload() -> None:
    settings = _settings()
    token, _ = create_access_token("alice", settings)
    parts = token.split(".")
    tampered = ".".join([parts[0], parts[1][:-2] + ("AA" if not parts[1].endswith("AA") else "BB"), parts[2]])
    with pytest.raises(HTTPException) as excinfo:
        decode_token(tampered, settings)
    assert excinfo.value.status_code == 401
    assert "invalid" in str(excinfo.value.detail).lower()


def test_decode_token_rejects_wrong_signing_key() -> None:
    signer_settings = _settings(secret="signing-key-aaaaaaaaaaaaa")
    verifier_settings = _settings(secret="verifier-key-bbbbbbbbbbbb")
    token, _ = create_access_token("alice", signer_settings)
    with pytest.raises(HTTPException) as excinfo:
        decode_token(token, verifier_settings)
    assert excinfo.value.status_code == 401
    assert "invalid" in str(excinfo.value.detail).lower()


def test_decode_token_rejects_expired_token() -> None:
    settings = _settings()
    now = datetime.now(UTC)
    expired_payload = {
        "sub": "alice",
        "iat": now - timedelta(hours=2),
        "exp": now - timedelta(hours=1),
        "type": "access",
    }
    expired_token = jwt.encode(
        expired_payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM
    )
    with pytest.raises(HTTPException) as excinfo:
        decode_token(expired_token, settings)
    assert excinfo.value.status_code == 401
    assert "expired" in str(excinfo.value.detail).lower()


def test_decode_token_rejects_garbage_string() -> None:
    settings = _settings()
    with pytest.raises(HTTPException) as excinfo:
        decode_token("not.a.jwt", settings)
    assert excinfo.value.status_code == 401

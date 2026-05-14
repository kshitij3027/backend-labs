"""Unit tests for JWT encode/decode."""
import time
from datetime import datetime, timezone

import pytest

from src.auth.jwt import InvalidTokenError, decode_token, encode_token


def test_round_trip_encodes_and_decodes() -> None:
    token, expires_at = encode_token(user_id="u1", username="alice", roles=("administrator",))
    payload = decode_token(token)
    assert payload["sub"] == "u1"
    assert payload["username"] == "alice"
    assert payload["roles"] == ["administrator"]
    assert isinstance(expires_at, datetime)
    assert expires_at.tzinfo is timezone.utc


def test_decode_rejects_garbage_token() -> None:
    with pytest.raises(InvalidTokenError):
        decode_token("not.a.jwt")


def test_decode_rejects_expired_token() -> None:
    """Encode with a negative expiry — should already be expired."""
    token, _exp = encode_token(user_id="u1", username="alice", roles=(), expires_minutes=-1)
    with pytest.raises(InvalidTokenError):
        decode_token(token)


def test_decode_rejects_tampered_signature() -> None:
    token, _exp = encode_token(user_id="u1", username="alice", roles=())
    # Flip a byte in the signature portion (last segment of the JWT).
    head, payload, sig = token.split(".")
    tampered = f"{head}.{payload}.{sig[:-2] + 'XY'}"
    with pytest.raises(InvalidTokenError):
        decode_token(tampered)


def test_expiry_minutes_is_respected() -> None:
    token, expires_at = encode_token(
        user_id="u1", username="alice", roles=(), expires_minutes=5
    )
    now = datetime.now(timezone.utc)
    delta = (expires_at - now).total_seconds()
    # 5 minutes = 300 seconds. Allow ±5s tolerance for test scheduling jitter.
    assert 290 < delta < 310

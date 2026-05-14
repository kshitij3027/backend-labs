"""JWT encode/decode helpers using python-jose. Uses HS256 by default."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt

from src.config import get_settings


class InvalidTokenError(Exception):
    """Raised when a token is malformed, expired, or signature-invalid."""


def encode_token(
    *,
    user_id: str,
    username: str,
    roles: tuple[str, ...] | list[str],
    expires_minutes: int | None = None,
) -> tuple[str, datetime]:
    """Return (token, expires_at). Expiry defaults to settings.jwt_expiry_minutes."""
    settings = get_settings()
    now = datetime.now(timezone.utc)
    exp_minutes = expires_minutes if expires_minutes is not None else settings.jwt_expiry_minutes
    expires_at = now + timedelta(minutes=exp_minutes)
    payload = {
        "sub": user_id,
        "username": username,
        "roles": list(roles),
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return token, expires_at


def decode_token(token: str) -> dict:
    """Decode + verify. Raises InvalidTokenError on any issue."""
    settings = get_settings()
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        raise InvalidTokenError(str(exc)) from exc

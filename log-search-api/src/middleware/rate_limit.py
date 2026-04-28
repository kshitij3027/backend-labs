from __future__ import annotations

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.requests import Request

from src.config import Settings, get_settings


def user_or_ip_key(request: Request) -> str:
    user_id = getattr(request.state, "user_id", None)
    if isinstance(user_id, str) and user_id:
        return user_id
    return get_remote_address(request)


def _build_limiter(settings: Settings) -> Limiter:
    storage_uri = f"{settings.REDIS_URL}/{settings.RATE_LIMIT_REDIS_DB}"
    return Limiter(
        key_func=user_or_ip_key,
        storage_uri=storage_uri,
        headers_enabled=True,
        strategy="fixed-window",
    )


def make_limiter(settings: Settings) -> Limiter:
    return _build_limiter(settings)


def default_limit_string(settings: Settings) -> str:
    return f"{settings.RATE_LIMIT_REQUESTS}/{settings.RATE_LIMIT_WINDOW_SECONDS} second"


_settings = get_settings()
limiter = _build_limiter(_settings)
DEFAULT_LIMIT = default_limit_string(_settings)


__all__ = [
    "DEFAULT_LIMIT",
    "Limiter",
    "RateLimitExceeded",
    "_rate_limit_exceeded_handler",
    "default_limit_string",
    "limiter",
    "make_limiter",
    "user_or_ip_key",
]

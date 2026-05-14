"""AuthService: thin wrapper that combines UserStore + JWT issuance."""
from __future__ import annotations

from datetime import datetime

from src.auth.jwt import encode_token
from src.auth.users import User, UserStore, default_store


class AuthenticationError(Exception):
    """Raised when login fails (bad username or password)."""


class AuthService:
    def __init__(self, user_store: UserStore = default_store) -> None:
        self._users = user_store

    def login(self, username: str, password: str) -> tuple[str, datetime, User]:
        """Authenticate + issue a JWT. Returns (token, expires_at, user) or raises."""
        user = self._users.authenticate(username, password)
        if user is None:
            raise AuthenticationError("invalid credentials")
        token, expires_at = encode_token(
            user_id=user.user_id,
            username=user.username,
            roles=user.roles,
        )
        return token, expires_at, user

    def user_from_username(self, username: str) -> User | None:
        return self._users.get(username)

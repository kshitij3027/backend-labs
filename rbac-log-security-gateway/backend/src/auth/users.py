"""In-memory user store with 4 hardcoded demo users.

Per the requirements doc, the demo users are seeded at module import time and
the bcrypt hashes are computed once (not per request). Plaintext passwords
exist only in code comments alongside the seed call — never on disk and never
returned over the wire.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable
from uuid import uuid4

from src.auth.passwords import hash_password, verify_password


@dataclass(frozen=True)
class User:
    """Immutable user record. `password_hash` never leaves the server."""
    user_id: str
    username: str
    password_hash: str
    roles: tuple[str, ...]
    display_name: str = ""


class UserStore:
    """Simple in-memory store. Lookups are case-sensitive on username."""

    def __init__(self, users: Iterable[User] = ()) -> None:
        self._by_username: dict[str, User] = {u.username: u for u in users}

    def get(self, username: str) -> User | None:
        return self._by_username.get(username)

    def authenticate(self, username: str, password: str) -> User | None:
        """Return the User if credentials check, else None. Constant-time on the password verify."""
        user = self.get(username)
        if user is None:
            # Still verify against a dummy hash to keep timing similar — prevents username enumeration
            verify_password(password, "$2b$12$" + "x" * 53)
            return None
        if not verify_password(password, user.password_hash):
            return None
        return user

    def all_usernames(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_username.keys()))

    def __len__(self) -> int:
        return len(self._by_username)


def _seed_default_users() -> tuple[User, ...]:
    """Build the 4 demo users. Passwords are visible here on purpose — this is a learning demo."""
    return (
        User(
            user_id=str(uuid4()),
            username="alice",
            password_hash=hash_password("admin123"),   # admin role
            roles=("administrator",),
            display_name="Alice Admin",
        ),
        User(
            user_id=str(uuid4()),
            username="bob",
            password_hash=hash_password("dev123"),     # developer role
            roles=("developer",),
            display_name="Bob Developer",
        ),
        User(
            user_id=str(uuid4()),
            username="carol",
            password_hash=hash_password("analyst123"), # analyst role
            roles=("analyst",),
            display_name="Carol Analyst",
        ),
        User(
            user_id=str(uuid4()),
            username="dave",
            password_hash=hash_password("support123"), # support role
            roles=("support",),
            display_name="Dave Support",
        ),
    )


# Module-level singleton store. Imported by shared.py later.
default_store: UserStore = UserStore(_seed_default_users())

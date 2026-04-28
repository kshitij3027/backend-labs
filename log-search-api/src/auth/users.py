from __future__ import annotations

import logging

from src.auth.security import verify_password
from src.config import Settings

logger = logging.getLogger(__name__)


class SeededUserStore:
    def __init__(self, username: str, password_hash: str) -> None:
        self.username = username
        self.password_hash = password_hash

    def authenticate(self, username: str, password: str) -> bool:
        if not self.username or not self.password_hash:
            logger.warning("seeded user store is not configured")
            return False
        return username == self.username and verify_password(password, self.password_hash)


def get_user_store(settings: Settings) -> SeededUserStore:
    return SeededUserStore(settings.SEED_USERNAME, settings.SEED_PASSWORD_HASH)

"""Shared pytest fixtures: Settings, an app with an injected Runtime, and a TestClient.

The ``app`` fixture injects a pre-built :class:`src.main.Runtime` into
:func:`src.main.create_app`, which makes the app skip the FastAPI lifespan entirely — no
startup work, no corpus seeding, no environment dependency — so tests exercise the HTTP
surface hermetically.

The ``settings`` fixture constructs :class:`~src.config.Settings` **directly** rather than
mutating global environment state. Two reasons: the compose ``test`` service already pins
SEED_ENTRIES/STORE_CAPACITY for the container, and monkeypatching ``os.environ`` in one test
leaks into the ``get_settings`` LRU cache that another test reads. Anything that genuinely
needs a fresh global calls ``get_settings.cache_clear()`` explicitly.
"""

import os

# IMPORTANT — this MUST run before `src` is imported. `src.main` builds its module-level
# ``app`` at import time, which calls ``get_settings()``, which rejects a missing JWT_SECRET
# (that is the README's "no usable default" contract). ``setdefault`` means the compose
# `test` service's own JWT_SECRET still wins when present; this only makes a bare
# `pytest` run outside Docker work too.
os.environ.setdefault("JWT_SECRET", "test-only-insecure-signing-key-0123456789")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.config import Settings  # noqa: E402
from src.main import Runtime, create_app  # noqa: E402

#: A valid (long enough, non-placeholder) signing key for tests. Never used outside them.
TEST_JWT_SECRET = "test-only-insecure-signing-key-0123456789"


@pytest.fixture()
def settings() -> Settings:
    """Directly-constructed Settings for a small, fast, deterministic test process.

    ``store_capacity`` is tiny and ``seed_entries`` is 0 so tests that need a corpus build one
    explicitly; ``bcrypt_rounds=4`` drops a password hash from ~250 ms to ~2 ms, which is the
    difference between a suite that runs in seconds and one that runs in minutes.
    """
    return Settings(
        jwt_secret=TEST_JWT_SECRET,
        store_capacity=1000,
        seed_entries=0,
        bcrypt_rounds=4,
    )


@pytest.fixture()
def app(settings: Settings):
    """A FastAPI app wired to a fresh, unseeded Runtime (lifespan skipped)."""
    return create_app(runtime=Runtime.build(settings))


@pytest.fixture()
def client(app) -> TestClient:
    """A synchronous TestClient against the injected-runtime app."""
    return TestClient(app)

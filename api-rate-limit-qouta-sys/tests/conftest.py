"""Shared pytest fixtures: Settings, an app with an injected Runtime, and a TestClient.

The ``app`` fixture injects a pre-built :class:`src.main.Runtime` into
:func:`src.main.create_app`, which makes the app skip the FastAPI lifespan entirely — no startup
work, no Redis connection, no environment dependency — so tests exercise the HTTP surface
hermetically.

The ``settings`` fixture constructs :class:`~src.config.Settings` **directly** rather than mutating
global environment state. Two reasons: kwargs outrank both the environment and ``.env`` in
pydantic-settings' source order, so a directly-constructed Settings is the only kind whose values
a test can actually assert on; and monkeypatching ``os.environ`` in one test leaks into the
``get_settings`` LRU cache that another test reads. Anything that genuinely needs a fresh global
calls ``get_settings.cache_clear()`` explicitly.
"""

import os

# IMPORTANT — this MUST run before `src` is imported. `src.main` builds its module-level ``app``
# at import time, which calls ``get_settings()``, which rejects a missing or placeholder value for
# any of the three secrets (that is the "refuses to start" contract, and it is enforced by
# ``validate_default=True`` so it fires even when nothing was set at all).
#
# ``setdefault``, not assignment, so an operator- or compose-supplied value still wins. The compose
# `test` service deliberately sets none of these: declaring the suite's credentials here — in the
# file the tests actually read — means a bare `pytest` run outside Docker behaves identically to
# one inside it, and there is exactly one place to look for them.
os.environ.setdefault("JWT_SECRET", "test-only-insecure-signing-key-0123456789")
os.environ.setdefault("API_KEY_PEPPER", "test-only-insecure-pepper-0123456789")
os.environ.setdefault("ADMIN_TOKEN", "test-only-insecure-admin-token-0123456789")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.config import Settings  # noqa: E402
from src.main import Runtime, create_app  # noqa: E402

#: Valid (long enough, non-placeholder) secrets for tests. Never used outside them. Distinct
#: values per field so a test that accidentally compares the wrong one fails instead of passing.
TEST_JWT_SECRET = "test-only-insecure-signing-key-0123456789"
TEST_API_KEY_PEPPER = "test-only-insecure-pepper-0123456789"
TEST_ADMIN_TOKEN = "test-only-insecure-admin-token-0123456789"


@pytest.fixture()
def settings() -> Settings:
    """Directly-constructed Settings for a fast, deterministic test process.

    ``_env_file=None`` cuts the ``.env`` source out entirely, so nothing on the developer's disk
    or in the tester image can influence what these tests are asserting about. Every other field
    keeps its declared default, which is the point: the suite should be testing the shipped
    configuration, not a bespoke one.
    """
    return Settings(
        _env_file=None,
        jwt_secret=TEST_JWT_SECRET,
        api_key_pepper=TEST_API_KEY_PEPPER,
        admin_token=TEST_ADMIN_TOKEN,
    )


@pytest.fixture()
def app(settings: Settings):
    """A FastAPI app wired to a fresh Runtime (lifespan skipped)."""
    return create_app(runtime=Runtime.build(settings))


@pytest.fixture()
def client(app) -> TestClient:
    """A synchronous TestClient against the injected-runtime app."""
    return TestClient(app)

"""Shared pytest fixtures: Settings, an app built from them, and a TestClient.

.. rubric:: Why ``settings`` constructs :class:`~src.config.Settings` directly

Not by monkeypatching ``os.environ``, and not by writing a ``.env``. Two reasons, and the second
is the one that actually bites:

* The compose ``test`` service already pins the container's environment (a separate
  ``gqllogs_test`` database, ``SEED_ENTRIES=0``), so tests inherit a sane baseline and only have
  to state what they are *changing*.
* Mutating the environment in one test leaks into the :func:`~src.config.get_settings` LRU cache
  that a later test reads, and the failure shows up in whichever test happens to run next — a
  class of flake that is very hard to attribute. Constructing the object directly has no global
  to leak through at all.

``_env_file=None`` is passed so the fixture cannot be perturbed by a stray ``.env`` in the working
directory when the suite is run on a host rather than in the tester image. Environment variables
still apply, which is deliberate: from C2 the integration tests need the real ``DATABASE_URL`` and
``REDIS_URL`` that compose injects.

.. rubric:: Why the cache is cleared around every test

:func:`~src.config.get_settings` is ``@lru_cache``'d for the life of the process, and pytest runs
the whole suite in one process. Without the autouse fixture below, the first test that calls it
would freeze the configuration every later test sees — including tests that deliberately set an
environment variable to observe the override.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config import Settings, get_settings
from src.main import create_app


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Iterator[None]:
    """Drop the process-wide settings cache before and after every test.

    Both sides matter: clearing *before* means a test never inherits a Settings object built from
    another test's environment, and clearing *after* means this test's environment cannot escape
    into the next one through the cache.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture()
def settings() -> Settings:
    """Directly-constructed Settings for a small, fast, deterministic test process.

    ``seed_entries``/``seed_orders`` are 0 so nothing is generated implicitly — a test that wants
    a corpus builds one with a known seed, so every expected count is derived from the fixture
    rather than inherited from rows nobody in the test wrote down. ``log_level`` is WARNING so a
    failing assertion is not buried under startup INFO lines.
    """
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
    )


@pytest.fixture()
def app(settings: Settings) -> FastAPI:
    """A FastAPI app built from the injected Settings (environment not consulted)."""
    return create_app(settings=settings)


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    """A synchronous TestClient that does NOT enter the lifespan.

    Used bare (rather than as a context manager) on purpose: the unit suite exercises the HTTP
    surface hermetically, and startup work belongs to the integration suite, whose
    ``real_client`` fixture drives the same app *through* the lifespan.
    """
    return TestClient(app)

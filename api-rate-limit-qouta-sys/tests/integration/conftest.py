"""Fixtures for the integration suite: a real Redis URL and a connected, isolated gateway.

The compose ``test`` service sets ``REDIS_URL=redis://redis:6379/0`` and waits on Redis's own
``redis-cli ping`` healthcheck, so by the time pytest starts the server is answering commands
(rather than merely listening — the distinction ``depends_on: condition: service_healthy`` exists
to make).

.. rubric:: Why the gateway fixture flushes BEFORE as well as after

Flushing only on teardown leaves the suite's correctness depending on every test that came before
it having exited cleanly. A test interrupted by ``ctrl-c``, an ``xdist`` worker crash, or simply a
failing assertion inside a ``with`` block leaves keys behind, and the very next test then reads a
counter it did not write. For a rate limiter that is the worst possible flakiness: a leftover
``rate_limit:{alice}`` hash makes a bucket test pass or fail depending on what ran before it, and
the failure looks like a limiter bug rather than a fixture bug. Flushing on the way in makes each
test's starting state a fact rather than an inference.
"""

from __future__ import annotations

import os

import pytest

from src.config import Settings
from src.redis_client import RedisGateway

#: The compose ``test`` service injects this; the default is the same value so a developer running
#: pytest inside any container on the compose network gets the same target without extra setup.
DEFAULT_REDIS_URL = "redis://redis:6379/0"


@pytest.fixture(scope="session")
def redis_url() -> str:
    """The real Redis this suite runs against. Session-scoped — it is a constant, not a resource."""
    return os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)


@pytest.fixture()
def redis_settings(redis_url: str) -> Settings:
    """Settings pointed at the real Redis, with everything else left at its shipped default.

    ``_env_file=None`` cuts the ``.env`` source out so nothing on a developer's disk influences the
    run. The three secrets are NOT passed: ``tests/conftest.py`` puts them in ``os.environ`` with
    ``setdefault`` before ``src`` is imported, so they resolve from there — one declaration, in the
    file the tests actually read, exactly as C1 designed it.
    """
    return Settings(_env_file=None, redis_url=redis_url)


@pytest.fixture()
async def gateway(redis_settings: Settings):
    """Yield a connected :class:`~src.redis_client.RedisGateway` against a flushed database.

    Function-scoped, deliberately. A session-scoped async fixture would be bound to whichever event
    loop created it, while ``pytest.ini`` sets ``asyncio_default_fixture_loop_scope = function`` —
    so a shared gateway would hand loop-bound connections to tests running on a different loop, and
    the failure surfaces as an intermittent "attached to a different loop" hundreds of lines from
    its cause. Connection setup here is lazy and local, so the per-test cost is a ``FLUSHDB``.
    """
    instance = RedisGateway(redis_settings)
    await instance.connect()
    try:
        await instance.client.flushdb()
        yield instance
    finally:
        try:
            await instance.client.flushdb()
        finally:
            await instance.aclose()

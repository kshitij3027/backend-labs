"""Integration fixtures: the app built the way production builds it, lifespan included.

``real_app`` calls :func:`src.main.create_app` with **no injected settings**, so configuration is
resolved exactly as it is in the container — field defaults, then the optional ``.env``, then the
environment compose supplies. ``real_client`` then drives it as a **context manager**, which is
what makes Starlette run the lifespan: startup on entry, shutdown on exit.

That distinction is the whole point of this file. The unit suite's ``client`` skips the lifespan
so it can stay fast and hermetic; the code that runs *in* the lifespan is precisely the code that
can only break in production — from C2 it opens the SQLAlchemy engine and runs the schema-create
retry loop, and from C6 it starts the Redis pub/sub reader task and, on the way out, has to close
every live subscriber and dispose the pool. A suite that only ever used the injected-settings path
would prove the wrong half of the application.

Session-scoped for the app, function-scoped for the client, so each test gets a fresh
startup/shutdown cycle over one construction — the startup path is what is under test, so running
it once per test is a feature rather than overhead.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.main import create_app


@pytest.fixture()
def real_app() -> FastAPI:
    """The application as production builds it: settings resolved from the environment."""
    return create_app()


@pytest.fixture()
def real_client(real_app: FastAPI) -> Iterator[TestClient]:
    """A TestClient over ``real_app``, entered as a context manager so the lifespan runs.

    Yielded from a generator fixture rather than returned, so the ``__exit__`` — and therefore
    the shutdown half of the lifespan — is guaranteed to run even when the test fails.
    """
    with TestClient(real_app) as client:
        yield client

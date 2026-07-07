"""Shared pytest fixtures: an app with an injected Runtime, and a TestClient.

The ``app`` fixture injects a pre-built :class:`src.main.Runtime` into
:func:`src.api.create_app`, which makes the app skip the FastAPI lifespan entirely —
no startup work, no background pipeline — so tests exercise the HTTP surface
hermetically.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import get_settings
from src.main import Runtime


@pytest.fixture()
def app():
    """A FastAPI app wired to a fresh Runtime (lifespan skipped, pipeline off)."""
    return create_app(runtime=Runtime.build(get_settings()))


@pytest.fixture()
def client(app):
    """A synchronous TestClient against the injected-runtime app."""
    return TestClient(app)

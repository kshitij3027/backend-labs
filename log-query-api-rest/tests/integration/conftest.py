"""Integration fixtures: a TestClient whose app was built through the SEEDED runtime path.

``seeded_client`` exercises :meth:`src.main.Runtime.build_seeded` — the same constructor the
production lifespan uses — rather than the cheap :meth:`~src.main.Runtime.build`, so the
integration suite covers the path that actually ships. It is intentionally thin in C1 (the two
paths are still identical); C5 grows it once ``build_seeded`` really populates a log store and
the pagination tests need a corpus to walk.

The ``settings`` fixture it depends on comes from ``tests/conftest.py``, which already pins a
small store and ``seed_entries=0``.
"""

import pytest
from fastapi.testclient import TestClient

from src.config import Settings
from src.main import Runtime, create_app


@pytest.fixture()
def seeded_client(settings: Settings) -> TestClient:
    """A TestClient against an app built via the production (seeded) Runtime path."""
    return TestClient(create_app(runtime=Runtime.build_seeded(settings)))

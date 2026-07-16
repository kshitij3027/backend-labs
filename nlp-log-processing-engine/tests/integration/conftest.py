"""Integration fixtures: a TestClient whose app has a fully LOADED NLPEngine.

The ``loaded_client`` fixture builds the app with ``Runtime.build_loaded`` — which constructs
an :class:`~src.nlp.NLPEngine` and ``load()``s it (spaCy, the intent pipeline, VADER, YAKE) —
so the API is exercised against a real, ready engine (``analyzer_ready`` is ``True`` and the
analyze routes serve real results instead of a 503).

The fixture is **session-scoped** so that expensive load happens exactly ONCE for the whole
integration package, not per test. ``get_settings.cache_clear()`` is called first so the
Runtime reflects the current environment rather than a Settings instance cached by an earlier
(unit) test.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import get_settings
from src.main import Runtime


@pytest.fixture(scope="session")
def loaded_client() -> TestClient:
    """A TestClient against an app whose Runtime carries a fully loaded NLPEngine (built once)."""
    get_settings.cache_clear()
    app = create_app(runtime=Runtime.build_loaded(get_settings()))
    return TestClient(app)

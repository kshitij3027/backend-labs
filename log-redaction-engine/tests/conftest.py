"""Shared pytest fixtures and environment bootstrap.

A deterministic 64-hex test salt is injected into ``os.environ`` BEFORE any
``src.*`` import so that ``src.settings.Settings()`` (which marks
``REDACTION_HASH_SALT`` as required) can construct without a real ``.env``
file. The value itself is irrelevant for C1 - the smoke test only checks
that the app boots and ``/api/health`` returns the documented payload.

Why os.environ at module top vs a monkeypatch fixture
-----------------------------------------------------
``src.settings`` is imported transitively by ``src.main`` at module import
time, which happens the moment a test file does ``from src.main import app``.
A function-scoped monkeypatch fixture runs too late: the import has already
been triggered by the test module's top-level imports. Setting the env var
at this module's top (and using ``setdefault`` so an operator-supplied
value still wins) guarantees the env is correct before any import chain
reaches pydantic-settings.
"""
from __future__ import annotations

import os

import pytest

# CRITICAL: must run before any ``src.*`` import so ``Settings()`` can
# construct. ``"deadbeef" * 8`` produces a 64-character hex string, matching
# the operator-facing format documented in ``.env.example``.
os.environ.setdefault("REDACTION_HASH_SALT", "deadbeef" * 8)
os.environ.setdefault("LOG_LEVEL", "WARNING")


@pytest.fixture(autouse=True, scope="session")
def _bootstrap_settings_env() -> None:
    """Session-scoped no-op fixture that documents the env bootstrap above.

    The real work happens at module import (see the ``os.environ.setdefault``
    calls above) because Settings() runs at import time. This fixture exists
    so future commits can extend it with monkeypatch-style overrides without
    moving the bootstrap logic.
    """
    return None

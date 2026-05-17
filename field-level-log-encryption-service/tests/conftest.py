"""Shared pytest fixtures and environment bootstrap.

A fixed test KEK is injected into the environment BEFORE any `src.*` import
so that `src.settings.Settings()` can construct without requiring a real
`.env` file. Using a zero-byte 32-byte key is intentional: we only need a
syntactically-valid value here; cryptographic tests in C3+ will validate
properties, not values.
"""
from __future__ import annotations

import base64
import os

# CRITICAL: must run before any `src.*` import so Settings() can construct.
_TEST_KEY = base64.b64encode(b"\x00" * 32).decode()
os.environ.setdefault("MASTER_KEY_B64", _TEST_KEY)
os.environ.setdefault("LOG_LEVEL", "WARNING")

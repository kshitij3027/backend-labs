"""Shared pytest fixtures for the multi-region log replication tests."""

from __future__ import annotations

import time
import uuid
from typing import Dict

import pytest

from src.config import AppConfig
from src.models import LogEntry


@pytest.fixture
def app_config() -> AppConfig:
    """Fresh AppConfig with all spec defaults — no env mutation."""
    # Pass an empty mapping so we don't pick up the host's env.
    return AppConfig.from_env(env={})


@pytest.fixture
def sample_vc_a() -> Dict[str, int]:
    """A vector clock where us-east has advanced once."""
    return {"us-east": 1, "europe": 0}


@pytest.fixture
def sample_vc_b() -> Dict[str, int]:
    """A vector clock where us-east has advanced twice (causally after sample_vc_a)."""
    return {"us-east": 2, "europe": 0}


@pytest.fixture
def sample_vc_concurrent_a() -> Dict[str, int]:
    """Concurrent companion to ``sample_vc_concurrent_b`` (each ahead in one region)."""
    return {"us-east": 1, "europe": 0}


@pytest.fixture
def sample_vc_concurrent_b() -> Dict[str, int]:
    """Concurrent companion to ``sample_vc_concurrent_a``."""
    return {"us-east": 0, "europe": 1}


@pytest.fixture
def sample_log_entry() -> LogEntry:
    """A fully-populated LogEntry for round-trip + handler tests."""
    return LogEntry(
        log_id=uuid.uuid4().hex,
        data={"message": "hello", "level": "info", "service": "test"},
        region="us-east",
        created_at=time.time(),
        vector_clock={"us-east": 1, "europe": 0, "asia": 0},
        logical_ts=1,
    )

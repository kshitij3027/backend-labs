"""Shared pytest fixtures for the protocol-buffer-log-processing test suite."""

from __future__ import annotations

import pytest

from src.config import Config
from src.log_generator import generate_log_batch, generate_log_entry


@pytest.fixture()
def sample_log_entry() -> dict:
    """Return a single generated log entry dict."""
    return generate_log_entry()


@pytest.fixture()
def sample_log_batch() -> list[dict]:
    """Return a batch of 10 generated log entries."""
    return generate_log_batch(10)


@pytest.fixture()
def config() -> Config:
    """Return a Config instance built from current environment variables."""
    return Config.from_env()

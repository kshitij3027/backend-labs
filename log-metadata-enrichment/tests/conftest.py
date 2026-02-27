"""Shared pytest fixtures for the log metadata enrichment pipeline tests."""

import pytest

from src.config import AppConfig
from src.models import EnrichmentRequest


@pytest.fixture
def sample_config():
    """Return an AppConfig with default values."""
    return AppConfig()


@pytest.fixture
def sample_request():
    """Return a basic enrichment request for testing."""
    return EnrichmentRequest(log_message="Test log message", source="test")


@pytest.fixture
def error_request():
    """Return an enrichment request containing an error message."""
    return EnrichmentRequest(log_message="ERROR: Something failed", source="test")

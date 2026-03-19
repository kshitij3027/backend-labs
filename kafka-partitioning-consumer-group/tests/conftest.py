"""Shared test fixtures."""
import pytest
from src.config import Settings, load_config
from src.models import LogEntry, LogLevel
from src.producer.log_generator import LogGenerator


@pytest.fixture
def settings() -> Settings:
    """Default settings for tests."""
    return Settings()


@pytest.fixture
def log_generator(settings: Settings) -> LogGenerator:
    """Log generator with default settings."""
    return LogGenerator(settings)


@pytest.fixture
def sample_log_entry() -> LogEntry:
    """A sample log entry for testing."""
    return LogEntry(
        level=LogLevel.INFO,
        service="test-service",
        message="Test log message",
        user_id="1234",
    )

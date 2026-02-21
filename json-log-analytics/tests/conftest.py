import pytest
from app import create_app
from config import Config
from validator import LogValidator


@pytest.fixture
def sample_valid_log():
    return {
        "timestamp": "2024-01-15T10:30:00Z",
        "level": "INFO",
        "service": "auth-service",
        "message": "User logged in",
    }


@pytest.fixture
def sample_invalid_log():
    return {
        "timestamp": "2024-01-15T10:30:00Z",
        "message": "Missing level and service",
    }


@pytest.fixture
def sample_log_with_metadata():
    return {
        "timestamp": "2024-01-15T10:30:00Z",
        "level": "INFO",
        "service": "auth-service",
        "message": "User logged in",
        "user_id": "user-123",
        "metadata": {
            "processing_time_ms": 45.2,
            "request_id": "req-001",
        },
    }


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def validator(config):
    return LogValidator(config["schema"]["path"])


@pytest.fixture
def app():
    """Create a Flask test app."""
    application = create_app()
    application.config["TESTING"] = True
    return application


@pytest.fixture
def client(app):
    """Create a Flask test client."""
    return app.test_client()

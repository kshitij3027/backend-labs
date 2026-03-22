"""Shared test fixtures for the kafka-streams-monitoring-dashboard tests."""

import pytest

from src.config import Settings


@pytest.fixture
def config():
    """Return a Settings instance pointing to localhost for tests."""
    return Settings(bootstrap_servers="localhost:9092")


@pytest.fixture
def flask_app(config):
    """Create a Flask app instance for testing."""
    from src.dashboard import create_app

    app, socketio = create_app(config)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(flask_app):
    """Return a Flask test client."""
    return flask_app.test_client()

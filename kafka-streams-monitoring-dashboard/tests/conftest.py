"""Shared test fixtures for the kafka-streams-monitoring-dashboard tests."""

import time

import pytest

from src.business_metrics import BusinessMetricsTracker
from src.config import Settings
from src.geo_analyzer import GeoAnalyzer
from src.metrics_store import MetricsStore
from src.stream_processor import StreamProcessor


@pytest.fixture
def config():
    """Return a Settings instance pointing to localhost for tests."""
    return Settings(bootstrap_servers="localhost:9092")


@pytest.fixture
def metrics_store():
    """Return a fresh MetricsStore with a small max_length for tests."""
    return MetricsStore(max_length=100)


@pytest.fixture
def business_metrics():
    """Return a fresh BusinessMetricsTracker for tests."""
    return BusinessMetricsTracker(max_users=100)


@pytest.fixture
def geo_analyzer():
    """Return a fresh GeoAnalyzer for tests."""
    return GeoAnalyzer()


@pytest.fixture
def stream_processor(metrics_store, business_metrics, geo_analyzer):
    """Return a StreamProcessor wired to the test metrics_store with business metrics and geo."""
    return StreamProcessor(metrics_store, business_metrics=business_metrics, geo_analyzer=geo_analyzer)


@pytest.fixture
def sample_log_event():
    return {
        "path": "/api/v1/users",
        "method": "GET",
        "status_code": 200,
        "response_time": 45.2,
        "ip_address": "10.0.0.1",
        "timestamp": time.time(),
    }


@pytest.fixture
def sample_error_event():
    return {
        "error_type": "TimeoutError",
        "severity": "high",
        "service": "auth-service",
        "stack_trace": "...",
        "timestamp": time.time(),
    }


@pytest.fixture
def sample_user_event():
    return {
        "user_id": "user-123",
        "action": "login",
        "session_id": "sess-abc",
        "path": "/login",
        "timestamp": time.time(),
    }


@pytest.fixture
def flask_app(config, metrics_store, business_metrics, geo_analyzer):
    """Create a Flask app instance for testing with a metrics_store."""
    from src.dashboard import create_app

    app, socketio = create_app(
        config,
        metrics_store=metrics_store,
        business_metrics=business_metrics,
        geo_analyzer=geo_analyzer,
    )
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(flask_app):
    """Return a Flask test client."""
    return flask_app.test_client()

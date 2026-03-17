"""Shared test fixtures."""
import json
import time

import pytest

from src.config import Settings


@pytest.fixture
def settings():
    """Return default Settings for testing."""
    return Settings(
        bootstrap_servers="localhost:9092",
        redis_host="localhost",
    )


@pytest.fixture
def sample_web_log():
    """Return a raw web access log dict."""
    return {
        "timestamp": time.time(),
        "log_type": "web_access",
        "endpoint": "/api/users",
        "method": "GET",
        "status_code": 200,
        "response_time_ms": 45.2,
        "source_ip": "192.168.1.10",
        "geo": "us-east",
    }


@pytest.fixture
def sample_app_log():
    """Return a raw application log dict."""
    return {
        "timestamp": time.time(),
        "log_type": "app_log",
        "service": "auth-service",
        "component": "login",
        "level": "INFO",
        "message": "User logged in successfully",
    }


@pytest.fixture
def sample_error_log():
    """Return a raw error log dict."""
    return {
        "timestamp": time.time(),
        "log_type": "error_log",
        "error_type": "NullPointerException",
        "stack_trace": "at com.example.App.main(App.java:10)",
        "endpoint": "/api/orders",
        "severity": "ERROR",
        "message": "Null reference in order processing",
    }

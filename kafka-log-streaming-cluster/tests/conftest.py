"""Shared pytest fixtures for the Kafka log streaming cluster tests."""

import pytest

from src.config import Settings
from src.models import LogLevel, LogMessage, ServiceName


@pytest.fixture
def settings() -> Settings:
    """Default test settings."""
    return Settings()


@pytest.fixture
def sample_log_message() -> LogMessage:
    """A single sample INFO-level log message."""
    return LogMessage(
        timestamp="2026-03-15T10:30:00+00:00",
        service=ServiceName.WEB_API,
        level=LogLevel.INFO,
        endpoint="/api/users",
        status_code=200,
        user_id="test-user-001",
        message="Request processed successfully",
        sequence_number=1,
    )


@pytest.fixture
def sample_error_message() -> LogMessage:
    """A sample ERROR-level message from the payment service."""
    return LogMessage(
        timestamp="2026-03-15T10:31:00+00:00",
        service=ServiceName.PAYMENT_SERVICE,
        level=LogLevel.ERROR,
        endpoint="/payments/process",
        status_code=500,
        user_id="test-user-002",
        message="Payment gateway timeout",
        sequence_number=2,
    )


@pytest.fixture
def sample_messages_batch() -> list[LogMessage]:
    """Batch of 10 diverse log messages across services and levels."""
    services = list(ServiceName)
    levels = list(LogLevel)
    endpoints = [
        "/api/users",
        "/api/login",
        "/users/profile",
        "/users/settings",
        "/payments/process",
        "/payments/refund",
        "/api/health",
        "/users/verify",
        "/payments/status",
        "/api/search",
    ]
    status_codes = [200, 201, 200, 404, 500, 200, 200, 401, 502, 200]
    messages = [
        "User list fetched",
        "Login successful",
        "Profile loaded",
        "Settings page not found",
        "Payment processing failed",
        "Refund issued",
        "Health check OK",
        "Email verification failed",
        "Payment gateway error",
        "Search completed",
    ]

    batch = []
    for i in range(10):
        batch.append(
            LogMessage(
                timestamp=f"2026-03-15T10:{30 + i}:00+00:00",
                service=services[i % len(services)],
                level=levels[i % len(levels)],
                endpoint=endpoints[i],
                status_code=status_codes[i],
                user_id=f"test-user-{i:03d}",
                message=messages[i],
                sequence_number=i + 1,
            )
        )
    return batch
